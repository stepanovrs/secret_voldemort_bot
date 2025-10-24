
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from pathlib import Path
from datetime import datetime, timedelta, timezone

import json

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from config import INITIAL_RATING, MAX_BLUE
from db import Player, Game, GameParticipant, now_msk

# ---- Time helpers (safe MSK) ----
try:
    from zoneinfo import ZoneInfo
    MSK = ZoneInfo("Europe/Moscow")
except Exception:
    MSK = timezone(timedelta(hours=3))

def _now_msk() -> datetime:
    try:
        from zoneinfo import ZoneInfo as _Z
        return datetime.now(_Z("Europe/Moscow"))
    except Exception:
        return datetime.now(timezone(timedelta(hours=3)))

# ===== JSON log path for per-player stats ("Игрок дня") =====
STATS_LOG_PATH = Path("game_stats.json")

def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

def _save_json(path: Path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

# ================= Team helpers =================
@dataclass
class TeamAverages:
    blue_avg: float
    red_avg: float

async def get_team_rosters(session: AsyncSession, game_id: int) -> Tuple[List[Player], List[Player], Optional[Player]]:
    """Return (blue_players, red_players, voldemort_player). Red list includes Voldemort."""
    g = await session.get(Game, game_id)
    if not g:
        return [], [], None
    res = await session.execute(select(GameParticipant).where(GameParticipant.game_id == game_id))
    parts = list(res.scalars().all())
    blue_ids = [p.player_id for p in parts if p.team == "blue"]
    red_ids = [p.player_id for p in parts if p.team in ("red", "voldemort")]
    blue: List[Player] = []
    red: List[Player] = []
    vold: Optional[Player] = None
    if blue_ids:
        resb = await session.execute(select(Player).where(Player.id.in_(blue_ids)))
        blue = list(resb.scalars().all())
    if red_ids:
        resr = await session.execute(select(Player).where(Player.id.in_(red_ids)))
        red = list(resr.scalars().all())
    if g.voldemort_id:
        vold = await session.get(Player, g.voldemort_id)
    return blue, red, vold

def _team_avgs(blue: List[Player], red: List[Player]) -> TeamAverages:
    b = sum(p.rating for p in blue) / max(len(blue), 1)
    r = sum(p.rating for p in red) / max(len(red), 1)
    return TeamAverages(b, r)

# ================= Core MMR =================
from typing import Tuple as _Tuple


def _mmr_delta(blue_avg: float, red_avg: float, winner: str) -> _Tuple[int, int]:
    """
    НЕмасштабированная (raw) формула дельты MMR:
      diff = |AVG_blue − AVG_red|
      x = floor(diff/10); если diff > 400 → x_eff = 41 (то есть как при 410+).
      Если побеждает более сильная команда: сильная +(51−x_eff), слабая −(49−x_eff).
      Если побеждает более слабая команда: слабая +(51+x_eff), сильная −(49+x_eff).
    НИКАКОГО дополнительного масштабирования по сумме 100 здесь нет.
    """
    diff = abs(blue_avg - red_avg)
    x = int(diff // 10)
    x_eff = 41 if x > 40 else x

    blue_is_strong = blue_avg >= red_avg

    if winner == "blue":
        if blue_is_strong:
            delta_blue = 51 - x_eff     # например diff=400 => +11
            delta_red  = -(49 - x_eff)  # и -9
        else:
            delta_blue = 51 + x_eff     # например diff=400 => +91
            delta_red  = -(49 + x_eff)  # и -89
    else:  # winner == "red"
        if not blue_is_strong:          # красные сильнее
            delta_red  = 51 - x_eff
            delta_blue = -(49 - x_eff)
        else:                            # красные слабее
            delta_red  = 51 + x_eff
            delta_blue = -(49 + x_eff)

    return int(delta_blue), int(delta_red)
    lam = 100.0 / s
    db = delta_blue * lam
    dr = delta_red * lam
    ib = int(round(db))
    ir = int(round(dr))
    # Коррекция до точной "100"
    total = abs(ib) + abs(ir)
    if total < 100:
        # добавим 1 очко стороне с большим модулем (чаще победителю)
        if abs(ib) >= abs(ir):
            ib += 1 if ib > 0 else -1 if ib < 0 else 1
        else:
            ir += 1 if ir > 0 else -1 if ir < 0 else -1
    elif total > 100:
        if abs(ib) >= abs(ir):
            ib -= 1 if ib > 0 else -1 if ib < 0 else 1
        else:
            ir -= 1 if ir > 0 else -1 if ir < 0 else -1
    return int(ib), int(ir)

# ================= Social points =================
def _add_social(result_type: str, blue: List[Player], red: List[Player], killer: Optional[Player]) -> Dict[int, Dict[str, int]]:
    """Единичные «социальные» очки за конкретный исход (на основе прежней логики)."""
    inc: Dict[int, Dict[str, int]] = {}

    def add(p: Player, field: str, v: int = 1):
        d = inc.setdefault(p.id, {})
        d[field] = d.get(field, 0) + v

    if result_type.startswith("blue_"):
        for p in blue:
            add(p, "social_blue", 1)
        if killer:
            add(killer, "killer_points", 1)
    else:
        for p in red:
            add(p, "social_red", 1)
        if killer:
            add(killer, "killer_points", 1)
    return inc

# ================= Galleons =================
def _win_streak_bonus(streak: int) -> int:
    # 2->+2, 3->+4, 4->+8, 5->+16, 6->+32, 7->+100, >7 -> +100
    if streak == 2: return 2
    if streak == 3: return 4
    if streak == 4: return 8
    if streak == 5: return 16
    if streak == 6: return 32
    if streak >= 7: return 100
    return 0

def _lose_streak_bonus(streak: int) -> int:
    # 2 подряд -> +2; 4 -> +4; 6 -> +6; >6 -> каждое следующее поражение +6
    if streak == 2: return 2
    if streak == 4: return 4
    if streak == 6: return 6
    if streak > 6: return 6
    return 0


async def _apply_galleons_for_game(session: AsyncSession, g: Game, blue: List[Player], red: List[Player], vold: Optional[Player], killer: Optional[Player]) -> None:
    """Зачисления по правилам:
       +1 всем за участие; +1 победителям; +3 Воландеморту; +5 убийце Воландеморта;
       бонус за win-streak по таблице выше; за lose-streak: 2/4/6 на 2/4/6, далее по 6 за каждое поражение (сбрасываем win-streak у проигравших)."""
    winner = "blue" if (g.result_type or "").startswith("blue_") else "red"

    # +1 участие всем
    for p in blue + red:
        p.galleons_balance = int(getattr(p, "galleons_balance", 0)) + 1

    # +3 Voldemort (независимо от исхода)
    if vold:
        vold.galleons_balance = int(vold.galleons_balance) + 3

    # +5 убийце (если есть)
    if killer:
        killer.galleons_balance = int(killer.galleons_balance) + 5

    # Победители +1, обновление стриков и бонус за винстрик
    if winner == "blue":
        winners, losers = blue, red
    else:
        winners, losers = red, blue

    for p in winners:
        p.galleons_balance = int(p.galleons_balance) + 1  # победа
        p.win_streak = int(getattr(p, "win_streak", 0) or 0) + 1
        p.lose_streak = 0
        p.galleons_balance = int(p.galleons_balance) + _win_streak_bonus(p.win_streak)

    for p in losers:
        p.lose_streak = int(getattr(p, "lose_streak", 0) or 0) + 1
        p.win_streak = 0
        p.galleons_balance = int(p.galleons_balance) + _lose_streak_bonus(p.lose_streak)

    await session.commit()

# ================== Apply ratings ==================
def _append_game_stats(game_id: int, blue: List[Player], red: List[Player], avgs: TeamAverages, d_blue: int, d_red: int, inc: Dict[int, Dict[str, int]]):
    """Append per-player snapshot into JSON for daily/weekly analytics."""
    payload = _load_json(STATS_LOG_PATH)
    ts = _now_msk().isoformat()
    def social_sum(pid: int) -> int:
        return sum(inc.get(pid, {}).values()) if pid in inc else 0

    for p in blue:
        payload.append({
            "game_id": game_id,
            "player_id": p.id,
            "side": "blue",
            "mmr_delta": d_blue,
            "social_gain": social_sum(p.id),
            "opponent_avg": float(avgs.red_avg),
            "ts": ts,
        })
    for p in red:
        payload.append({
            "game_id": game_id,
            "player_id": p.id,
            "side": "red",
            "mmr_delta": d_red,
            "social_gain": social_sum(p.id),
            "opponent_avg": float(avgs.blue_avg),
            "ts": ts,
        })
    _save_json(STATS_LOG_PATH, payload)

async def set_team_roster(session: AsyncSession, game_id: int, team: str, player_ids: List[int]) -> None:
    """Replace team roster for a game."""
    # remove existing
    res = await session.execute(select(GameParticipant).where(GameParticipant.game_id == game_id, GameParticipant.team == team))
    for row in list(res.scalars().all()):
        await session.delete(row)
    # add new
    for pid in player_ids:
        session.add(GameParticipant(game_id=game_id, player_id=pid, team=team))
    await session.commit()

async def validate_rosters(*args) -> Tuple[bool, str]:
    """Проверка составов.
    Поддерживает оба вызова:
      1) await validate_rosters(session, game_id)
      2) await/прямо validate_rosters(blue_list, red_list, vold_player | None)
    Возвращает (ok: bool, message: str).
    """
    # Определим форму аргументов
    if len(args) == 2:
        session, game_id = args
        blue, red, vold = await get_team_rosters(session, int(game_id))
    else:
        blue, red = args[0], args[1]
        vold = args[2] if len(args) >= 3 else None

    if not blue or not red:
        return False, "Добавьте игроков в обе команды."
    if len(blue) > MAX_BLUE:
        return False, f"Макс. синих: {MAX_BLUE}"
    if vold is None:
        return False, "Выберите Воландеморта."
    # Воландеморт не должен быть в синей команде
    if any(p.id == getattr(vold, "id", -1) for p in blue):
        return False, "Воландеморт не может быть в Ордене Феникса."
    return True, "Составы корректны."

async def set_voldemort(session: AsyncSession, game_id: int, player_id: Optional[int]) -> None:
    g = await session.get(Game, game_id)
    if not g:
        return
    g.voldemort_id = player_id
    await session.commit()

async def set_result_type_and_killer(session: AsyncSession, game_id: int, result_type: str, killer_id: Optional[int]) -> None:
    g = await session.get(Game, game_id)
    if not g:
        return
    g.result_type = result_type
    g.killer_id = killer_id
    await session.commit()

async def apply_ratings(session: AsyncSession, game_id: int) -> str:
    """Main entry: applies MMR/social and galleons for a finished game."""
    g = await session.get(Game, game_id)
    if not g or not g.result_type:
        return "Игра не завершена."

    blue, red, vold = await get_team_rosters(session, game_id)
    killer = await session.get(Player, g.killer_id) if g.killer_id else None
    avgs = _team_avgs(blue, red)

    winner = "blue" if g.result_type.startswith("blue_") else "red"
    d_blue, d_red = _mmr_delta(avgs.blue_avg, avgs.red_avg, winner)

    # Социальные очки
    inc = _add_social(g.result_type, blue, red, killer)

    # Применяем MMR
    for p in blue:
        p.rating = int(p.rating) + d_blue
    for p in red:
        p.rating = int(p.rating) + d_red

    # Применяем соц-очки
    for pid, fields in inc.items():
        pl = await session.get(Player, pid)
        for field, v in fields.items():
            setattr(pl, field, int(getattr(pl, field)) + int(v))

    await session.commit()

    # Галлеоны
    await _apply_galleons_for_game(session, g, blue, red, vold, killer)

    # Лог в файл
    _append_game_stats(game_id, blue, red, avgs, d_blue, d_red, inc)

    fav = "Орден Феникса" if avgs.blue_avg >= avgs.red_avg else "Красные"
    # Winner & how
    result = g.result_type or ""
    side = "Орден Феникса" if result.startswith("blue_") else "Пожиратели"
    try:
        nlaws = int(result.split("_", 1)[1])
    except Exception:
        nlaws = 5
    color = "синих" if side == "Орден Феникса" else "красных"
    head = f"Игра завершена.\nПобеда {side} — выложены {nlaws} {color} законов\n"
    text = (
        head +
        f"Средний MMR — Орден Феникса: {avgs.blue_avg:.1f}, Красные: {avgs.red_avg:.1f}\n"
        f"Фаворит матча: {fav}\n"
        f"Изменение MMR — Синие: {d_blue}, Красные: {d_red}"
    )
    return text

# ============= Recomputation utilities =============
async def recompute_all_ratings(session: AsyncSession) -> str:
    """Recompute all MMR & socials from scratch (games in id order)."""
    # reset ratings + social
    res = await session.execute(select(Player))
    players = list(res.scalars().all())
    for p in players:
        p.rating = int(INITIAL_RATING)
        p.social_blue = 0
        p.social_red = 0
        p.social_vold = 0
        p.killer_points = 0
    await session.commit()

    resg = await session.execute(select(Game).where(Game.result_type.is_not(None)).order_by(Game.id.asc()))
    games = list(resg.scalars().all())
    for g in games:
        # team rosters
        res_parts = await session.execute(select(GameParticipant).where(GameParticipant.game_id == g.id))
        parts = list(res_parts.scalars().all())
        blue_ids = [p.player_id for p in parts if p.team == "blue"]
        red_ids  = [p.player_id for p in parts if p.team in ("red", "voldemort")]
        blue, red = [], []
        if blue_ids:
            resb = await session.execute(select(Player).where(Player.id.in_(blue_ids)))
            blue = list(resb.scalars().all())
        if red_ids:
            resr = await session.execute(select(Player).where(Player.id.in_(red_ids)))
            red = list(resr.scalars().all())

        avgs = _team_avgs(blue, red)
        winner = "blue" if g.result_type.startswith("blue_") else "red"
        d_blue, d_red = _mmr_delta(avgs.blue_avg, avgs.red_avg, winner)
        inc = _add_social(g.result_type, blue, red, await session.get(Player, g.killer_id) if g.killer_id else None)

        for p in blue:
            p.rating = int(p.rating) + d_blue
        for p in red:
            p.rating = int(p.rating) + d_red
        for pid, fields in inc.items():
            pl = await session.get(Player, pid)
            for field, v in fields.items():
                setattr(pl, field, int(getattr(pl, field)) + int(v))
        await session.commit()

    return f"Пересчитано игр: {len(games)}; игроков: {len(players)}"

async def recompute_all_galleons(session: AsyncSession) -> str:
    """Full recomputation of galleons & streaks across all finished games, preserving purchases (spendings)."""
    # reset balances & streaks
    res = await session.execute(select(Player))
    players = list(res.scalars().all())
    for p in players:
        p.galleons_balance = 0
        p.win_streak = 0
        p.lose_streak = 0
    await session.commit()

    resg = await session.execute(select(Game).where(Game.result_type.is_not(None)).order_by(Game.id.asc()))
    games = list(resg.scalars().all())
    for g in games:
        blue, red, vold = await get_team_rosters(session, g.id)
        killer = await session.get(Player, g.killer_id) if g.killer_id else None
        await _apply_galleons_for_game(session, g, blue, red, vold, killer)

    # учтём сделанные покупки как «расходы»
    from sqlalchemy import select as _select
    from db import Purchase
    res = await session.execute(_select(Purchase))
    all_purchases = list(res.scalars().all())
    spent_by: Dict[int, int] = {}
    for pur in all_purchases:
        spent_by[pur.player_id] = spent_by.get(pur.player_id, 0) + int(pur.cost or 0)
    for p in players:
        if p.id in spent_by:
            p.galleons_balance = int(p.galleons_balance) - int(spent_by[p.id])
    await session.commit()
    return f"Пересчитано игр: {len(games)}; игроков: {len(players)}; покупок учтено: {len(all_purchases)}"

# ============= Search helper =============
async def search_players(session: AsyncSession, query: str) -> List[Player]:
    """Simple case-insensitive search over first/last/username."""
    q = (query or "").strip().lower()
    if not q:
        return []
    res = await session.execute(select(Player))
    players = list(res.scalars().all())
    def matches(p: Player) -> bool:
        items = [
            (p.first_name or "").lower(),
            (p.last_name or "").lower(),
            (p.username or "").lower(),
        ]
        return any(q in s for s in items)
    return [p for p in players if matches(p)]


# ============= Search helper & streaks =============
async def get_player_streaks(session: AsyncSession, player_id: int) -> Dict[str, int]:
    """
    Возвращает стрики игрока:
      { 'max_win': N, 'max_lose': X, 'cur_win': N1, 'cur_lose': X1 }
    Считается по завершённым играм игрока в хронологическом порядке.
    """
    res = await session.execute(
        select(GameParticipant).where(GameParticipant.player_id == player_id).order_by(GameParticipant.game_id.asc())
    )
    parts = list(res.scalars().all())
    if not parts:
        return {"max_win": 0, "max_lose": 0, "cur_win": 0, "cur_lose": 0}

    seq = []  # True = победа, False = поражение
    for gp in parts:
        g = await session.get(Game, gp.game_id)
        if not g or not g.result_type:
            continue
        winner = "blue" if g.result_type.startswith("blue_") else "red"
        side = "blue" if gp.team == "blue" else "red"  # 'red' включает и 'voldemort'
        seq.append(side == winner)

    cur_w = cur_l = max_w = max_l = 0
    for win in seq:
        if win:
            cur_w += 1
            cur_l = 0
            if cur_w > max_w:
                max_w = cur_w
        else:
            cur_l += 1
            cur_w = 0
            if cur_l > max_l:
                max_l = cur_l

    return {"max_win": max_w, "max_lose": max_l, "cur_win": cur_w, "cur_lose": cur_l}
