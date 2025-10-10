from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import INITIAL_RATING, MAX_BLUE
from db import Game, GameParticipant, Player

# ====== пути лёгких логов (для "Игрок дня" и аналитики) ======
STATS_LOG_PATH = Path("game_stats.json")  # поигровая статистика на уровне игроков

RU_MONTHS = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]

# ============== Small FS helpers ==============
def _now_msk() -> datetime:
    return datetime.now(ZoneInfo("Europe/Moscow"))

def _load_json(path: Path):
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_json(path: Path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

# ============== Team helpers ==============
async def get_team_rosters(session: AsyncSession, game_id: int) -> Tuple[List[Player], List[Player], Optional[Player]]:
    res = await session.execute(select(Game).where(Game.id == game_id))
    g: Game = res.scalar_one()
    res2 = await session.execute(select(GameParticipant).where(GameParticipant.game_id == game_id))
    parts = list(res2.scalars().all())
    blue_ids = [p.player_id for p in parts if p.team == "blue"]
    red_ids = [p.player_id for p in parts if p.team in ("red", "voldemort")]
    blue: List[Player] = []
    red: List[Player] = []
    if blue_ids:
        resb = await session.execute(select(Player).where(Player.id.in_(blue_ids)))
        blue = list(resb.scalars().all())
    if red_ids:
        resr = await session.execute(select(Player).where(Player.id.in_(red_ids)))
        red = list(resr.scalars().all())
    vold = await session.get(Player, g.voldemort_id) if g.voldemort_id else None
    return blue, red, vold

def validate_rosters(blue: List[Player], red: List[Player], vold: Optional[Player]) -> Tuple[bool, str]:
    if not blue:
        return False, "Не выбрана синяя команда."
    if not red:
        return False, "Не выбрана красная сторона (красные и/или Воландеморт)."
    if not vold:
        return False, "Не выбран Воландеморт."
    if any(p.id == vold.id for p in blue):
        return False, "Воландеморт не может быть в синих."
    if len(blue) > MAX_BLUE:
        return False, f"Синих игроков не может быть больше {MAX_BLUE}."
    red_wo_vold = [p for p in red if p.id != (vold.id if vold else -1)]
    if len(red_wo_vold) > 3:
        return False, "Красных игроков не может быть больше 3 (Воландеморт отдельно)."
    return True, "Состав корректен."

# ============== Search proxy ==============
async def search_players(session: AsyncSession, query: str) -> List[Player]:
    from db import search_players as _search
    return await _search(session, query)

# ============== Team mutators ==============
async def set_team_roster(session: AsyncSession, game_id: int, team: str, player_ids: List[int]) -> None:
    """Перезаписывает состав указанной стороны (blue/red). Воландеморта не трогаем здесь."""
    if team not in ("blue", "red"):
        return
    g = await session.get(Game, game_id)
    vold_id = g.voldemort_id

    await session.execute(
        delete(GameParticipant).where(
            GameParticipant.game_id == game_id,
            GameParticipant.team == team,
        )
    )
    for pid in player_ids:
        if team == "red" and vold_id and pid == vold_id:
            continue
        session.add(GameParticipant(game_id=game_id, player_id=pid, team=team))
    await session.commit()

# ============== Result setters ==============
async def set_voldemort(session: AsyncSession, game_id: int, player_id: int):
    g = await session.get(Game, game_id)
    g.voldemort_id = player_id
    existing = await session.execute(
        select(GameParticipant).where(GameParticipant.game_id == game_id, GameParticipant.player_id == player_id)
    )
    if not existing.scalars().first():
        session.add(GameParticipant(game_id=game_id, player_id=player_id, team="voldemort"))
    else:
        await session.execute(
            delete(GameParticipant).where(
                GameParticipant.game_id == game_id,
                GameParticipant.player_id == player_id,
                GameParticipant.team.in_(("blue", "red")),
            )
        )
        session.add(GameParticipant(game_id=game_id, player_id=player_id, team="voldemort"))
    await session.commit()

async def set_result_type_and_killer(session: AsyncSession, game_id: int, result_type: str, killer_id: Optional[int]):
    g = await session.get(Game, game_id)
    g.result_type = result_type
    if result_type == "blue_kill":
        g.killer_id = killer_id
        g.winner = "blue"
    elif result_type == "blue_laws":
        g.killer_id = None
        g.winner = "blue"
    elif result_type in ("red_laws", "red_director"):
        g.killer_id = None
        g.winner = "red"
    await session.commit()

# ============== Ratings logic ==============
@dataclass
class TeamAverages:
    blue_avg: float
    red_avg: float

async def _team_avgs(session: AsyncSession, blue: List[Player], red: List[Player]) -> TeamAverages:
    b = sum(p.rating for p in blue) / max(len(blue), 1)
    r = sum(p.rating for p in red) / max(len(red), 1)
    return TeamAverages(blue_avg=b, red_avg=r)

def _mmr_delta(blue_avg: float, red_avg: float, winner: str) -> Tuple[int, int]:
    """
    Новые правила:
      diff ≤ 250: обе стороны ±25
      251–500:     сильная +23 / −28, слабая +28 / −23
      501–1000:    сильная +20 / −30, слабая +30 / −20
      ≥1001:       сильная +15 / −35, слабая +35 / −15
    Возвращаем (delta_blue, delta_red) с точки зрения команды-победителя `winner`.
    """
    diff = abs(blue_avg - red_avg)

    def pack(strong_win, strong_lose, weak_win, weak_lose, blue_is_strong) -> Tuple[int, int]:
        if winner == "blue":
            return (strong_win if blue_is_strong else weak_win,
                    strong_lose if not blue_is_strong else weak_lose)
        else:
            return (strong_lose if blue_is_strong else weak_lose,
                    strong_win if not blue_is_strong else weak_win)

    # определяем кто сильнее по среднему
    blue_is_strong = blue_avg > red_avg

    if diff <= 250:
        # симметричный случай
        return (25, -25) if winner == "blue" else (-25, 25)
    elif diff <= 500:
        return pack(23, -28, 28, -23, blue_is_strong)
    elif diff <= 1000:
        return pack(20, -30, 30, -20, blue_is_strong)
    else:
        return pack(15, -35, 35, -15, blue_is_strong)

def _add_social(result_type: str, blue: List[Player], red: List[Player], vold: Optional[Player], killer: Optional[Player]) -> Dict[int, Dict[str, int]]:
    """Возвращает dict{id: {field: +inc}} для единоразового применения."""
    inc: dict[int, dict[str, int]] = {}

    def add(p: Player, field: str, v: int = 1):
        inc.setdefault(p.id, {})
        inc[p.id][field] = inc[p.id].get(field, 0) + v

    if result_type == "blue_laws":
        for p in blue:
            add(p, "social_blue", 1)
            add(p, "blue_wins", 1)
    elif result_type == "blue_kill":
        for p in blue:
            add(p, "social_blue", 1)
            add(p, "blue_wins", 1)
        if killer:
            add(killer, "social_blue", 1)   # бонус за убийство
            add(killer, "killer_points", 1)
    elif result_type == "red_laws":
        for p in red:
            add(p, "social_red", 1)
            add(p, "red_wins", 1)
    elif result_type == "red_director":
        for p in red:
            add(p, "social_red", 1)
            add(p, "red_wins", 1)
        if vold:
            add(vold, "social_vold", 1)
            add(vold, "vold_wins", 1)

    return inc

def _append_game_stats(game_id: int,
                       blue: List[Player], red: List[Player],
                       avgs: TeamAverages,
                       d_blue: int, d_red: int,
                       social_inc: Dict[int, Dict[str, int]]):
    """Логирует вклад каждого игрока в игру — для «Игрок дня» и аналитики."""
    payload = _load_json(STATS_LOG_PATH)
    ts = _now_msk().isoformat()

    # для каждого игрока пишем его дельту MMR, сумму соц-полей и средний рейтинг соперников в ЭТОЙ игре
    def social_sum(pid: int) -> int:
        mp = social_inc.get(pid, {})
        # соц-очки любых типов: blue/red/vold
        return int(mp.get("social_blue", 0) + mp.get("social_red", 0) + mp.get("social_vold", 0))

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
        # красные включают и Воланда — он в red-ростере тоже
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

async def apply_ratings(session: AsyncSession, game_id: int) -> str:
    """
    Применяет MMR/социальные очки согласно результату.
    Возвращает краткую строку для лога (без «Исход» и «Средний MMR», чтобы не дублировать в bot.py).
    Параллельно пишет поигровую статистику в game_stats.json.
    """
    g = await session.get(Game, game_id)
    blue, red, vold = await get_team_rosters(session, game_id)
    if not g.result_type:
        g.result_type = "blue_laws" if g.winner == "blue" else "red_laws"
        await session.commit()

    avgs = await _team_avgs(session, blue, red)
    d_blue, d_red = _mmr_delta(avgs.blue_avg, avgs.red_avg,
                               "blue" if g.result_type.startswith("blue_") else "red")

    killer = await session.get(Player, g.killer_id) if g.killer_id else None
    inc = _add_social(g.result_type, blue, red, vold, killer)

    # применяем MMR
    for p in blue:
        p.rating += d_blue
    for p in red:
        p.rating += d_red

    # применяем соц-очки
    for pid, fields in inc.items():
        pl = await session.get(Player, pid)
        for field, val in fields.items():
            setattr(pl, field, getattr(pl, field) + val)

    await session.commit()

    # лог в файл для «Игрок дня»
    _append_game_stats(game_id, blue, red, avgs, d_blue, d_red, inc)

    # краткое резюме (без дублирующих строк)
    summary = f"Изменение MMR — Синие: {d_blue}, Красные: {d_red}\n"
    if g.result_type == "blue_kill" and killer:
        name = f"{killer.first_name}{(' ' + killer.last_name) if killer.last_name else ''}"
        summary += f"Киллер Воландеморта: {name} (+1 соц, +1 убийство)\n"
    return summary

# ============== Recompute all ==============
async def recompute_all_ratings(session: AsyncSession) -> str:
    res = await session.execute(select(Player))
    players = list(res.scalars().all())
    for p in players:
        p.rating = INITIAL_RATING
        p.blue_wins = 0
        p.red_wins = 0
        p.vold_wins = 0
        p.social_blue = 0
        p.social_red = 0
        p.social_vold = 0
        p.killer_points = 0
    await session.commit()

    resg = await session.execute(select(Game).order_by(Game.id.asc()))
    games = list(resg.scalars().all())
    # при полном пересчёте очищать исторический лог НЕ будем — это аналитика по реальным матчам
    for g in games:
        if not g.result_type and g.winner in ("blue", "red"):
            g.result_type = "blue_laws" if g.winner == "blue" else "red_laws"
            await session.commit()
        await apply_ratings(session, g.id)
    return f"Пересчитано игр: {len(games)}, игроков: {len(players)}"
