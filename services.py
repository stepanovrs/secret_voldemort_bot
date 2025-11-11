
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from pathlib import Path
from datetime import datetime, timedelta, timezone
import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import INITIAL_RATING, MAX_BLUE
from db import Player, Game, GameParticipant

# ---- MSK time helper ----
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

# ===== JSON log path for per-player stats =====
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
    """Return (blue_players, red_players, voldemort_player). Red list includes team in ('red','voldemort')."""
    g = await session.get(Game, game_id)
    if not g:
        return [], [], None
    res = await session.execute(select(GameParticipant).where(GameParticipant.game_id == game_id))
    parts = list(res.scalars().all())
    blue_ids = [p.player_id for p in parts if p.team == 'blue']
    red_ids = [p.player_id for p in parts if p.team in ('red','voldemort')]
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

def _extend_red_with_vold(red: List[Player], vold: Optional[Player]) -> List[Player]:
    """Return red side list that always contains Voldemort (if chosen), without duplicates."""
    if vold is None:
        return list(red)
    if any(p.id == vold.id for p in red):
        return list(red)
    return list(red) + [vold]

def _team_avgs(blue: List[Player], red: List[Player]) -> TeamAverages:
    b = sum(int(p.rating) for p in blue) / max(len(blue), 1)
    r = sum(int(p.rating) for p in red) / max(len(red), 1)
    return TeamAverages(b, r)

# ================= Core MMR =================
def _mmr_delta(blue_avg: float, red_avg: float, winner: str) -> Tuple[int, int]:
    """
    Basic MMR delta formula.
    diff = abs(blue_avg - red_avg)
    x = floor(diff/10). If diff > 400 then x_eff = 41 (as for 410+).
    If stronger team wins: strong +(51-x_eff), weak -(49-x_eff).
    If weaker team wins: weak +(51+x_eff), strong -(49+x_eff).
    """
    diff = abs(blue_avg - red_avg)
    x = int(diff // 10)
    x_eff = 41 if x > 40 else x

    blue_is_strong = blue_avg >= red_avg

    if winner == 'blue':
        if blue_is_strong:
            delta_blue = 51 - x_eff
            delta_red  = -(49 - x_eff)
        else:
            delta_blue = 51 + x_eff
            delta_red  = -(49 + x_eff)
    else:  # winner == 'red'
        if not blue_is_strong:          # red is strong
            delta_red  = 51 - x_eff
            delta_blue = -(49 - x_eff)
        else:                            # red is weak
            delta_red  = 51 + x_eff
            delta_blue = -(49 + x_eff)

    return int(delta_blue), int(delta_red)

# ================= Social points =================
def _add_social(result_type: str, blue: List[Player], red: List[Player], killer: Optional[Player], vold: Optional[Player] = None) -> Dict[int, Dict[str, int]]:
    inc: Dict[int, Dict[str, int]] = {}

    def add(p: Player, field: str, v: int = 1):
        d = inc.setdefault(p.id, {})
        d[field] = d.get(field, 0) + v

    if result_type.startswith('blue_'):
        for p in blue:
            add(p, 'social_blue', 1)
        if killer:
            add(killer, 'killer_points', 1)
    else:
        for p in red:
            add(p, 'social_red', 1)
        if killer:
            add(killer, 'killer_points', 1)
    # Separate credit to Voldemort when elected director
    if result_type == 'red_director' and vold is not None:
        add(vold, 'social_vold', 1)
    return inc

# ================= Galleons =================
def _win_streak_bonus(streak: int) -> int:
    if streak == 2: return 2
    if streak == 3: return 4
    if streak == 4: return 8
    if streak == 5: return 16
    if streak == 6: return 32
    if streak >= 7: return 100
    return 0

def _lose_streak_bonus(streak: int) -> int:
    if streak == 2: return 2
    if streak == 4: return 4
    if streak == 6: return 6
    if streak > 6: return 6
    return 0

async def _apply_galleons_for_game(session: AsyncSession, g: Game, blue: List[Player], red: List[Player], vold: Optional[Player], killer: Optional[Player]) -> None:
    winner = 'blue' if (g.result_type or '').startswith('blue_') else 'red'

    participants: list[Player] = []
    seen = set()
    def add(p: Optional[Player]):
        if p and p.id not in seen:
            participants.append(p); seen.add(p.id)
    for p in blue: add(p)
    for p in red: add(p)
    add(vold)

    for p in participants:
        p.galleons_balance = int(getattr(p, 'galleons_balance', 0)) + 1

    if vold:
        vold.galleons_balance = int(vold.galleons_balance) + 3

    if killer:
        killer.galleons_balance = int(killer.galleons_balance) + 5

    red_ext: list[Player] = list(red)
    if vold and all(p.id != vold.id for p in red_ext):
        red_ext.append(vold)
    if winner == 'blue':
        winners, losers = blue, red_ext
    else:
        winners, losers = red_ext, blue

    for p in winners:
        p.galleons_balance = int(p.galleons_balance) + 1
        p.win_streak = int(getattr(p, 'win_streak', 0) or 0) + 1
        p.lose_streak = 0
        p.galleons_balance = int(p.galleons_balance) + _win_streak_bonus(p.win_streak)

    for p in losers:
        p.lose_streak = int(getattr(p, 'lose_streak', 0) or 0) + 1
        p.win_streak = 0
        p.galleons_balance = int(p.galleons_balance) + _lose_streak_bonus(p.lose_streak)

    await session.commit()

# ================== Apply ratings ==================
def _append_game_stats(game_id: int, blue: List[Player], red: List[Player], avgs: TeamAverages, d_blue: int, d_red: int, inc: Dict[int, Dict[str, int]]):
    payload = _load_json(STATS_LOG_PATH)
    ts = _now_msk().isoformat()
    def social_sum(pid: int) -> int:
        return sum(inc.get(pid, {}).values()) if pid in inc else 0

    for p in blue:
        payload.append({
            'game_id': game_id,
            'player_id': p.id,
            'side': 'blue',
            'mmr_delta': d_blue,
            'social_gain': social_sum(p.id),
            'opponent_avg': float(avgs.red_avg),
            'ts': ts,
        })
    for p in red:
        payload.append({
            'game_id': game_id,
            'player_id': p.id,
            'side': 'red',
            'mmr_delta': d_red,
            'social_gain': social_sum(p.id),
            'opponent_avg': float(avgs.blue_avg),
            'ts': ts,
        })
    _save_json(STATS_LOG_PATH, payload)

async def set_team_roster(session: AsyncSession, game_id: int, team: str, player_ids: List[int]) -> None:
    res = await session.execute(select(GameParticipant).where(GameParticipant.game_id == game_id, GameParticipant.team == team))
    for row in list(res.scalars().all()):
        await session.delete(row)
    for pid in player_ids:
        session.add(GameParticipant(game_id=game_id, player_id=pid, team=team))
    await session.commit()

async def validate_rosters(*args) -> Tuple[bool, str]:
    if len(args) == 2 and isinstance(args[0], AsyncSession):
        session, game_id = args
        blue, red, vold = await get_team_rosters(session, int(game_id))
    else:
        blue, red = args[0], args[1]
        vold = args[2] if len(args) >= 3 else None

    if not blue or not red:
        return False, 'Добавьте игроков в обе команды.'
    if len(blue) > MAX_BLUE:
        return False, f'Макс. синих: {MAX_BLUE}'
    if vold is None:
        return False, 'Выберите Воландеморта.'
    if any(p.id == getattr(vold, 'id', -1) for p in blue):
        return False, 'Воландеморт не может быть в Ордене Феникса.'
    return True, 'Составы корректны.'

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
    g = await session.get(Game, game_id)
    if not g or not g.result_type:
        return 'Игра не завершена.'

    blue, red, vold = await get_team_rosters(session, game_id)
    red_ext = _extend_red_with_vold(red, vold)
    killer = await session.get(Player, g.killer_id) if g.killer_id else None
    avgs = _team_avgs(blue, red_ext)

    winner = 'blue' if g.result_type.startswith('blue_') else 'red'
    d_blue, d_red = _mmr_delta(avgs.blue_avg, avgs.red_avg, winner)

    inc = _add_social(g.result_type, blue, red, killer, vold)

    for p in blue:
        p.rating = int(p.rating) + d_blue
    seen = set()
    for p in red_ext:
        if p.id in seen:
            continue
        seen.add(p.id)
        p.rating = int(p.rating) + d_red

    for pid, fields in inc.items():
        pl = await session.get(Player, pid)
        for field, v in fields.items():
            setattr(pl, field, int(getattr(pl, field)) + int(v))

    await session.commit()

    await _apply_galleons_for_game(session, g, blue, red, vold, killer)

    _append_game_stats(game_id, blue, red_ext, avgs, d_blue, d_red, inc)

    fav = 'Орден Феникса' if avgs.blue_avg >= avgs.red_avg else 'Пожиратели'
    result = g.result_type or ''
    side = 'Орден Феникса' if result.startswith('blue_') else 'Пожиратели'
    if result == 'red_director':
        head = 'Игра завершена.\nПобеда Пожирателей — Воландеморт избран директором\n'
    else:
        try:
            nlaws = int(result.split('_', 1)[1])
        except Exception:
            nlaws = 5
        color = 'синих' if side == 'Орден Феникса' else 'красных'
        head = f'Игра завершена.\nПобеда {side} — выложены {nlaws} {color} законов\n'
    text = (
        head +
        f'Средний MMR — Орден Феникса: {avgs.blue_avg:.1f}, Красные: {avgs.red_avg:.1f}\n'
        f'Фаворит матча: {fav}\n'
        f'Изменение MMR — Синие: {d_blue}, Красные: {d_red}'
    )
    return text

# ============= Recomputation utilities =============
async def recompute_all_ratings(session: AsyncSession) -> str:
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
        res_parts = await session.execute(select(GameParticipant).where(GameParticipant.game_id == g.id))
        parts = list(res_parts.scalars().all())
        blue_ids = [p.player_id for p in parts if p.team == 'blue']
        red_ids  = [p.player_id for p in parts if p.team in ('red','voldemort')]
        blue, red = [], []
        if blue_ids:
            resb = await session.execute(select(Player).where(Player.id.in_(blue_ids)))
            blue = list(resb.scalars().all())
        if red_ids:
            resr = await session.execute(select(Player).where(Player.id.in_(red_ids)))
            red = list(resr.scalars().all())

        vold = await session.get(Player, g.voldemort_id) if g.voldemort_id else None
        red_ext = _extend_red_with_vold(red, vold)

        avgs = _team_avgs(blue, red_ext)
        winner = 'blue' if g.result_type.startswith('blue_') else 'red'
        d_blue, d_red = _mmr_delta(avgs.blue_avg, avgs.red_avg, winner)
        inc = _add_social(
            g.result_type, blue, red,
            await session.get(Player, g.killer_id) if g.killer_id else None,
            vold
        )

        for p in blue:
            p.rating = int(p.rating) + d_blue
        seen = set()
        for p in red_ext:
            if p.id in seen:
                continue
            seen.add(p.id)
            p.rating = int(p.rating) + d_red
        for pid, fields in inc.items():
            pl = await session.get(Player, pid)
            for field, v in fields.items():
                setattr(pl, field, int(getattr(pl, field)) + int(v))
        await session.commit()

    await recompute_win_counters(session)
    return f'Пересчитано игр: {len(games)}; игроков: {len(players)}'

async def recompute_all_galleons(session: AsyncSession) -> str:
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
    return f'Пересчитано игр: {len(games)}; игроков: {len(players)}; покупок учтено: {len(all_purchases)}'

# --------- NEW: recompute per-side win counters ---------
async def recompute_win_counters(session: AsyncSession) -> str:
    res = await session.execute(select(Player))
    players = list(res.scalars().all())
    for p in players:
        p.blue_wins = 0
        p.red_wins = 0
        p.vold_wins = 0
    await session.commit()

    resg = await session.execute(select(Game).where(Game.result_type.is_not(None)).order_by(Game.id.asc()))
    games = list(resg.scalars().all())

    for g in games:
        resp = await session.execute(select(GameParticipant).where(GameParticipant.game_id == g.id))
        parts = list(resp.scalars().all())
        blue_ids = [gp.player_id for gp in parts if gp.team == 'blue']
        red_ids  = [gp.player_id for gp in parts if gp.team == 'red']
        vold_part_ids = [gp.player_id for gp in parts if gp.team == 'voldemort']
        vold_id = g.voldemort_id or (vold_part_ids[0] if vold_part_ids else None)

        winner = 'blue' if g.result_type.startswith('blue_') else 'red'

        if winner == 'blue':
            for pid in blue_ids:
                pl = await session.get(Player, pid)
                if pl:
                    pl.blue_wins = int(getattr(pl, 'blue_wins', 0)) + 1
        else:
            for pid in red_ids:
                if vold_id is not None and pid == vold_id:
                    continue
                pl = await session.get(Player, pid)
                if pl:
                    pl.red_wins = int(getattr(pl, 'red_wins', 0)) + 1
            if vold_id is not None:
                pl = await session.get(Player, vold_id)
                if pl:
                    pl.vold_wins = int(getattr(pl, 'vold_wins', 0)) + 1

    await session.commit()
    return f'Счётчики побед обновлены из {len(games)} игр для {len(players)} игроков.'

# ============= Search helper =============
async def search_players(session: AsyncSession, query: str) -> List[Player]:
    q = (query or '').strip().lower()
    if not q:
        return []
    res = await session.execute(select(Player))
    players = list(res.scalars().all())
    def matches(p: Player) -> bool:
        items = [
            (p.first_name or '').lower(),
            (p.last_name or '').lower(),
            (p.username or '').lower(),
        ]
        return any(q in s for s in items)
    return [p for p in players if matches(p)]

# ============= Streaks =============
async def get_player_streaks(session: AsyncSession, player_id: int) -> Dict[str, int]:
    res = await session.execute(
        select(GameParticipant).where(GameParticipant.player_id == player_id).order_by(GameParticipant.game_id.asc())
    )
    parts = list(res.scalars().all())

    gp_game_ids = {gp.game_id for gp in parts}

    resg = await session.execute(select(Game).where(Game.voldemort_id == player_id).order_by(Game.id.asc()))
    vold_games = list(resg.scalars().all())

    entries = []

    for gp in parts:
        g = await session.get(Game, gp.game_id)
        if not g or not g.result_type:
            continue
        winner = 'blue' if g.result_type.startswith('blue_') else 'red'
        side = 'blue' if gp.team == 'blue' else 'red'
        entries.append((gp.game_id, side, winner))

    for g in vold_games:
        if not g or not g.result_type:
            continue
        if g.id in gp_game_ids:
            continue
        winner = 'blue' if g.result_type.startswith('blue_') else 'red'
        side = 'red'
        entries.append((g.id, side, winner))

    entries.sort(key=lambda x: x[0])

    if not entries:
        return {'max_win': 0, 'max_lose': 0, 'cur_win': 0, 'cur_lose': 0}

    cur_w = cur_l = max_w = max_l = 0
    for _, side, winner in entries:
        win = (side == winner)
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

    return {'max_win': max_w, 'max_lose': max_l, 'cur_win': cur_w, 'cur_lose': cur_l}
