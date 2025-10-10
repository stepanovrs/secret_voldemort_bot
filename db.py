from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from sqlalchemy import (
    Integer, String, DateTime, ForeignKey, Text, func, select
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from config import DATABASE_URL, INITIAL_RATING

# --- корректный МСК (Windows -> pip install tzdata) ---
try:
    from zoneinfo import ZoneInfo
    MSK = ZoneInfo("Europe/Moscow")
except Exception:
    MSK = timezone(timedelta(hours=3))


class Base(DeclarativeBase):
    pass


class Player(Base):
    __tablename__ = "players"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(64), nullable=False)
    last_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)

    rating: Mapped[int] = mapped_column(Integer, nullable=False, default=INITIAL_RATING)

    blue_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    red_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    vold_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    social_blue: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    social_red:  Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    social_vold: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    killer_points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(MSK),
        server_default=func.now(),
    )

    participants: Mapped[List["GameParticipant"]] = relationship(back_populates="player", cascade="all,delete")


class Game(Base):
    __tablename__ = "games"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(128), nullable=False)

    # ВАЖНО: поле, из-за которого падало — делаем его частью модели
    created_by_id: Mapped[int] = mapped_column(Integer, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(MSK),
        server_default=func.now(),
    )

    result_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # blue_laws | blue_kill | red_laws | red_director
    voldemort_id: Mapped[Optional[int]] = mapped_column(ForeignKey("players.id", ondelete="SET NULL"), nullable=True)
    killer_id:    Mapped[Optional[int]] = mapped_column(ForeignKey("players.id", ondelete="SET NULL"), nullable=True)

    participants: Mapped[List["GameParticipant"]] = relationship(back_populates="game", cascade="all,delete-orphan")


class GameParticipant(Base):
    __tablename__ = "game_participants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id:   Mapped[int] = mapped_column(ForeignKey("games.id", ondelete="CASCADE"), index=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.id", ondelete="CASCADE"), index=True)
    team: Mapped[str] = mapped_column(String(8), nullable=False)  # 'blue' | 'red'

    game:   Mapped["Game"]   = relationship(back_populates="participants")
    player: Mapped["Player"] = relationship(back_populates="participants")


engine = create_async_engine(DATABASE_URL, echo=False, future=True)
Session: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)


def now_msk() -> datetime:
    return datetime.now(MSK)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ===== CRUD =====

async def create_player(session: AsyncSession, first_name: str, last_name: Optional[str], username: Optional[str]) -> Player:
    p = Player(first_name=first_name, last_name=last_name, username=username, rating=INITIAL_RATING)
    session.add(p)
    await session.commit()
    await session.refresh(p)
    return p


async def update_player_name(session: AsyncSession, player_id: int, first: str, last: Optional[str]) -> bool:
    p = await session.get(Player, player_id)
    if not p:
        return False
    p.first_name, p.last_name = first, last
    await session.commit()
    return True


async def delete_player_if_no_games(session: AsyncSession, player_id: int) -> tuple[bool, str]:
    q = await session.execute(
        select(GameParticipant).where(GameParticipant.player_id == player_id)
    )
    if q.scalars().first():
        return False, "Нельзя удалить: игрок уже участвовал в играх."
    p = await session.get(Player, player_id)
    if not p:
        return False, "Игрок не найден."
    await session.delete(p)
    await session.commit()
    return True, ""


async def create_game(session: AsyncSession, title: str, user_id: int) -> Game:
    """Создаёт игру, обязательно заполняя created_by_id и created_at (МСК)."""
    g = Game(
        title=title,
        created_by_id=int(user_id),
        created_at=now_msk(),
    )
    session.add(g)
    await session.commit()
    await session.refresh(g)
    return g


async def delete_game(session: AsyncSession, game_id: int) -> None:
    g = await session.get(Game, game_id)
    if g:
        await session.delete(g)  # каскадно удалятся участники
        await session.commit()


async def get_game(session: AsyncSession, game_id: int) -> Optional[Game]:
    return await session.get(Game, game_id)


async def list_all_games(session: AsyncSession) -> List[Game]:
    res = await session.execute(select(Game).order_by(Game.created_at.desc(), Game.id.desc()))
    return list(res.scalars().all())


# helpers для services.py

async def set_participants(session: AsyncSession, game_id: int, team: str, player_ids: List[int]) -> None:
    old = await session.execute(
        select(GameParticipant).where(GameParticipant.game_id == game_id, GameParticipant.team == team)
    )
    for row in old.scalars().all():
        await session.delete(row)
    for pid in player_ids:
        session.add(GameParticipant(game_id=game_id, player_id=pid, team=team))
    await session.commit()


async def fetch_participants(session: AsyncSession, game_id: int) -> Tuple[List[Player], List[Player]]:
    b_ids = await session.execute(
        select(GameParticipant.player_id).where(GameParticipant.game_id == game_id, GameParticipant.team == "blue")
    )
    r_ids = await session.execute(
        select(GameParticipant.player_id).where(GameParticipant.game_id == game_id, GameParticipant.team == "red")
    )

    blues, reds = [], []
    ids = [i[0] for i in b_ids.all()]
    if ids:
        res = await session.execute(select(Player).where(Player.id.in_(ids)))
        blues = list(res.scalars().all())

    ids = [i[0] for i in r_ids.all()]
    if ids:
        res = await session.execute(select(Player).where(Player.id.in_(ids)))
        reds = list(res.scalars().all())

    blues.sort(key=lambda p: (p.first_name, p.last_name or ""))
    reds.sort(key=lambda p: (p.first_name, p.last_name or ""))
    return blues, reds
