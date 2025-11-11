# config.py
from __future__ import annotations

import os
from typing import Set
from dotenv import load_dotenv
from pathlib import Path

ENV_PATH = (Path(__file__).parent / ".env").resolve()
load_dotenv(ENV_PATH, override=True)


# --- helpers ---
def env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1","true","yes","y","on"}:
        return True
    if s in {"0","false","no","n","off"}:
        return False
    return default

# === базовые настройки ===
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в .env")

# SQLite по умолчанию, асинхронный драйвер
DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///app.db").strip()

# Игровые константы
INITIAL_RATING: int = int(os.getenv("INITIAL_RATING", "3000"))
MAX_BLUE: int = int(os.getenv("MAX_BLUE", "6"))  # максимум игроков синих
# для красных лимит зашит в коде = 3

# === администраторы ===
# Можно задавать и по username (через запятую), и по user_id (через запятую).
# Пример в .env:
#   ADMIN_USERNAMES=admin1, @admin2
#   ADMIN_USER_IDS=123456789,987654321

def _parse_admin_usernames(csv: str) -> Set[str]:
    return {x.strip().lstrip("@").lower() for x in csv.split(",") if x.strip()}

def _parse_admin_ids(csv: str) -> Set[int]:
    out: Set[int] = set()
    for t in csv.split(","):
        t = t.strip()
        if not t:
            continue
        try:
            out.add(int(t))
        except ValueError:
            pass
    return out

ADMIN_USERNAMES: Set[str] = _parse_admin_usernames(os.getenv("ADMIN_USERNAMES", ""))
ADMIN_USER_IDS: Set[int] = _parse_admin_ids(os.getenv("ADMIN_USER_IDS", ""))

def is_admin(user_id: int | None, username: str | None) -> bool:
    """
    Возвращает True, если пользователь является администратором:
    - по точному совпадению ID (предпочтительно);
    - или по username (без @, в нижнем регистре).
    """
    if user_id is not None and user_id in ADMIN_USER_IDS:
        return True
    if username:
        if username.lstrip("@").lower() in ADMIN_USERNAMES:
            return True
    return False


# В тестовом боте включаем кнопку «Создать игрока», в проде — выключаем
ENABLE_ADMIN_CREATE_PLAYER: bool = env_bool("ENABLE_ADMIN_CREATE_PLAYER", default=True)


# helpers

def env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1","true","yes","y","on"}:
        return True
    if s in {"0","false","no","n","off"}:
        return False
    return default
