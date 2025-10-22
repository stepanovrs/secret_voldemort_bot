import os, sys, re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"

JSON_FILES = [
    "applications.json",
    "auth_map.json",
    "bot_metrics.json",
    "day_list.json",
    "game_notes.json",
    "game_stats.json",
]

def read_env(path: Path) -> dict:
    data = {}
    if not path.exists():
        return data
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k.strip()] = v.strip()
    return data

def parse_sqlite_path(db_url: str):
    m = re.match(r"^sqlite(\+aiosqlite)?:///(.+)$", (db_url or "").strip(), re.I)
    if not m:
        return None
    db_rel = m.group(2)
    p = Path(db_rel)
    if not p.is_absolute():
        p = ROOT / db_rel
    return p

def main():
    if "--yes" not in sys.argv and "-y" not in sys.argv:
        print("Эта операция УДАЛИТ ВСЕ ДАННЫЕ (игры, пользователи, метрики).")
        print("Для продолжения запустите с ключом --yes")
        sys.exit(2)

    env = read_env(ENV_FILE)
    db_url = env.get("DATABASE_URL", "")
    removed = []

    # 1) Очистка SQLite файла
    db_path = parse_sqlite_path(db_url)
    if db_path:
        if db_path.exists():
            db_path.unlink()
            removed.append(f"DB: {db_path}")
        else:
            removed.append(f"DB: {db_path} (не найдено)")
    else:
        print("DATABASE_URL не SQLite — выполните TRUNCATE/DROP в вашей СУБД вручную.")
        print("Пример для Postgres (проверьте имена таблиц в db.py):")
        print("  TRUNCATE TABLE games, players, participants, ratings RESTART IDENTITY CASCADE;")

    # 2) JSON-файлы
    for name in JSON_FILES:
        p = ROOT / name
        if p.exists():
            p.unlink()
            removed.append(f"JSON: {name}")
        else:
            removed.append(f"JSON: {name} (не найдено)")

    print("\nГОТОВО. Удалено/проверено:")
    for item in removed:
        print(" -", item)

    print("\nАдмины не тронуты (берутся из .env). Все пользователи авторизуются заново.")
    print("Перезапустите бота: python bot.py")

if __name__ == "__main__":
    main()
