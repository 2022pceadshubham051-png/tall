"""
database.py — Tagoverse Bot SQLite persistence layer.

All database access is centralized here. Uses stdlib sqlite3 wrapped with
asyncio.to_thread so callers can `await` every method without blocking the
event loop. A single module-level connection (WAL mode, thread-safe via
check_same_thread=False + a lock) is used across the bot's lifetime.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("tagoverse.database")

DB_PATH = Path("tagoverse.db")

DEFAULT_TEMPLATES = [
    "random", "animals", "gaming", "fire", "love", "nature",
    "funny", "premium", "halloween", "festival", "night",
]

DEFAULT_SETTINGS: dict[str, Any] = {
    "template": "random",
    "random_templates": True,
    "batch_size": 8,
    "delay": 3.0,
    "ignore_bots": True,
    "ignore_deleted": True,
    "vacation_mode": True,
    "progress": True,
    "admin_only": True,
}


class Database:
    """Async-friendly wrapper around a single SQLite connection."""

    def __init__(self, path: Path = DB_PATH) -> None:
        self._path = str(path)
        self._lock = asyncio.Lock()
        self._conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------ #
    # Connection / schema lifecycle
    # ------------------------------------------------------------------ #
    async def connect(self) -> None:
        def _connect() -> sqlite3.Connection:
            conn = sqlite3.connect(self._path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            return conn

        self._conn = await asyncio.to_thread(_connect)
        await self._create_tables()
        logger.info("Database connected at %s", self._path)

    async def close(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            logger.info("Database connection closed")

    async def _execute(
        self, query: str, params: tuple = (), *, fetch: str = "none", commit: bool = False
    ) -> Any:
        """Run a query in a worker thread. fetch: none|one|all."""
        assert self._conn is not None, "Database not connected"

        def _run() -> Any:
            cur = self._conn.execute(query, params)
            if commit:
                self._conn.commit()
            if fetch == "one":
                row = cur.fetchone()
                return dict(row) if row else None
            if fetch == "all":
                return [dict(r) for r in cur.fetchall()]
            return cur.lastrowid

        async with self._lock:
            return await asyncio.to_thread(_run)

    async def _executemany(self, query: str, seq: list[tuple]) -> None:
        assert self._conn is not None

        def _run() -> None:
            self._conn.executemany(query, seq)
            self._conn.commit()

        async with self._lock:
            await asyncio.to_thread(_run)

    async def _create_tables(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS groups (
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            settings TEXT NOT NULL DEFAULT '{}',
            added_at REAL NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            is_bot INTEGER NOT NULL DEFAULT 0,
            first_seen REAL NOT NULL,
            last_seen REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS group_members (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            vacation_until REAL,
            joined_at REAL NOT NULL,
            PRIMARY KEY (chat_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS statistics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER,
            command TEXT NOT NULL,
            tagged_count INTEGER NOT NULL DEFAULT 0,
            duration REAL NOT NULL DEFAULT 0,
            template TEXT,
            flood_waits INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sent_by INTEGER NOT NULL,
            target TEXT NOT NULL,
            success INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            duration REAL NOT NULL DEFAULT 0,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS restarts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reason TEXT,
            created_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_stats_chat ON statistics(chat_id);
        CREATE INDEX IF NOT EXISTS idx_stats_created ON statistics(created_at);
        CREATE INDEX IF NOT EXISTS idx_members_chat ON group_members(chat_id);
        """

        def _run() -> None:
            self._conn.executescript(schema)
            self._conn.commit()

        async with self._lock:
            await asyncio.to_thread(_run)

    # ------------------------------------------------------------------ #
    # Groups & settings
    # ------------------------------------------------------------------ #
    async def ensure_group(self, chat_id: int, title: str = "") -> dict:
        row = await self._execute(
            "SELECT * FROM groups WHERE chat_id=?", (chat_id,), fetch="one"
        )
        if row:
            if title and row["title"] != title:
                await self._execute(
                    "UPDATE groups SET title=? WHERE chat_id=?",
                    (title, chat_id), commit=True,
                )
            return row
        settings = json.dumps(DEFAULT_SETTINGS)
        await self._execute(
            "INSERT INTO groups (chat_id, title, settings, added_at, active) "
            "VALUES (?, ?, ?, ?, 1)",
            (chat_id, title, settings, time.time()), commit=True,
        )
        return await self._execute(
            "SELECT * FROM groups WHERE chat_id=?", (chat_id,), fetch="one"
        )

    async def get_settings(self, chat_id: int) -> dict:
        row = await self.ensure_group(chat_id)
        try:
            settings = json.loads(row["settings"])
        except (TypeError, json.JSONDecodeError):
            settings = {}
        merged = {**DEFAULT_SETTINGS, **settings}
        return merged

    async def update_settings(self, chat_id: int, **kwargs: Any) -> dict:
        settings = await self.get_settings(chat_id)
        settings.update(kwargs)
        await self._execute(
            "UPDATE groups SET settings=? WHERE chat_id=?",
            (json.dumps(settings), chat_id), commit=True,
        )
        return settings

    async def reset_settings(self, chat_id: int) -> dict:
        await self._execute(
            "UPDATE groups SET settings=? WHERE chat_id=?",
            (json.dumps(DEFAULT_SETTINGS), chat_id), commit=True,
        )
        return dict(DEFAULT_SETTINGS)

    async def set_group_active(self, chat_id: int, active: bool) -> None:
        await self._execute(
            "UPDATE groups SET active=? WHERE chat_id=?",
            (1 if active else 0, chat_id), commit=True,
        )

    async def all_group_ids(self, active_only: bool = True) -> list[int]:
        q = "SELECT chat_id FROM groups"
        if active_only:
            q += " WHERE active=1"
        rows = await self._execute(q, fetch="all")
        return [r["chat_id"] for r in rows]

    async def group_count(self) -> int:
        row = await self._execute("SELECT COUNT(*) c FROM groups", fetch="one")
        return row["c"] if row else 0

    # ------------------------------------------------------------------ #
    # Users & group membership
    # ------------------------------------------------------------------ #
    async def upsert_user(
        self, user_id: int, username: str = "", first_name: str = "", is_bot: bool = False
    ) -> None:
        now = time.time()
        await self._execute(
            """
            INSERT INTO users (user_id, username, first_name, is_bot, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_seen=excluded.last_seen
            """,
            (user_id, username, first_name, int(is_bot), now, now), commit=True,
        )

    async def upsert_member(
        self, chat_id: int, user_id: int, is_admin: bool = False
    ) -> None:
        await self._execute(
            """
            INSERT INTO group_members (chat_id, user_id, is_admin, joined_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET is_admin=excluded.is_admin
            """,
            (chat_id, user_id, int(is_admin), time.time()), commit=True,
        )

    async def bulk_upsert_members(self, chat_id: int, members: list[tuple[int, bool]]) -> None:
        """members: list of (user_id, is_admin)"""
        now = time.time()
        rows = [(chat_id, uid, int(is_admin), now) for uid, is_admin in members]
        await self._executemany(
            """
            INSERT INTO group_members (chat_id, user_id, is_admin, joined_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET is_admin=excluded.is_admin
            """,
            rows,
        )

    async def get_member_ids(self, chat_id: int) -> list[int]:
        rows = await self._execute(
            "SELECT user_id FROM group_members WHERE chat_id=?", (chat_id,), fetch="all"
        )
        return [r["user_id"] for r in rows]

    async def get_admin_ids(self, chat_id: int) -> list[int]:
        rows = await self._execute(
            "SELECT user_id FROM group_members WHERE chat_id=? AND is_admin=1",
            (chat_id,), fetch="all",
        )
        return [r["user_id"] for r in rows]

    async def is_admin(self, chat_id: int, user_id: int) -> bool:
        row = await self._execute(
            "SELECT is_admin FROM group_members WHERE chat_id=? AND user_id=?",
            (chat_id, user_id), fetch="one",
        )
        return bool(row and row["is_admin"])

    async def total_users(self) -> int:
        row = await self._execute("SELECT COUNT(*) c FROM users", fetch="one")
        return row["c"] if row else 0

    async def users_since(self, seconds_ago: float) -> int:
        row = await self._execute(
            "SELECT COUNT(*) c FROM users WHERE last_seen >= ?",
            (time.time() - seconds_ago,), fetch="one",
        )
        return row["c"] if row else 0

    async def all_user_ids(self) -> list[int]:
        rows = await self._execute("SELECT user_id FROM users", fetch="all")
        return [r["user_id"] for r in rows]

    # ------------------------------------------------------------------ #
    # Vacation
    # ------------------------------------------------------------------ #
    async def set_vacation(self, chat_id: int, user_id: int, until_ts: float) -> None:
        await self._execute(
            """
            INSERT INTO group_members (chat_id, user_id, is_admin, vacation_until, joined_at)
            VALUES (?, ?, 0, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET vacation_until=excluded.vacation_until
            """,
            (chat_id, user_id, until_ts, time.time()), commit=True,
        )

    async def clear_vacation(self, chat_id: int, user_id: int) -> None:
        await self._execute(
            "UPDATE group_members SET vacation_until=NULL WHERE chat_id=? AND user_id=?",
            (chat_id, user_id), commit=True,
        )

    async def get_vacation(self, chat_id: int, user_id: int) -> Optional[float]:
        row = await self._execute(
            "SELECT vacation_until FROM group_members WHERE chat_id=? AND user_id=?",
            (chat_id, user_id), fetch="one",
        )
        return row["vacation_until"] if row else None

    async def get_vacationing_ids(self, chat_id: int) -> set[int]:
        now = time.time()
        rows = await self._execute(
            "SELECT user_id FROM group_members WHERE chat_id=? AND vacation_until IS NOT NULL AND vacation_until > ?",
            (chat_id, now), fetch="all",
        )
        return {r["user_id"] for r in rows}

    async def expire_vacations(self) -> int:
        """Clear expired vacations globally; returns count cleared."""
        now = time.time()
        rows = await self._execute(
            "SELECT chat_id, user_id FROM group_members WHERE vacation_until IS NOT NULL AND vacation_until <= ?",
            (now,), fetch="all",
        )
        if rows:
            await self._execute(
                "UPDATE group_members SET vacation_until=NULL WHERE vacation_until IS NOT NULL AND vacation_until <= ?",
                (now,), commit=True,
            )
        return len(rows)

    async def total_vacationing(self) -> int:
        now = time.time()
        row = await self._execute(
            "SELECT COUNT(*) c FROM group_members WHERE vacation_until IS NOT NULL AND vacation_until > ?",
            (now,), fetch="one",
        )
        return row["c"] if row else 0

    async def vacation_expired_today(self) -> int:
        day_ago = time.time() - 86400
        row = await self._execute(
            "SELECT COUNT(*) c FROM group_members WHERE vacation_until IS NOT NULL AND vacation_until <= ? AND vacation_until >= ?",
            (time.time(), day_ago), fetch="one",
        )
        return row["c"] if row else 0

    # ------------------------------------------------------------------ #
    # Templates (selection stored inside group settings)
    # ------------------------------------------------------------------ #
    async def set_template(self, chat_id: int, template: str) -> None:
        await self.update_settings(chat_id, template=template)

    async def get_template(self, chat_id: int) -> str:
        settings = await self.get_settings(chat_id)
        return settings.get("template", "random")

    # ------------------------------------------------------------------ #
    # Statistics / tag sessions
    # ------------------------------------------------------------------ #
    async def log_session(
        self,
        chat_id: int,
        user_id: int,
        command: str,
        tagged_count: int,
        duration: float,
        template: str = "",
        flood_waits: int = 0,
        failed: int = 0,
    ) -> None:
        await self._execute(
            """
            INSERT INTO statistics
                (chat_id, user_id, command, tagged_count, duration, template, flood_waits, failed, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (chat_id, user_id, command, tagged_count, duration, template,
             flood_waits, failed, time.time()),
            commit=True,
        )

    async def group_stats(self, chat_id: int) -> dict:
        row = await self._execute(
            """
            SELECT COUNT(*) sessions,
                   COALESCE(AVG(tagged_count / NULLIF(duration, 0)), 0) avg_speed
            FROM statistics WHERE chat_id=? AND command='tagall'
            """,
            (chat_id,), fetch="one",
        )
        return row or {"sessions": 0, "avg_speed": 0}

    async def stats_since(self, seconds_ago: float, chat_id: Optional[int] = None) -> dict:
        q = "SELECT COUNT(*) c, COALESCE(SUM(flood_waits),0) fw, COALESCE(SUM(failed),0) fail FROM statistics WHERE created_at >= ?"
        params: list = [time.time() - seconds_ago]
        if chat_id is not None:
            q += " AND chat_id=?"
            params.append(chat_id)
        row = await self._execute(q, tuple(params), fetch="one")
        return row or {"c": 0, "fw": 0, "fail": 0}

    async def total_sessions(self) -> int:
        row = await self._execute("SELECT COUNT(*) c FROM statistics", fetch="one")
        return row["c"] if row else 0

    async def average_speed(self) -> float:
        row = await self._execute(
            "SELECT COALESCE(AVG(tagged_count / NULLIF(duration, 0)), 0) s FROM statistics WHERE command='tagall'",
            fetch="one",
        )
        return round(row["s"], 2) if row else 0.0

    async def average_batch_size(self) -> float:
        row = await self._execute(
            "SELECT COALESCE(AVG(tagged_count), 0) b FROM statistics WHERE command='tagall'",
            fetch="one",
        )
        return round(row["b"], 1) if row else 0.0

    async def total_flood_waits(self) -> int:
        row = await self._execute("SELECT COALESCE(SUM(flood_waits),0) c FROM statistics", fetch="one")
        return row["c"] if row else 0

    async def total_failed(self) -> int:
        row = await self._execute("SELECT COALESCE(SUM(failed),0) c FROM statistics", fetch="one")
        return row["c"] if row else 0

    async def success_rate(self) -> float:
        row = await self._execute(
            "SELECT COALESCE(SUM(tagged_count),0) t, COALESCE(SUM(failed),0) f FROM statistics",
            fetch="one",
        )
        total = (row["t"] or 0) + (row["f"] or 0)
        if not total:
            return 100.0
        return round((row["t"] / total) * 100, 2)

    async def template_usage(self) -> list[dict]:
        rows = await self._execute(
            """
            SELECT template, COUNT(*) c FROM statistics
            WHERE template IS NOT NULL AND template != ''
            GROUP BY template ORDER BY c DESC
            """,
            fetch="all",
        )
        return rows

    async def top_groups(self, limit: int = 5) -> list[dict]:
        return await self._execute(
            """
            SELECT chat_id, COUNT(*) c FROM statistics
            GROUP BY chat_id ORDER BY c DESC LIMIT ?
            """,
            (limit,), fetch="all",
        )

    async def top_admins(self, limit: int = 5) -> list[dict]:
        return await self._execute(
            """
            SELECT user_id, COUNT(*) c FROM statistics
            WHERE command IN ('tagall','tagadmins','tagrandom')
            GROUP BY user_id ORDER BY c DESC LIMIT ?
            """,
            (limit,), fetch="all",
        )

    async def top_commands(self, limit: int = 5) -> list[dict]:
        return await self._execute(
            "SELECT command, COUNT(*) c FROM statistics GROUP BY command ORDER BY c DESC LIMIT ?",
            (limit,), fetch="all",
        )

    async def largest_group(self) -> Optional[dict]:
        return await self._execute(
            """
            SELECT g.chat_id, g.title, COUNT(m.user_id) members
            FROM groups g LEFT JOIN group_members m ON g.chat_id = m.chat_id
            GROUP BY g.chat_id ORDER BY members DESC LIMIT 1
            """,
            fetch="one",
        )

    async def smallest_group(self) -> Optional[dict]:
        return await self._execute(
            """
            SELECT g.chat_id, g.title, COUNT(m.user_id) members
            FROM groups g LEFT JOIN group_members m ON g.chat_id = m.chat_id
            GROUP BY g.chat_id ORDER BY members ASC LIMIT 1
            """,
            fetch="one",
        )

    async def average_group_size(self) -> float:
        row = await self._execute(
            """
            SELECT AVG(cnt) a FROM (
                SELECT COUNT(user_id) cnt FROM group_members GROUP BY chat_id
            )
            """,
            fetch="one",
        )
        return round(row["a"], 1) if row and row["a"] else 0.0

    async def active_inactive_groups(self, days: int = 7) -> tuple[int, int]:
        since = time.time() - days * 86400
        active = await self._execute(
            "SELECT COUNT(DISTINCT chat_id) c FROM statistics WHERE created_at >= ?",
            (since,), fetch="one",
        )
        total = await self.group_count()
        act = active["c"] if active else 0
        return act, max(total - act, 0)

    # ------------------------------------------------------------------ #
    # Broadcasts
    # ------------------------------------------------------------------ #
    async def log_broadcast(
        self, sent_by: int, target: str, success: int, failed: int, duration: float
    ) -> None:
        await self._execute(
            """
            INSERT INTO broadcasts (sent_by, target, success, failed, duration, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (sent_by, target, success, failed, duration, time.time()), commit=True,
        )

    async def broadcast_history(self, limit: int = 5) -> list[dict]:
        return await self._execute(
            "SELECT * FROM broadcasts ORDER BY created_at DESC LIMIT ?",
            (limit,), fetch="all",
        )

    # ------------------------------------------------------------------ #
    # Logs & restarts (for /logs, /botstats "Errors" page)
    # ------------------------------------------------------------------ #
    async def log_event(self, level: str, message: str) -> None:
        await self._execute(
            "INSERT INTO logs (level, message, created_at) VALUES (?, ?, ?)",
            (level, message[:2000], time.time()), commit=True,
        )

    async def error_count(self, level: str = "ERROR") -> int:
        row = await self._execute(
            "SELECT COUNT(*) c FROM logs WHERE level=?", (level,), fetch="one"
        )
        return row["c"] if row else 0

    async def flood_wait_log_count(self) -> int:
        row = await self._execute(
            "SELECT COUNT(*) c FROM logs WHERE level='FLOODWAIT'", fetch="one"
        )
        return row["c"] if row else 0

    async def db_failure_count(self) -> int:
        row = await self._execute(
            "SELECT COUNT(*) c FROM logs WHERE level='DB_ERROR'", fetch="one"
        )
        return row["c"] if row else 0

    async def log_restart(self, reason: str = "manual") -> None:
        await self._execute(
            "INSERT INTO restarts (reason, created_at) VALUES (?, ?)",
            (reason, time.time()), commit=True,
        )

    async def restart_count(self) -> int:
        row = await self._execute("SELECT COUNT(*) c FROM restarts", fetch="one")
        return row["c"] if row else 0

    # ------------------------------------------------------------------ #
    # Database introspection (for /botstats "Database" page)
    # ------------------------------------------------------------------ #
    async def table_info(self) -> list[dict]:
        tables = ["groups", "users", "group_members", "statistics", "broadcasts", "logs", "restarts"]
        info = []
        for t in tables:
            row = await self._execute(f"SELECT COUNT(*) c FROM {t}", fetch="one")
            info.append({"table": t, "rows": row["c"] if row else 0})
        return info

    async def db_size_bytes(self) -> int:
        try:
            return Path(self._path).stat().st_size
        except OSError:
            return 0

    async def index_count(self) -> int:
        row = await self._execute(
            "SELECT COUNT(*) c FROM sqlite_master WHERE type='index'", fetch="one"
        )
        return row["c"] if row else 0
