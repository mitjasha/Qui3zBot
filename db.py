import aiosqlite
import time
from typing import Optional, Tuple, List

DB_PATH = "quizbot.sqlite3"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS config (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            allowed_chat_id INTEGER,
            allowed_thread_id INTEGER
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER,
            user_id INTEGER,
            full_name TEXT,
            username TEXT,
            updated_at INTEGER,
            PRIMARY KEY (chat_id, user_id)
        )
        """)

        # All-time score in this topic
        await db.execute("""
        CREATE TABLE IF NOT EXISTS scores (
            chat_id INTEGER,
            thread_id INTEGER,
            user_id INTEGER,
            points INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, thread_id, user_id)
        )
        """)

        # Sessions (one quiz run)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            thread_id INTEGER,
            tag TEXT,
            round_total INTEGER,
            started_at INTEGER,
            ended_at INTEGER
        )
        """)

        # Per-session scores
        await db.execute("""
        CREATE TABLE IF NOT EXISTS session_scores (
            session_id INTEGER,
            user_id INTEGER,
            points INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (session_id, user_id)
        )
        """)

        # Points events for day/week ratings
        await db.execute("""
        CREATE TABLE IF NOT EXISTS points_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER,
            chat_id INTEGER,
            thread_id INTEGER,
            session_id INTEGER,
            user_id INTEGER,
            delta INTEGER,
            reason TEXT
        )
        """)

        # Single global state (one topic bound)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            active INTEGER NOT NULL DEFAULT 0,
            current_qid INTEGER,
            winner_user_id INTEGER,
            deadline_ts INTEGER,
            tag TEXT,
            round_total INTEGER,
            round_current INTEGER,
            session_id INTEGER,
            hint_level INTEGER,
            hint_total INTEGER,
            hint_answer TEXT,
            next_hint_ts INTEGER
        )
        """)

        await db.execute("INSERT OR IGNORE INTO config(id) VALUES (1)")
        await db.execute("INSERT OR IGNORE INTO state(id) VALUES (1)")
                # lightweight migrations for existing DBs
        for sql in [
            "ALTER TABLE state ADD COLUMN session_id INTEGER",
            "ALTER TABLE state ADD COLUMN hint_level INTEGER",
            "ALTER TABLE state ADD COLUMN hint_total INTEGER",
            "ALTER TABLE state ADD COLUMN hint_answer TEXT",
            "ALTER TABLE state ADD COLUMN next_hint_ts INTEGER",
            "ALTER TABLE state ADD COLUMN tag TEXT",
            "ALTER TABLE state ADD COLUMN round_total INTEGER",
            "ALTER TABLE state ADD COLUMN round_current INTEGER"
        ]:
            try:
                await db.execute(sql)
            except Exception:
                pass

        await db.commit()

async def set_topic(chat_id: int, thread_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE config SET allowed_chat_id=?, allowed_thread_id=? WHERE id=1",
            (chat_id, thread_id),
        )
        await db.commit()

async def get_topic() -> Tuple[Optional[int], Optional[int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT allowed_chat_id, allowed_thread_id FROM config WHERE id=1")
        row = await cur.fetchone()
        return (row[0], row[1]) if row else (None, None)

async def upsert_user(chat_id: int, user_id: int, full_name: str, username: Optional[str]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO users(chat_id, user_id, full_name, username, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, user_id)
        DO UPDATE SET full_name=excluded.full_name, username=excluded.username, updated_at=excluded.updated_at
        """, (chat_id, user_id, full_name, username, int(time.time())))
        await db.commit()

async def add_points_alltime(chat_id: int, thread_id: int, user_id: int, delta: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO scores(chat_id, thread_id, user_id, points)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chat_id, thread_id, user_id)
        DO UPDATE SET points = points + excluded.points
        """, (chat_id, thread_id, user_id, delta))
        await db.commit()

async def add_points_session(session_id: int, user_id: int, delta: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO session_scores(session_id, user_id, points)
        VALUES (?, ?, ?)
        ON CONFLICT(session_id, user_id)
        DO UPDATE SET points = points + excluded.points
        """, (session_id, user_id, delta))
        await db.commit()

async def add_points_event(ts: int, chat_id: int, thread_id: int, session_id: int, user_id: int, delta: int, reason: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO points_events(ts, chat_id, thread_id, session_id, user_id, delta, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (ts, chat_id, thread_id, session_id, user_id, delta, reason))
        await db.commit()

async def top_alltime(chat_id: int, thread_id: int, limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT s.user_id, s.points, u.full_name, u.username
        FROM scores s
        LEFT JOIN users u ON u.chat_id = s.chat_id AND u.user_id = s.user_id
        WHERE s.chat_id=? AND s.thread_id=?
        ORDER BY s.points DESC
        LIMIT ?
        """, (chat_id, thread_id, limit))
        return await cur.fetchall()

async def top_session(session_id: int, chat_id_for_names: int, limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT ss.user_id, ss.points, u.full_name, u.username
        FROM session_scores ss
        LEFT JOIN users u ON u.chat_id = ? AND u.user_id = ss.user_id
        WHERE ss.session_id=?
        ORDER BY ss.points DESC
        LIMIT ?
        """, (chat_id_for_names, session_id, limit))
        return await cur.fetchall()

async def top_period(chat_id: int, thread_id: int, ts_from: int, ts_to: int, limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT pe.user_id, SUM(pe.delta) as pts, u.full_name, u.username
        FROM points_events pe
        LEFT JOIN users u ON u.chat_id = pe.chat_id AND u.user_id = pe.user_id
        WHERE pe.chat_id=? AND pe.thread_id=? AND pe.ts>=? AND pe.ts<? 
        GROUP BY pe.user_id
        ORDER BY pts DESC
        LIMIT ?
        """, (chat_id, thread_id, ts_from, ts_to, limit))
        return await cur.fetchall()

async def create_session(chat_id: int, thread_id: int, tag: str, round_total: int) -> int:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        INSERT INTO sessions(chat_id, thread_id, tag, round_total, started_at, ended_at)
        VALUES (?, ?, ?, ?, ?, NULL)
        """, (chat_id, thread_id, tag, round_total, now))
        await db.commit()
        return cur.lastrowid

async def end_session(session_id: int):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sessions SET ended_at=? WHERE session_id=?", (now, session_id))
        await db.commit()

async def set_state(**kwargs):
    # kwargs keys match columns in state; only update provided keys
    cols = []
    vals = []
    for k, v in kwargs.items():
        cols.append(f"{k}=?")
        vals.append(v)
    if not cols:
        return
    sql = "UPDATE state SET " + ", ".join(cols) + " WHERE id=1"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, tuple(vals))
        await db.commit()

async def get_state():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT active, current_qid, winner_user_id, deadline_ts,
               tag, round_total, round_current, session_id,
               hint_level, hint_total, hint_answer, next_hint_ts
        FROM state WHERE id=1
        """)
        row = await cur.fetchone()
        if not row:
            return {
                "active": False, "current_qid": None, "winner_user_id": None, "deadline_ts": None,
                "tag": "all", "round_total": None, "round_current": 0, "session_id": None,
                "hint_level": 0, "hint_total": 0, "hint_answer": None, "next_hint_ts": None
            }
        return {
            "active": bool(row[0]),
            "current_qid": row[1],
            "winner_user_id": row[2],
            "deadline_ts": row[3],
            "tag": row[4] or "all",
            "round_total": row[5],
            "round_current": row[6] or 0,
            "session_id": row[7],
            "hint_level": row[8] or 0,
            "hint_total": row[9] or 0,
            "hint_answer": row[10],
            "next_hint_ts": row[11]
        }
