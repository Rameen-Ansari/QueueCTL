# db.py
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List

DB_PATH = "queuectl.db"

class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self.conn = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")  
        cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY,
          command TEXT NOT NULL,
          state TEXT NOT NULL, -- 'pending','processing','completed','dead'
          attempts INTEGER NOT NULL DEFAULT 0,
          max_retries INTEGER NOT NULL DEFAULT 3,
          created_at TEXT DEFAULT (datetime('now')),
          updated_at TEXT DEFAULT (datetime('now')),
          available_at TEXT  -- timestamp when job becomes available (NULL -> immediately)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_state_available ON jobs(state, available_at, created_at);")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
          key TEXT PRIMARY KEY,
          value TEXT
        );
        """)
        cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('max_retries', '3');")
        cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('backoff_base', '2');")
        self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def get_config(self, key: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM config WHERE key = ?;", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def set_config(self, key: str, value: str):
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO config(key, value) VALUES(?, ?);", (key, value))
        self.conn.commit()

    def add_job(self, command: str, job_id: Optional[str] = None, max_retries: Optional[int] = None) -> str:
        if job_id is None:
            job_id = str(uuid.uuid4())
        if max_retries is None:
            mr = self.get_config("max_retries")
            try:
                max_retries = int(mr) if mr is not None else 3
            except Exception:
                max_retries = 3
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, available_at) "
            "VALUES(?, ?, 'pending', 0, ?, datetime('now'), datetime('now'), NULL);",
            (job_id, command, max_retries)
        )
        self.conn.commit()
        return job_id

    def list_jobs(self, state: Optional[str] = None, limit: int = 100) -> List[sqlite3.Row]:
        cur = self.conn.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at LIMIT ?;", (state, limit))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?;", (limit,))
        return cur.fetchall()

    def get_counts(self) -> Dict[str,int]:
        cur = self.conn.cursor()
        cur.execute("""
          SELECT state, COUNT(*) AS cnt FROM jobs GROUP BY state;
        """)
        rows = cur.fetchall()
        result = {r["state"]: r["cnt"] for r in rows}
        for s in ("pending","processing","completed","dead"):
            result.setdefault(s, 0)
        return result

    def get_job(self, job_id: str) -> Optional[sqlite3.Row]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
        return cur.fetchone()

    def claim_job(self) -> Optional[Dict[str,Any]]:
        """
        Atomically select one pending job that is available and mark it processing and increment attempts.
        Returns the job dict or None if none available.
        """
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE;")
            cur.execute("""
              SELECT id FROM jobs
              WHERE state = 'pending'
                AND (available_at IS NULL OR available_at <= datetime('now'))
              ORDER BY created_at
              LIMIT 1;
            """)
            row = cur.fetchone()
            if not row:
                cur.execute("COMMIT;")
                return None
            job_id = row["id"]
            cur.execute("""
              UPDATE jobs
              SET state='processing', attempts = attempts + 1, updated_at = datetime('now')
              WHERE id = ?;
            """, (job_id,))
            cur.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,))
            job = cur.fetchone()
            cur.execute("COMMIT;")
            return dict(job) if job else None
        except Exception:
            try:
                cur.execute("ROLLBACK;")
            except Exception:
                pass
            return None

    def set_job_completed(self, job_id: str):
        cur = self.conn.cursor()
        cur.execute("""
          UPDATE jobs
          SET state='completed', updated_at = datetime('now')
          WHERE id = ?;
        """, (job_id,))
        self.conn.commit()

    def set_job_dead(self, job_id: str):
        cur = self.conn.cursor()
        cur.execute("""
          UPDATE jobs
          SET state='dead', updated_at = datetime('now')
          WHERE id = ?;
        """, (job_id,))
        self.conn.commit()

    def reschedule_job_with_backoff(self, job_id: str, attempts: int, backoff_base: int):
        """
        attempts here is the attempts count AFTER increment (so 1..)
        compute delay = backoff_base ** attempts (seconds)
        set state back to pending and available_at accordingly
        """
        delay = int(backoff_base) ** int(attempts)
        cur = self.conn.cursor()
        cur.execute(f"""
          UPDATE jobs
          SET state='pending', available_at = datetime('now', '+{delay} seconds'), updated_at = datetime('now')
          WHERE id = ?;
        """, (job_id,))
        self.conn.commit()

    def reset_job_to_pending(self, job_id: str):
        cur = self.conn.cursor()
        cur.execute("""
          UPDATE jobs
          SET state='pending', attempts=0, available_at = NULL, updated_at = datetime('now')
          WHERE id = ?;
        """, (job_id,))
        self.conn.commit()

    def delete_job(self, job_id: str):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM jobs WHERE id = ?;", (job_id,))
        self.conn.commit()
