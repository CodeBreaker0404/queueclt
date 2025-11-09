# queue/db.py
import sqlite3
import threading
from typing import Optional, List, Dict, Any
from .job import Job
from .utils import logger
from .config import DB_PATH

_lock = threading.Lock()

class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._local = threading.local()
        self._ensure_tables()

    def _conn(self):
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    def _ensure_tables(self):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT,
                payload TEXT,
                is_dynamic INTEGER,
                state TEXT,
                attempts INTEGER,
                max_retries INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id TEXT PRIMARY KEY,
                command TEXT,
                payload TEXT,
                attempts INTEGER,
                max_retries INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.commit()

    # CRUD helpers (thread-safe by sqlite locking + module-level lock)
    def insert_job(self, job: Job):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs (id, command, payload, is_dynamic, state, attempts, max_retries, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (job.id, job.command, job.payload, 1 if job.is_dynamic else 0, job.state, job.attempts, job.max_retries, job.created_at, job.updated_at))
        conn.commit()

    def update_job(self, job: Job):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE jobs SET command=?, payload=?, is_dynamic=?, state=?, attempts=?, max_retries=?, created_at=?, updated_at=?
            WHERE id=?
        """, (job.command, job.payload, 1 if job.is_dynamic else 0, job.state, job.attempts, job.max_retries, job.created_at, job.updated_at, job.id))
        conn.commit()

    def delete_job(self, job_id: str):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs")
        return [dict(r) for r in cur.fetchall()]

    def fetch_job_by_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def fetch_next_pending_job(self) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor()
        try:
            # take lock via BEGIN IMMEDIATE to avoid race conditions
            cur.execute("BEGIN IMMEDIATE")
            cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC LIMIT 1", ("pending",))
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            job_dict = dict(row)
            # mark as processing immediately
            cur.execute("UPDATE jobs SET state=? WHERE id=?", ("processing", job_dict["id"]))
            conn.commit()
            return job_dict
        except sqlite3.OperationalError as e:
            logger.error(f"DB fetch lock error: {e}")
            return None

    # DLQ operations
    def add_to_dlq(self, job: Job):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO dlq (id, command, payload, attempts, max_retries, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (job.id, job.command, job.payload, job.attempts, job.max_retries, job.created_at, job.updated_at))
        # also delete from jobs table
        cur.execute("DELETE FROM jobs WHERE id=?", (job.id,))
        conn.commit()

    def list_dlq(self) -> List[Dict[str, Any]]:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM dlq")
        return [dict(r) for r in cur.fetchall()]

    def restore_dlq(self, job_id: str) -> bool:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
        row = cur.fetchone()
        if not row:
            return False
        d = dict(row)
        # move back to jobs
        cur.execute("""
            INSERT OR REPLACE INTO jobs (id, command, payload, is_dynamic, state, attempts, max_retries, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d["id"], d["command"], d.get("payload"), 0, "pending", d.get("attempts", 0), d.get("max_retries", 3), d.get("created_at"), d.get("updated_at")))
        cur.execute("DELETE FROM dlq WHERE id=?", (job_id,))
        conn.commit()
        return True

    def delete_dlq(self, job_id: str):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM dlq WHERE id=?", (job_id,))
        conn.commit()

# Module-level DB instance & simple wrappers
_db = Database(DB_PATH)

def init_db():
    # no-op because Database() already ensures tables, but keep API
    return

def insert_job(job: Job):
    _db.insert_job(job)

def update_job(job: Job):
    _db.update_job(job)

def delete_job(job_id: str):
    _db.delete_job(job_id)

def fetch_jobs():
    return _db.fetch_jobs()

def fetch_job_by_id(job_id: str):
    return _db.fetch_job_by_id(job_id)

def fetch_next_pending_job():
    return _db.fetch_next_pending_job()

def add_to_dlq(job: Job):
    _db.add_to_dlq(job)

def list_dlq():
    return _db.list_dlq()

def restore_dlq(job_id: str):
    return _db.restore_dlq(job_id)

def delete_dlq(job_id: str):
    _db.delete_dlq(job_id)
