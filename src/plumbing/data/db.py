"""Postgres access: one connection factory and the job_run bookkeeping every job calls."""

import json
import logging

import psycopg
from psycopg.types.json import Json

from plumbing.config import PG_DSN

log = logging.getLogger(__name__)


def connect() -> psycopg.Connection:
    """A new autocommit connection from PG_DSN. Use as ``with connect() as conn, conn.cursor() as cur:``."""
    return psycopg.connect(PG_DSN, autocommit=True)


def job_started(job: str) -> int:
    """Insert a running job_run row; returns its id for job_finished."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO job_run (job) VALUES (%s) RETURNING id", (job,))
        return cur.fetchone()[0]


def job_finished(run_id: int, summary: dict | None = None, error: str | None = None) -> None:
    """Close a job_run row: status ok (with the job's summary dict) or failed (with the error text)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE job_run SET finished = now(), status = %s, summary = %s, error = %s WHERE id = %s",
            (
                "failed" if error else "ok",
                Json(json.loads(json.dumps(summary, default=str))) if summary else None,
                error,
                run_id,
            ),
        )
