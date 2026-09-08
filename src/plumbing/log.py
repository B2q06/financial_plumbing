"""Logging setup. Call setup(job) once at a job's entrypoint; every module logs via logging.getLogger(__name__).

Each job gets its own file under DATA/logs/<job>.log, plus DATA/logs/all.log shared by every job, plus stdout (so docker compose logs shows it).
Line format: time  level  module  message.
"""

import logging
import sys

from plumbing.config import EODHD_TOKEN, LOGS, OPENFIGI_TOKEN

FORMAT = "%(asctime)s %(levelname)-5s %(name)s: %(message)s"
SECRETS = [t for t in (EODHD_TOKEN, OPENFIGI_TOKEN) if t]


class _Redact(logging.Filter):
    """Replace API tokens in every log record. httpx exception messages carry the full request URL,
    token included, and those get logged verbatim on failures; this is the one place that catches all of it."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for t in SECRETS:
            msg = msg.replace(t, "***")
        record.msg, record.args = msg, ()
        return True


def setup(job: str, level: int = logging.INFO) -> logging.Logger:
    """Configure logging for one job. Call once at the job's entrypoint (or the REPL).

    Attaches three destinations to the root logger: ``DATA/logs/<job>.log``, ``DATA/logs/all.log`` (every job,
    interleaved) and stdout. Every module then logs through ``logging.getLogger(__name__)``. Library loggers that
    print request URLs (httpx, httpcore, asyncio) are pinned to WARNING and a filter redacts API tokens from every
    record.

    Args:
        job: name of the job; becomes the per-job log filename.
        level: root level, ``logging.INFO`` by default; ``logging.DEBUG`` in the REPL.

    Returns:
        The logger named after the job (rarely needed; modules use their own).
    """
    LOGS.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)
    # httpx logs every request URL at INFO, token included; never let that reach a file
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)  # "Using selector" spam at DEBUG
    if root.handlers:  # already configured (e.g. called twice in a REPL)
        return logging.getLogger(job)
    fmt = logging.Formatter(FORMAT, datefmt="%Y-%m-%dT%H:%M:%S")
    handlers = (
        logging.FileHandler(LOGS / f"{job}.log"),  # this job only
        logging.FileHandler(LOGS / "all.log"),  # every job, interleaved
        logging.StreamHandler(sys.stdout),
    )
    redact = _Redact()
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(redact)
        root.addHandler(h)
    return logging.getLogger(job)
