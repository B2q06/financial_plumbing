"""Weekly compaction: fold the inbox day files into the FIGI-keyed ticker files, then delete them.

Rule: the FIGI file wins. For each ticker, only inbox rows dated AFTER the file's last bar are appended
(repull may have rewritten the file with fresher adjustments; older inbox rows are stale by definition).
Same rule read_series uses, so reads and compaction can never disagree.

Sunday 03:00 CT via cron. First run also catches ~2,600 dead tickers from the April backfill via add_ticker.
"""

import logging
import os

import duckdb
import pandas as pd

from plumbing.config import PARQUET_BY_DATE, STORE_LOCK
from plumbing.data.db import job_finished, job_started
from plumbing.data.master import add_ticker, master_tickers, path_for

log = logging.getLogger(__name__)

# the seven columns a FIGI file holds, in order. Inbox rows carry ticker/exchange too; drop them before writing.
FILE_COLUMNS = ["date", "open", "high", "low", "close", "adjusted_close", "volume"]

# ---- SQL (DuckDB). Bind with conn.execute(sql, [params]).df(); `?` binds left to right. ----

# read_inbox(): every row of every day file as one frame. Param: the list of inbox paths.
# CAST on date because the inbox stores plain dates while read_parquet may infer TIMESTAMP on some files;
# CAST on volume because it is INT64 in some day files and DOUBLE in others (union_by_name reconciles the
# names, the CAST reconciles the types so the frame has one dtype).
SQL_READ_INBOX = """
    SELECT ticker,
           CAST(date AS DATE) AS date,
           open, high, low, close, adjusted_close,
           CAST(volume AS DOUBLE) AS volume
    FROM read_parquet(?, union_by_name=true)
    ORDER BY ticker, date
"""

# Optional, cheaper alternative for fold_one(): ask DuckDB for the file's last bar instead of reading the
# whole file into pandas just to find max(date). Param: the FIGI file path. Returns one value via fetchone()[0].
SQL_LAST_BAR = """
    SELECT CAST(max(date) AS DATE) FROM read_parquet(?)
"""

# Optional: the whole fold for one ticker in SQL, if you prefer it to pandas concat. Params, in order:
# FIGI file path, then the ticker, then the FIGI file path again (for the subquery). Returns the merged frame
# ready to write. Note the second half selects from a DataFrame *variable* named `rows` -- DuckDB can read a
# pandas frame in scope by name when you use duckdb.sql / conn.register; that's the one piece not shown yet,
# so the pandas version in the docstring is the simpler path. Kept here so you can see the shape.
SQL_FOLD_ONE = """
    SELECT date, open, high, low, close, adjusted_close, volume FROM read_parquet(?)
    UNION ALL
    SELECT date, open, high, low, close, adjusted_close, volume FROM rows
    WHERE ticker = ? AND date > (SELECT max(date) FROM read_parquet(?))
    ORDER BY date
"""


def foldable_day_files() -> list:
    """Inbox day files that are safe to fold: every one except the newest.

    The newest day is still open — tomorrow's refresh re-pulls it for late rows, and the file-wins rule would
    otherwise pin the early snapshot into the ticker files.

    Returns:
        Sorted list of paths, possibly empty.
    """
    files = sorted(PARQUET_BY_DATE.glob("*.parquet"))
    return files[:-1]


def read_inbox() -> pd.DataFrame:
    """All foldable inbox day files as one frame.

    Returns:
        DataFrame ``ticker, date, open, high, low, close, adjusted_close, volume``; ``date`` as ``datetime.date``
        to match the ticker files; empty frame when nothing is foldable.
    """
    inbox = [str(p) for p in foldable_day_files()]
    if not inbox:
        return pd.DataFrame(columns=["ticker", *FILE_COLUMNS])

    conn = duckdb.connect()
    df = conn.execute(SQL_READ_INBOX, [inbox]).df()
    df["date"] = df["date"].dt.date  # DuckDB hands back datetime64; the FIGI files hold plain dates. Match them.
    log.info("inbox: %d files, %d rows, %d tickers", len(inbox), len(df), df["ticker"].nunique())
    return df


def fold_one(ticker: str, rows: pd.DataFrame) -> tuple[int, bool]:
    """Append one ticker's new inbox rows to its FIGI file.

    File wins: only rows dated after the file's last bar are appended. A ticker with no file yet gets one made
    from its inbox rows. Atomic write.

    Args:
        ticker: must be in the master (``run`` adds unknowns first).
        rows: that ticker's inbox rows, from ``read_inbox``.

    Returns:
        ``(rows_appended, file_created)``.
    """
    final = path_for(ticker)
    rows = rows[FILE_COLUMNS].sort_values("date")

    if final.exists():
        existing = pd.read_parquet(final)
        new = rows[rows["date"] > existing["date"].max()]
        if new.empty:
            return 0, False  # nothing newer than the file: don't touch it
        out = pd.concat([existing, new], ignore_index=True)
        created = False
    else:
        new = rows  # first time we've seen this security: its inbox rows are the whole file
        out = rows
        created = True

    tmp = final.with_name(final.name + ".tmp")
    out.to_parquet(tmp, index=False)
    os.replace(tmp, final)
    log.debug("fold %s -> %s: +%d rows%s", ticker, final.name, len(new), " (created)" if created else "")
    return len(new), created


def run() -> dict:
    """The weekly job. Fold the inbox into the FIGI-keyed ticker files and delete the folded day files.

    Adds unknown tickers to the master first (per-ticker failures are logged, not fatal). Day files are deleted
    only when every ticker folded cleanly; otherwise they stay for the next run and the failures are logged.
    Holds the store lock throughout.

    Returns:
        ``{"folded", "appended", "created", "failed", "inbox_deleted"}``.

    Raises:
        FileExistsError: another job holds the store lock.
    """
    STORE_LOCK.touch(exist_ok=False)
    try:
        df = read_inbox()
        if df.empty:
            log.info("inbox empty, nothing to compact")
            return {"folded": 0, "appended": 0, "created": 0, "failed": [], "inbox_deleted": False}
        day_files = foldable_day_files()  # the newest day file stays for tomorrow's re-pull

        unknown = sorted(set(df["ticker"]) - master_tickers())
        failed: list[str] = []
        if unknown:
            log.info("%d inbox tickers not in the master, adding", len(unknown))
        for i, t in enumerate(unknown, 1):
            try:
                add_ticker(t)
            except Exception as e:  # noqa: BLE001 - a dead name must not stop the fold
                log.warning("add_ticker %s failed: %s", t, e)
                failed.append(t)
            if i % 250 == 0:
                log.info("add_ticker %d/%d", i, len(unknown))

        appended = created = folded = 0
        skip = set(failed)
        for n, (ticker, rows) in enumerate(df.groupby("ticker"), 1):
            if ticker in skip:
                continue
            try:
                k, was_created = fold_one(ticker, rows)
            except Exception as e:  # noqa: BLE001
                log.warning("fold %s failed: %s", ticker, e)
                failed.append(ticker)
                continue
            appended += k
            created += was_created
            folded += 1
            if n % 5000 == 0:
                log.info("fold %d/%d tickers", n, df["ticker"].nunique())

        if failed:
            log.error("%d tickers failed, inbox kept for next run: %s", len(failed), failed[:20])
            inbox_deleted = False
        else:
            for f in day_files:
                f.unlink()
            inbox_deleted = True
    finally:
        STORE_LOCK.unlink()

    log.info(
        "compact done: %d tickers folded, %d rows appended, %d files created, %d failed, inbox deleted=%s",
        folded,
        appended,
        created,
        len(failed),
        inbox_deleted,
    )
    return {
        "folded": folded,
        "appended": appended,
        "created": created,
        "failed": failed,
        "inbox_deleted": inbox_deleted,
    }


if __name__ == "__main__":
    from plumbing.log import setup

    setup("compact")
    run_id = job_started("compact")
    try:
        job_finished(run_id, run())
    except Exception as e:
        job_finished(run_id, error=repr(e))
        raise
