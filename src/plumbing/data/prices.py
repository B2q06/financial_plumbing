from pathlib import Path

import duckdb
import pandas as pd
import httpx
import datetime as dt
import logging
import os
import threading
import time

from plumbing.config import EODHD_TOKEN, PARQUET_BY_DATE, PARQUET_BY_TICKER
from plumbing.data.master import TickerNotFound, path_for

log = logging.getLogger(__name__)


def read_series(
    ticker: str,
    start: str | None = None,
    end: str | None = None,
    all_history: bool = False,
) -> pd.Series:
    """Adjusted closes for one ticker as a date-indexed Series, ascending.

    Resolves the ticker to its FIGI-keyed file through the security master and unions it with the inbox day files.
    The file wins wherever it has a date; the inbox only fills dates after the file's last bar. Delisted tickers
    not in the master fall back to ``parquet/legacy/<TICKER>.parquet``.

    Args:
        ticker: e.g. ``"SPY"``; case-insensitive.
        start: first date, ``"YYYY-MM-DD"``. Required unless ``all_history``.
        end: last date inclusive; default = no upper bound.
        all_history: return every bar on file; ``start``/``end`` ignored.

    Returns:
        ``pd.Series`` named ``close`` (this is ``adjusted_close``), index of ``datetime.date``.

    Raises:
        ValueError: no start date, malformed date, start after end, or no bars in the window.
        FileNotFoundError: no price file anywhere (not in master, not in legacy, not in the inbox).
    """

    ticker = ticker.upper()
    # FIGI-keyed file via the master. Tickers EODHD no longer lists (delisted before the 2026-09-08 seed) are not in
    # the master; their old history lives in parquet/legacy/<TICKER>.parquet, so fall back there.
    try:
        path = path_for(ticker)
    except TickerNotFound:
        path = PARQUET_BY_TICKER.parent / "legacy" / f"{ticker}.parquet"
    inbox = [str(p) for p in PARQUET_BY_DATE.glob("*.parquet")]
    has_file = path.exists()

    if not has_file and not inbox:
        raise FileNotFoundError(
            f"no price file found for ticker: {ticker}"
        )

    if all_history:
        start, end = "1900-01-01", "3000-12-31"

    elif start is None:
        raise ValueError(
            f"Start date required for {ticker}. Pass start='YYYY-MM-DD', or all_history=True for the full series."
        )

    end = end or "3000-12-31"
    # validate here so a typo is a ValueError naming the argument, not a DuckDB ConversionException
    for name, value in (("start", start), ("end", end)):
        try:
            dt.date.fromisoformat(value)
        except ValueError:
            raise ValueError(f"{name}={value!r} is not YYYY-MM-DD") from None
    if start > end:
        raise ValueError(f"start {start} is after end {end}")

    conn = duckdb.connect()
    if has_file and not inbox:
        # single tier: only the ticker file exists (inbox is empty right after compaction)
        sql = """
            SELECT CAST(date AS DATE) AS date, adjusted_close AS close
            FROM read_parquet(?)
            WHERE CAST(date AS DATE) >= ? AND CAST(date AS DATE) <= ?
            ORDER BY date ASC
        """
        params = [str(path), start, end]
    elif not has_file:
        # inbox only: a listing newer than the last compaction (no ticker file yet)
        sql = """
            SELECT CAST(date AS DATE) AS date, adjusted_close AS close
            FROM read_parquet(?, union_by_name=true)
            WHERE ticker = ? AND CAST(date AS DATE) >= ? AND CAST(date AS DATE) <= ?
            ORDER BY date ASC
        """
        params = [inbox, ticker, start, end]
    else:
        # two tiers: the ticker file wins wherever it has a date (repull rewrites it with fresh
        # adjustments); the inbox only fills dates AFTER the file's last bar. Same rule as compaction.
        sql = """
            SELECT CAST(date AS DATE) AS date, adjusted_close AS close
            FROM read_parquet(?)
            WHERE CAST(date AS DATE) >= ? AND CAST(date AS DATE) <= ?
            UNION ALL
            SELECT CAST(date AS DATE) AS date, adjusted_close AS close
            FROM read_parquet(?, union_by_name=true)
            WHERE ticker = ?
              AND CAST(date AS DATE) >= ? AND CAST(date AS DATE) <= ?
              AND CAST(date AS DATE) > (SELECT CAST(max(date) AS DATE) FROM read_parquet(?))
            ORDER BY date ASC
        """
        params = [str(path), start, end, inbox, ticker, start, end, str(path)]
    df = conn.execute(sql, params).df()
    series = df.set_index("date")["close"]
    log.debug(
        "read_series %s %s..%s -> %d bars (file=%s, inbox=%d)", ticker, start, end, len(series), has_file, len(inbox)
    )
    if series.empty:
        raise ValueError(
            f"no bars for {ticker} between {start} and {end} (file: {has_file}, inbox files: {len(inbox)})"
        )
    assert isinstance(series, pd.Series)
    return series


EODHD = "https://eodhd.com/api"
REQUESTS_PER_MINUTE = 950  # plan ceiling is ~1,000-1,200/min (X-RateLimit-Limit); stay under it, never burst


class _RateGate:
    """Let one request through every 60/REQUESTS_PER_MINUTE seconds, across all threads.

    A turnstile: workers queue on the lock, each waits until the next slot, then goes. The worker count no
    longer sets the rate; this does.
    """

    def __init__(self, per_minute: int):
        self.interval = 60.0 / per_minute
        self.lock = threading.Lock()
        self.next_slot = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            if now < self.next_slot:
                time.sleep(self.next_slot - now)
                now = time.monotonic()
            self.next_slot = now + self.interval


_gate = _RateGate(REQUESTS_PER_MINUTE)


def eodhd_get(path: str, timeout: int = 60, **params) -> httpx.Response:
    """GET one EODHD endpoint. The single door every EODHD call goes through.

    Adds the token and ``fmt=json``, waits for the shared rate gate (950 requests/min across all threads), retries
    HTTP 429 with 5/10/20/40 s backoff, and pauses briefly when the per-minute budget runs low.

    Args:
        path: endpoint path after ``https://eodhd.com/api/``, e.g. ``"eod/AAPL.US"``.
        timeout: seconds per request.
        **params: query parameters for the endpoint (``date=``, ``type=``, ``order=`` ...).

    Returns:
        The successful ``httpx.Response``; call ``.json()`` on it.

    Raises:
        httpx.HTTPStatusError: any non-429 error status (401 bad token, 402 quota spent, 404 unknown symbol),
            or a fifth consecutive 429.
        httpx.TransportError: network failure; not retried here (see ``jobs.refresh.fetch_with_retry``).
    """
    url = f"{EODHD}/{path}"
    for attempt in range(5):
        _gate.wait()
        r = httpx.get(url, params={"api_token": EODHD_TOKEN, "fmt": "json", **params}, timeout=timeout)
        if r.status_code == 429:
            wait = 5 * 2**attempt
            log.warning("%s: 429 rate limited, retry %d/4 in %ds", path, attempt + 1, wait)
            time.sleep(wait)
            continue
        r.raise_for_status()
        remaining = int(r.headers.get("X-RateLimit-Remaining", "1000"))
        if remaining < 50:
            log.debug("%s: %d requests left this minute, pausing 3s", path, remaining)
            time.sleep(3)
        return r
    r.raise_for_status()  # fifth 429 in a row: give up loudly
    return r


class NoDataForDate(Exception):
    """EODHD returned no rows for the requested date -_-"""


def fetch_bulk_prices(date: str) -> pd.DataFrame:
    """Every US ticker's bar for one trading day from EODHD's bulk endpoint (100 quota units).

    Args:
        date: ``"YYYY-MM-DD"``. Weekends, holidays and today-before-the-close return nothing.

    Returns:
        DataFrame sorted by ticker with columns ``ticker, exchange, date, open, high, low, close, adjusted_close,
        volume``; ``date`` holds ``datetime.date`` objects. Typically 45-50k rows.

    Raises:
        NoDataForDate: EODHD returned no rows for that date.
        ValueError: malformed date, or the response carried rows for a different date.
        httpx.HTTPStatusError: see ``eodhd_get``.
    """
    dt.date.fromisoformat(date)

    r = eodhd_get("eod-bulk-last-day/US", date=date)  # bad token -> 401, spent quota -> 402, 429 retried inside

    # create variable with the json data
    json_data = r.json()

    # transform json -> df
    df = pd.DataFrame(json_data)

    if df.empty:
        raise NoDataForDate(f"no data for {date}")

    # rename columns to match data schema
    df = df.rename(columns={"code": "ticker", "exchange_short_name": "exchange"})

    # drop prev_close, change and change_p
    keep = ["ticker", "exchange", "date", "open", "high", "low", "close", "adjusted_close", "volume"]
    df = df[keep]

    # set date column to actual date data type from strings
    df["date"] = pd.to_datetime(df["date"]).dt.date

    expected = dt.date.fromisoformat(date)
    if not (df["date"] == expected).all():
        raise ValueError(f"wtf? The response contains dates other than {date}")

    df = df.sort_values("ticker")
    log.info("fetch_bulk_prices %s: %d rows", date, len(df))
    assert isinstance(df, pd.DataFrame)
    return df


def write_day(df: pd.DataFrame) -> Path:
    """Write one day's bulk frame to the inbox as ``prices_by_date/YYYY-MM-DD.parquet``.

    Temp file then ``os.replace``, so a reader never sees a partial file and a re-pull of the same day simply
    replaces the earlier one.

    Args:
        df: a frame from ``fetch_bulk_prices`` (single date, any number of tickers).

    Returns:
        Path of the written file.

    Raises:
        ValueError: empty frame, or more than one date in it.
        FileNotFoundError: the inbox directory doesn't exist (create it once by hand; never auto-created).
    """
    if df.empty:
        raise ValueError("the df is empty, cannot write")

    day = pd.to_datetime(df["date"].iloc[0]).date()

    if not (df["date"] == day).all():
        raise ValueError("write_day expects a single-day")

    if not PARQUET_BY_DATE.is_dir():
        raise FileNotFoundError("ensure PARQUET_BY_DATE is set in config.")

    final = PARQUET_BY_DATE / f"{day.isoformat()}.parquet"

    # build final name
    tmp = final.with_name(final.name + ".tmp")
    # write file to tmp (for atomic lock incase read_series or other func is running)
    df.to_parquet(tmp, index=False)
    # write file to database
    os.replace(tmp, final)
    log.info("write_day %s: %d rows -> %s", day, len(df), final.name)

    return final


def last_stored_date() -> dt.date:
    """The store's frontier: the newest day we have.

    Later of the newest inbox filename and the most common last date across the ticker files. The daily refresh
    starts from this day (not the day after) so late-arriving rows for the frontier day get picked up.

    Returns:
        ``datetime.date``.
    """
    inbox_dates = []

    for p in PARQUET_BY_DATE.glob("*.parquet"):
        inbox_dates.append(dt.date.fromisoformat(p.stem))

    paths = [str(p) for p in PARQUET_BY_TICKER.glob("*.parquet")]

    sql = """
     SELECT last_bar
        FROM (
            SELECT filename, CAST(max(date) AS DATE) AS last_bar
            FROM read_parquet(?, union_by_name=true, filename=true)
            GROUP BY filename
        )
        GROUP BY last_bar
        ORDER BY count(*) DESC
        LIMIT 1
    """

    conn = duckdb.connect()
    ticker_frontier = conn.execute(sql, [paths]).fetchone()[0]

    result = max(inbox_dates + [ticker_frontier])
    log.debug(
        "last_stored_date -> %s (inbox newest %s, ticker vote %s)",
        result,
        max(inbox_dates, default=None),
        ticker_frontier,
    )
    return result


def repull(ticker: str) -> Path:
    """Replace one security's whole price file with fresh full history from EODHD (1 quota unit).

    Use after a split or dividend (adjusted history changed), to patch a series that looks wrong, or in bulk via
    ``jobs.refresh.repull_all``. Writes ``prices_by_ticker/<FIGI>.parquet`` atomically; safe to run concurrently
    for different tickers.

    Args:
        ticker: e.g. ``"AAPL"``; case-insensitive; must be in the security master.

    Returns:
        Path of the rewritten file.

    Raises:
        TickerNotFound: not in the security master.
        ValueError: EODHD returned no history.
        httpx.HTTPStatusError: see ``eodhd_get``.
    """
    ticker = ticker.upper()
    final = path_for(ticker)  # raises TickerNotFound before any network call

    r = eodhd_get(f"eod/{ticker}.US", timeout=120, order="a")
    df = pd.DataFrame(r.json())
    if df.empty:
        raise ValueError(f"EODHD returned no history for {ticker}")

    keep = ["date", "open", "high", "low", "close", "adjusted_close", "volume"]
    df = df[keep]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date").drop_duplicates("date", keep="last")

    # No store lock here: the temp+replace is atomic per file, so concurrent repulls of different tickers are safe,
    # and a reader never sees a half-written file. Compaction takes STORE_LOCK because it touches many files.
    tmp = final.with_name(final.name + ".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, final)

    log.info("repull %s -> %s: %d bars %s..%s", ticker, final.name, len(df), df["date"].iloc[0], df["date"].iloc[-1])
    return final


def corporate_actions(date: str) -> set[str]:
    """Tickers that had a split or a dividend on a date (two bulk calls, 200 quota units).

    Their adjusted history changed that day; the caller (``jobs.refresh.run``) repulls each one.

    Args:
        date: ``"YYYY-MM-DD"``.

    Returns:
        Set of ticker codes; empty on a day with none. Includes OTC names that may not be in the master.

    Raises:
        ValueError: malformed date.
        httpx.HTTPStatusError: see ``eodhd_get``.
    """
    dt.date.fromisoformat(date)

    codes: set[str] = set()
    for kind in ("splits", "dividends"):
        r = eodhd_get("eod-bulk-last-day/US", date=date, type=kind)
        codes |= {row["code"] for row in r.json()}

    log.info("corporate_actions %s: %d tickers", date, len(codes))
    return codes
