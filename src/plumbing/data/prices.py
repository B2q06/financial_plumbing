from pathlib import Path


import duckdb


import pandas as pd


import httpx


import datetime as dt


import logging


import os


from plumbing.config import EODHD_TOKEN, PARQUET_BY_DATE, PARQUET_BY_TICKER


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
