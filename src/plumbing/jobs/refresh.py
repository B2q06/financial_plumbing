import datetime as dt


import logging


import httpx


import time


import pandas as pd


from plumbing.config import STORE_LOCK


from plumbing.data.prices import NoDataForDate, fetch_bulk_prices, last_stored_date, write_day, corporate_actions, repull


from plumbing.data.master import TickerNotFound, add_ticker, master_tickers


LOCK = STORE_LOCK


log = logging.getLogger(__name__)


def fetch_with_retry(day: str) -> pd.DataFrame:
    """``fetch_bulk_prices`` with up to three attempts on network errors (30 s apart).

    Only ``httpx.TransportError`` is retried; HTTP status errors (401/402/...) propagate immediately.
    """
    for attempt in range(3):
        try:
            return fetch_bulk_prices(day)
        except httpx.TransportError:  # timeouts / connection errors only; 4xx/5xx go straight to run()
            if attempt == 2:
                raise
            log.warning("%s: transport error, retry %d/2 in 30s", day, attempt + 1)
            time.sleep(30)


def run(start: str | None = None, through: str | None = None) -> dict:
    """The daily job. Pull each day's prices into the inbox and keep the master and FIGI files current.

    Per day from ``start`` through ``through``: fetch the bulk file, add any ticker the master doesn't know,
    write the day file. Then, for each day written, repull every ticker with a split or dividend that day.
    Holds the store lock for the fetch/write phase. Default ``start`` is the frontier day itself so late rows
    are re-pulled.

    Args:
        start: ``"YYYY-MM-DD"``; default ``last_stored_date()``.
        through: ``"YYYY-MM-DD"``; default today.

    Returns:
        ``{"start", "through", "written", "skipped", "repulled"}`` — dates written/skipped, tickers repulled.

    Raises:
        FileExistsError: another job holds the store lock.
        httpx.HTTPStatusError: bad token / spent quota; logged then re-raised.
    """
    if start is None:
        start = last_stored_date()
    else:
        start = dt.date.fromisoformat(start)

    if through is None:
        through = dt.date.today()
    else:
        through = dt.date.fromisoformat(through)

    written: list[dt.date] = []
    skipped: list[dt.date] = []

    d = start

    LOCK.touch(exist_ok=False)
    log.info("start %s through %s", start, through)

    try:
        while d <= through:
            try:
                df = fetch_with_retry(d.isoformat())

                for t in sorted(set(df["ticker"]) - master_tickers()):
                    add_ticker(t)

                write_day(df)
                written.append(d)
                log.info("%s written", d)

            except NoDataForDate:
                skipped.append(d)
                log.info("%s skipped (no data)", d)

            except httpx.HTTPStatusError as e:
                # 401 bad token / 402 quota spent / 5xx after retries: nothing further will succeed today
                log.error("%s aborted: HTTP %s", d, e.response.status_code)
                raise

            d += dt.timedelta(days=1)

    finally:
        LOCK.unlink()

    repulled = []

    for date in written:
        for t in corporate_actions(date.isoformat()):
            try:
                repull(t)
            except TickerNotFound:
                log.debug("%s not in master, attempting to add ticker", t)
                add_ticker(t)
                repull(t)
            repulled.append(t)
    log.info("done: %d written, %d skipped, %d repulled", len(written), len(skipped), len(repulled))

    return {"start": start, "through": through, "written": written, "skipped": skipped, "repulled": repulled}


if __name__ == "__main__":
    from plumbing.log import setup

    setup("refresh")
    run()
