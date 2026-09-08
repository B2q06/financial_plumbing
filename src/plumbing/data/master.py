"""Security master lookups: ticker -> FIGI -> file path.

Today the master is data/security_master.parquet written by scripts/seed_master.py.
When Postgres exists, only _load() changes (SELECT ticker, figi FROM security_alias WHERE valid_to IS NULL).
"""


import logging


import os


import pandas as pd


from plumbing.config import DATA, EODHD_TOKEN, OPENFIGI_TOKEN, PARQUET_BY_TICKER


log = logging.getLogger(__name__)


MASTER = DATA / "security_master.parquet"


_figi_by_ticker: dict[str, str] | None = None


class TickerNotFound(Exception):
    """The ticker is not in the security master at all (listed since the last seed / compaction)."""


def _load() -> dict[str, str]:
    """Load the ticker -> file-key dict once and cache it at module level.

    Key is the composite FIGI when OpenFIGI resolved the ticker, else the surrogate ``UNK_<ticker>``.
    POSTGRES-TODO: becomes ``SELECT ticker, key FROM security_alias WHERE valid_to IS NULL``.
    """
    global _figi_by_ticker
    if _figi_by_ticker is None:
        df = pd.read_parquet(MASTER)
        # file key: the composite FIGI when OpenFIGI resolved it, else a surrogate UNK_<ticker> so the
        # security is still tracked (mostly preferreds and some OTC). TickerNotFound = not in the master at all.
        key = df["figi"].where(df["figi_status"] == "ok", "UNK_" + df["ticker"])
        _figi_by_ticker = dict(zip(df["ticker"], key))
        log.debug(
            "master loaded: %d tickers (%d FIGI, %d surrogate) from %s",
            len(_figi_by_ticker),
            (df["figi_status"] == "ok").sum(),
            (df["figi_status"] != "ok").sum(),
            MASTER.name,
        )
    return _figi_by_ticker


def reload() -> None:
    """Forget the cached ticker -> key dict so the next lookup re-reads the master (after a seed or add)."""
    global _figi_by_ticker
    _figi_by_ticker = None


def figi_for(ticker: str) -> str:
    """File key for a ticker: its composite FIGI, or ``UNK_<ticker>`` if OpenFIGI couldn't resolve it.

    Args:
        ticker: case-insensitive.

    Returns:
        The key string, e.g. ``"BBG000B9XRY4"``.

    Raises:
        TickerNotFound: the ticker is not in the security master at all.
    """
    ticker = ticker.upper()
    try:
        return _load()[ticker]
    except KeyError:
        raise TickerNotFound(f"{ticker} is not in the security master") from None


def path_for(ticker: str):
    """Path of a ticker's price file, ``prices_by_ticker/<key>.parquet``. May not exist yet.

    Raises:
        TickerNotFound: see ``figi_for``.
    """
    return PARQUET_BY_TICKER / f"{figi_for(ticker)}.parquet"
