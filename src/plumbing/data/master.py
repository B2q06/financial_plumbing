"""Security master lookups: ticker -> FIGI -> file path.

Today the master is data/security_master.parquet written by scripts/seed_master.py.
When Postgres exists, only _load() changes (SELECT ticker, figi FROM security_alias WHERE valid_to IS NULL).
"""


import json


import logging


import os


import time


from concurrent.futures import ThreadPoolExecutor


import httpx


import pandas as pd


from plumbing.config import DATA, EODHD_TOKEN, OPENFIGI_TOKEN, PARQUET_BY_TICKER


log = logging.getLogger(__name__)


MASTER = DATA / "security_master.parquet"


FUND_DIR = DATA / "fundamentals"


GICS_CSV = DATA / "gics_hierarchy.csv"


OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"


MAJOR_VENUES = {"NYSE", "NASDAQ", "NYSE ARCA", "NYSE MKT", "AMEX", "BATS"}


UNIVERSE_TYPES = {"Common Stock"}


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


def symbol_list() -> pd.DataFrame:
    """Every symbol EODHD serves on US venues (1 quota unit, ~51k rows).

    Returns:
        DataFrame ``ticker, name, venue, type, isin`` sorted by ticker; ``venue`` is NYSE/NASDAQ/PINK/...,
        ``type`` is Common Stock/ETF/FUND/Preferred Stock/...
    """
    r = httpx.get(
        "https://eodhd.com/api/exchange-symbol-list/US", params={"api_token": EODHD_TOKEN, "fmt": "json"}, timeout=120
    )
    r.raise_for_status()
    df = pd.DataFrame(r.json())[["Code", "Name", "Exchange", "Type", "Isin"]]
    df.columns = ["ticker", "name", "venue", "type", "isin"]
    df = df.drop_duplicates("ticker").sort_values("ticker").reset_index(drop=True)
    log.info("symbol list: %d tickers", len(df))
    return df


def openfigi(tickers: list[str]) -> pd.DataFrame:
    """Resolve tickers to composite FIGIs via OpenFIGI, 100 per request, paced to its rate limit.

    Args:
        tickers: EODHD-style codes; share classes with a dash (``BRK-B``) are converted to OpenFIGI's slash form.

    Returns:
        DataFrame ``ticker, figi, share_class_figi, openfigi_type, figi_status`` (``"ok"`` or ``"unresolved"``),
        one row per input ticker in input order.
    """
    rows = []
    for i in range(0, len(tickers), 100):
        batch = tickers[i : i + 100]
        jobs = [{"idType": "TICKER", "idValue": t.replace("-", "/"), "exchCode": "US"} for t in batch]
        r = httpx.post(OPENFIGI_URL, json=jobs, headers={"X-OPENFIGI-APIKEY": OPENFIGI_TOKEN}, timeout=60)
        r.raise_for_status()
        for t, res in zip(batch, r.json()):
            if "data" in res:
                d = res["data"][0]
                rows.append(
                    {
                        "ticker": t,
                        "figi": d["compositeFIGI"],
                        "share_class_figi": d["shareClassFIGI"],
                        "openfigi_type": d["securityType"],
                        "figi_status": "ok",
                    }
                )
            else:
                rows.append(
                    {
                        "ticker": t,
                        "figi": None,
                        "share_class_figi": None,
                        "openfigi_type": None,
                        "figi_status": "unresolved",
                    }
                )
        log.debug("openfigi %d/%d remaining=%s", i + len(batch), len(tickers), r.headers.get("ratelimit-remaining"))
        if int(r.headers.get("ratelimit-remaining", "250")) < 5:
            time.sleep(60)
    df = pd.DataFrame(rows)
    log.info("openfigi: %d resolved, %d unresolved", (df.figi_status == "ok").sum(), (df.figi_status != "ok").sum())
    return df


def fundamentals_general(ticker: str) -> dict:
    """The ``General`` block of EODHD fundamentals for one ticker, cached as ``DATA/fundamentals/<ticker>.json``.

    10 quota units on a cache miss, free afterwards. Contains name, exchange, type, ISIN/CUSIP/CIK/LEI, the four
    GICS names, description, officers, IPO date and more.

    Args:
        ticker: EODHD code.

    Returns:
        The dict, or ``{}`` when EODHD has no fundamentals for it (funds, preferreds, some OTC).
    """
    cache = FUND_DIR / f"{ticker}.json"
    if cache.exists():
        return json.loads(cache.read_text())
    for attempt in range(6):
        r = httpx.get(
            f"https://eodhd.com/api/fundamentals/{ticker}.US",
            params={"api_token": EODHD_TOKEN, "filter": "General", "fmt": "json"},
            timeout=60,
        )
        if r.status_code == 429:  # rate limited: back off 2, 4, 8, 16, 32 s
            time.sleep(2 ** (attempt + 1))
            continue
        break
    if r.status_code == 404:
        g = {}
    else:
        r.raise_for_status()
        body = r.json()
        g = body if isinstance(body, dict) else {}
    cache.write_text(json.dumps(g))
    return g


def fundamentals(tickers: list[str]) -> pd.DataFrame:
    """GICS names and a few identity fields for many tickers, 4 requests in flight.

    Args:
        tickers: EODHD codes.

    Returns:
        DataFrame ``ticker, gic_sector, gic_group, gic_industry, gic_sub_industry, isin_fund, ipo_date,
        is_delisted, fund_category``; missing values are ``None``.
    """
    FUND_DIR.mkdir(parents=True, exist_ok=True)
    keys = {
        "GicSector": "gic_sector",
        "GicGroup": "gic_group",
        "GicIndustry": "gic_industry",
        "GicSubIndustry": "gic_sub_industry",
        "ISIN": "isin_fund",
        "IPODate": "ipo_date",
        "IsDelisted": "is_delisted",
        "Category": "fund_category",
    }
    rows = []
    done = 0
    with ThreadPoolExecutor(max_workers=4) as pool:  # 4 in flight; 8 tripped EODHD's 429 limiter
        for t, g in zip(tickers, pool.map(fundamentals_general, tickers)):
            rows.append({"ticker": t, **{v: g.get(k) for k, v in keys.items()}})
            done += 1
            if done % 500 == 0:
                log.debug("fundamentals %d/%d", done, len(tickers))
    df = pd.DataFrame(rows)
    log.info("fundamentals: %d fetched, %d with a GICS sub-industry", len(df), df.gic_sub_industry.notna().sum())
    return df


def gics_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``gics_code`` column: the 8-digit code for each row's GICS sub-industry name.

    Looks the name up in ``DATA/gics_hierarchy.csv``; falls back to industry (6), group (4), then sector (2)
    when the finer name isn't in the CSV. Rows with no GICS names get ``None``.

    Args:
        df: frame with ``gic_sector, gic_group, gic_industry, gic_sub_industry`` columns.

    Returns:
        The same frame with ``gics_code`` added.
    """
    h = pd.read_csv(GICS_CSV, dtype={"gics_code": str})
    by_name = {
        lvl: dict(zip(h.loc[h.level_type == lvl, "name"], h.loc[h.level_type == lvl, "gics_code"]))
        for lvl in h.level_type.unique()
    }
    log.debug("gics levels in csv: %s", {k: len(v) for k, v in by_name.items()})
    levels = [
        ("gic_sub_industry", "Sub_Industry"),
        ("gic_industry", "Industry"),
        ("gic_group", "Industry_Group"),
        ("gic_sector", "Sector"),
    ]

    def code(row):
        for col, lvl in levels:
            name = row.get(col)
            if name and name in by_name.get(lvl, {}):
                return by_name[lvl][name]
        return None

    df["gics_code"] = df.apply(code, axis=1)
    unmatched = df[df.gic_sub_industry.notna() & df.gics_code.isna()].gic_sub_industry.value_counts()
    if len(unmatched):
        log.warning(
            "%d GICS sub-industry names not in gics_hierarchy.csv (top): %s",
            len(unmatched),
            unmatched.head(10).to_dict(),
        )
    log.info(
        "gics: %d tickers coded (%d at sub-industry level)",
        df.gics_code.notna().sum(),
        (df.gics_code.str.len() == 8).sum(),
    )
    return df
