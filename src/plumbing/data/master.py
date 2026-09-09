"""Security master lookups: ticker -> FIGI -> file path.

Today the master is data/security_master.parquet written by scripts/seed_master.py.
When Postgres exists, only _load() changes (SELECT ticker, figi FROM security_alias WHERE valid_to IS NULL).
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pandas as pd
from psycopg.types.json import Json

from plumbing.config import DATA, EODHD_TOKEN, OPENFIGI_TOKEN, PARQUET_BY_TICKER
from plumbing.data.db import connect

log = logging.getLogger(__name__)

MASTER = DATA / "security_master.parquet"  # legacy seed output; Postgres is the source of truth now
FUND_DIR = DATA / "fundamentals"
GICS_CSV = DATA / "gics_hierarchy.csv"
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
MAJOR_VENUES = {"NYSE", "NASDAQ", "NYSE ARCA", "NYSE MKT", "AMEX", "BATS"}
UNIVERSE_TYPES = {"Common Stock"}

_figi_by_ticker: dict[str, str] | None = None


class TickerNotFound(Exception):
    """The ticker is not in the security master at all (listed since the last seed / compaction)."""


def _load() -> dict[str, str]:
    """Load the ticker -> file-key dict once (current aliases from Postgres) and cache it at module level.

    Key is the composite FIGI when OpenFIGI resolved the ticker, else the surrogate ``UNK_<ticker>``.
    """
    global _figi_by_ticker
    if _figi_by_ticker is None:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT ticker, key FROM security_alias WHERE valid_to IS NULL")
            _figi_by_ticker = dict(cur.fetchall())
        log.debug("master loaded: %d tickers from postgres", len(_figi_by_ticker))
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


# ---- building the master: used by scripts/seed_master.py (all tickers) and add_ticker (one) ----


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
    """The ``General`` block of EODHD fundamentals for one ticker.

    Postgres first (``fundamentals`` table); on a miss, one EODHD call (10 quota units), then stored: in Postgres
    when the ticker is already in the master, else as ``DATA/fundamentals/<ticker>.json`` for ``load_master``
    to pick up (the seed path on a fresh machine, before any security rows exist).

    Args:
        ticker: EODHD code.

    Returns:
        The dict, or ``{}`` when EODHD has no fundamentals for it (funds, preferreds, some OTC).
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT f.general FROM fundamentals f JOIN security_alias a USING (key) WHERE a.ticker = %s AND a.valid_to IS NULL",
            (ticker,),
        )
        hit = cur.fetchone()
    if hit:
        return hit[0]

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

    if g and ticker in _load():
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO fundamentals (key, general) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET general = EXCLUDED.general, fetched_at = now()",
                (_load()[ticker], Json(g)),
            )
    else:
        FUND_DIR.mkdir(parents=True, exist_ok=True)
        (FUND_DIR / f"{ticker}.json").write_text(json.dumps(g))
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


def add_ticker(t: str) -> None:
    """Add one ticker to the security master and reload the cache.

    OpenFIGI for the FIGI, fundamentals for name/venue/type/ISIN/GICS, then one row appended to the master.
    If the FIGI already belongs to another ticker the security was renamed: the old row is marked delisted and
    both map to the same price file.

    Called by the daily refresh for tickers in the bulk file the master doesn't know, and by compaction.

    Args:
        t: ticker code; case-insensitive.

    Raises:
        ValueError: already in the master.
        httpx.HTTPStatusError: OpenFIGI or EODHD error.
    """
    t = t.upper()
    if t in master_tickers():
        raise ValueError(f"{t} is already in the security master")
    figi_df = openfigi([t])
    fdmtls_df = fundamentals([t])
    gen_dict = fundamentals_general(t)

    g_df = pd.DataFrame([gen_dict]) if gen_dict else pd.DataFrame([{"Code": t}])
    for col in ("Name", "Exchange", "Type", "ISIN"):
        if col not in g_df.columns:
            g_df[col] = None
    g_df = g_df[["Code", "Name", "Exchange", "Type", "ISIN"]]
    g_df = g_df.rename(columns={"Code": "ticker", "Name": "name", "Exchange": "venue", "Type": "type", "ISIN": "isin"})
    g_df["ticker"] = t  # fundamentals echo the code with its own casing/suffix; key on what we were asked for

    row = figi_df.merge(fdmtls_df, on="ticker").merge(g_df, on="ticker")
    row["in_universe"] = row["type"].isin(UNIVERSE_TYPES) & row["venue"].isin(MAJOR_VENUES)
    row["seeded_at"] = pd.Timestamp.now().floor("s")
    row = gics_codes(row)
    row = row.astype(object).where(pd.notna(row), None)

    r = row.iloc[0]
    figi = r["figi"] if r["figi_status"] == "ok" else None
    key = figi or f"UNK_{t}"

    with connect() as conn, conn.cursor() as cur:
        if figi:
            cur.execute(
                "SELECT ticker FROM security_alias WHERE key = %s AND valid_to IS NULL AND ticker <> %s", (key, t)
            )
            old = [x[0] for x in cur.fetchall()]
            if old:
                log.warning("rename: %s shares FIGI %s with %s -> closing old alias(es), marking delisted", t, key, old)
                cur.execute(
                    "UPDATE security_alias SET valid_to = CURRENT_DATE WHERE key = %s AND valid_to IS NULL", (key,)
                )
                cur.execute("UPDATE security SET is_delisted = TRUE, updated_at = now() WHERE key = %s", (key,))
        cur.execute(
            """INSERT INTO security (key, figi, share_class_figi, ticker, name, venue, type, openfigi_type, isin,
                   in_universe, gic_sector, gic_group, gic_industry, gic_sub_industry, gics_code, ipo_date,
                   is_delisted, fund_category)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (key) DO UPDATE SET ticker = EXCLUDED.ticker, is_delisted = FALSE, updated_at = now()""",
            (
                key,
                figi,
                r["share_class_figi"],
                t,
                r["name"],
                r["venue"],
                r["type"],
                r["openfigi_type"],
                r["isin"],
                bool(r["in_universe"]),
                r["gic_sector"],
                r["gic_group"],
                r["gic_industry"],
                r["gic_sub_industry"],
                r["gics_code"],
                pd.to_datetime(r["ipo_date"], errors="coerce").date() if r["ipo_date"] else None,
                bool(r["is_delisted"]) if r["is_delisted"] is not None else False,
                r["fund_category"],
            ),
        )
        cur.execute(
            "INSERT INTO security_alias (ticker, key, valid_from) VALUES (%s, %s, CURRENT_DATE) ON CONFLICT DO NOTHING",
            (t, key),
        )
        if gen_dict:
            cur.execute(
                "INSERT INTO fundamentals (key, general) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET general = EXCLUDED.general, fetched_at = now()",
                (key, Json(gen_dict)),
            )
    reload()

    log.info(
        "add_ticker %s -> %s (%s, %s, gics %s)",
        t,
        row["figi"].iloc[0],
        row["type"].iloc[0],
        row["venue"].iloc[0],
        row["gics_code"].iloc[0],
    )


def master_tickers() -> set[str]:
    """Every ticker the master knows, resolved or surrogate, as a set."""
    return set(_load())
