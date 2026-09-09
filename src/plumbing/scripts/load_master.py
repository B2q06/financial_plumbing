"""One-time load of the security master, aliases and cached fundamentals into Postgres.

Reads DATA/security_master.parquet and DATA/fundamentals/*.json, inserts what isn't there yet
(ON CONFLICT DO NOTHING), so re-running is harmless. Run: uv run python -m plumbing.scripts.load_master
"""

import json
import logging

import pandas as pd
import psycopg
from psycopg.types.json import Json

from plumbing.config import PG_DSN
from plumbing.data.master import FUND_DIR, MASTER

log = logging.getLogger(__name__)

SEED_DATE = "2026-09-08"  # the day the master was built; aliases are valid from here

SECURITY_COLS = [
    "key",
    "figi",
    "share_class_figi",
    "ticker",
    "name",
    "venue",
    "type",
    "openfigi_type",
    "isin",
    "in_universe",
    "gic_sector",
    "gic_group",
    "gic_industry",
    "gic_sub_industry",
    "gics_code",
    "ipo_date",
    "is_delisted",
    "fund_category",
    "seeded_at",
]
INSERT_SECURITY = f"""
    INSERT INTO security ({", ".join(SECURITY_COLS)})
    VALUES ({", ".join(["%s"] * len(SECURITY_COLS))})
    ON CONFLICT (key) DO NOTHING
"""
INSERT_ALIAS = "INSERT INTO security_alias (ticker, key, valid_from) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
INSERT_FUND = "INSERT INTO fundamentals (key, general) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING"


def master_rows() -> pd.DataFrame:
    """The parquet master shaped like the `security` table: one `key` column, NaN -> None, clean bools/dates."""
    m = pd.read_parquet(MASTER)
    m["key"] = m["figi"].where(m["figi_status"] == "ok", "UNK_" + m["ticker"])
    m["figi"] = m["figi"].where(m["figi_status"] == "ok", None)
    m["in_universe"] = m["in_universe"].fillna(False).astype(bool)
    m["is_delisted"] = m["is_delisted"].fillna(False).astype(bool)
    m["ipo_date"] = pd.to_datetime(m["ipo_date"], errors="coerce").dt.date
    m = m[SECURITY_COLS]
    m = m.astype(object).where(pd.notna(m), None)  # NaN/NaT -> None so psycopg sends NULL
    return m


def main() -> None:
    m = master_rows()
    keys = dict(zip(m["ticker"], m["key"]))

    with psycopg.connect(PG_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(INSERT_SECURITY, list(m.itertuples(index=False, name=None)))
        cur.executemany(INSERT_ALIAS, [(t, k, SEED_DATE) for t, k in keys.items()])

        fund_rows = []
        for f in FUND_DIR.glob("*.json"):
            g = json.loads(f.read_text())
            if g and f.stem in keys:
                fund_rows.append((keys[f.stem], Json(g)))
        cur.executemany(INSERT_FUND, fund_rows)

        for table in ("security", "security_alias", "fundamentals"):
            cur.execute(f"SELECT count(*) FROM {table}")
            log.info("%s: %d rows", table, cur.fetchone()[0])


if __name__ == "__main__":
    from plumbing.log import setup

    setup("load_master")
    main()
