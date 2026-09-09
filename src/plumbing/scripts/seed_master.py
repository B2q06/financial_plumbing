"""Build the security master from scratch: every US ticker EODHD serves, with FIGI, type, venue, and GICS.

Stages (see plumbing.data.master for the functions; each cached so a re-run only does what's missing):
  1. /exchange-symbol-list/US            -> ticker, name, venue, type, isin           (1 unit, ~51k rows)
  2. OpenFIGI mapping, 100 per request   -> figi, share_class_figi, openfigi type     (free, ~3 min)
  3. /fundamentals/{code}?filter=General -> GICS names, ISIN, IPO date, delisted flag (10 units each, universe only)
  4. GICS names -> 8-digit codes via the old repo's gics_hierarchy.csv

Output: DATA/security_master.parquet, one row per ticker. Daily additions go through master.add_ticker.
"""

import logging

import pandas as pd

from plumbing.config import DATA
from plumbing.data.master import MASTER, MAJOR_VENUES, UNIVERSE_TYPES, fundamentals, gics_codes, openfigi, symbol_list

log = logging.getLogger(__name__)


def main() -> None:
    """Build ``DATA/security_master.parquet`` from scratch. Run once; ``master.add_ticker`` maintains it after.

    Stage 1+2 (symbol list + OpenFIGI) are cached in ``security_master.stage2.parquet``; fundamentals responses
    are cached per ticker. Re-running only fetches what's missing.
    """
    stage2 = DATA / "security_master.stage2.parquet"
    if stage2.exists():
        m = pd.read_parquet(stage2)
        log.info("stage 1+2 loaded from cache: %d rows (delete %s to refresh)", len(m), stage2.name)
    else:
        syms = symbol_list()
        figis = openfigi(syms.ticker.tolist())
        m = syms.merge(figis, on="ticker", how="left")
        m.to_parquet(stage2, index=False)
    m["in_universe"] = m.type.isin(UNIVERSE_TYPES) & m.venue.isin(MAJOR_VENUES)
    uni = m.loc[m.in_universe, "ticker"].tolist()
    log.info("universe: %d tickers (%s on %s)", len(uni), sorted(UNIVERSE_TYPES), sorted(MAJOR_VENUES))
    f = fundamentals(uni)
    m = m.merge(f, on="ticker", how="left")
    m = gics_codes(m)
    m["seeded_at"] = pd.Timestamp.now().floor("s")
    m.to_parquet(MASTER, index=False)
    log.info(
        "master written: %d rows, %d figi ok, %d in universe, %d with gics -> %s",
        len(m),
        (m.figi_status == "ok").sum(),
        m.in_universe.sum(),
        m.gics_code.notna().sum(),
        MASTER,
    )


if __name__ == "__main__":
    from plumbing.log import setup

    setup("seed_master")
    main()
