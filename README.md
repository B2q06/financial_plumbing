# financial_plumbing

A financial data hub

Right now it is a self-maintaining, self-healing store of daily
price data for every instrument that trades on a US venue (about 51,000 tickers: common stock, ETFs, funds,
preferreds, OTC), fed by the [EODHD](https://eodhd.com) API, with a security master keyed by FIGI. The plan is
for it to grow into a factor manager: define a factor over a universe, backfill it across all of history, write
trading logic against it, and backtest that logic. The data layer is done and solid. 

I built this as a learning project.


## What it does atm

**Daily refresh** (cron, 17:30 ct, weekdays). One bulk call to EODHD pulls the day's bar for every US ticker
into an "inbox" file for that date. Any ticker the security master hasn't seen is resolved through OpenFIGI and
EODHD fundamentals  then added to the master on the spot, so new listings and renames are handled the day they
appear. Then the job asks EODHD which tickers had a split or dividend that day and re-pulls each one's full
history, because an adjustment rewrites every prior adjusted close. The frontier day is always re-pulled the
next evening to catch late-arriving rows.

**Weekly compaction** (cron, Sunday 03:00 CT). Folds the inbox day files into the per-security parquet files
and deletes them. The per-security file always wins on a date conflict, because a re-pulled file is fresher than
an inbox snapshot. The newest day file is left alone so the next refresh can still re-pull it. Inbox is used
as a temporary data hub for newly arrived data in a by date format (inbox holds by date parquets of all ticker
data). This is so that we can later preform transormation to move the data to the per ticker files that is 
primarily used by the database at a later point in time (I set it to every sunday but this is easily configurable
via crontab in the dockerfile. 

**Security master.** One row per ticker: composite FIGI (the file key), share-class FIGI, venue, type, ISIN,
the four GICS levels as names plus the 8-digit code, IPO date, delisted flag, and an `in_universe` flag for
common stock on a major exchange (~6,300 names). Tickers OpenFIGI can't resolve get a surrogate key so they
are still tracked. Built once by `scripts/seed_master.py` from the EODHD symbol list, OpenFIGI, and EODHD
fundamentals; maintained by the daily job.

**Reads.** `read_series("AAPL", start="2026-01-01")` returns adjusted closes as a pandas Series, resolving the
ticker through the master and unioning the per-security file with whatever is still in the inbox. DuckDB does
the reading; parquet is the storage.

**Repair.** `repull("AAPL")` rewrites one security's file from EODHD's full history. `repull_all("all")`
does the whole master with a shared rate gate (~1,000 requests/min). The quarterly cron run is a backstop
against adjustment drift; it should never find anything to fix.

**Plumbing.** One function is the door to EODHD: token, JSON, rate gate, 429 backoff. Every job logs to its
own file plus a shared `all.log`, with API tokens redacted. Writes are temp-file-then-rename, so a reader never
sees a half-written file. A lock file keeps the two jobs from interleaving.

## Layout

    src/plumbing/config.py            paths (DATA_DIR), tokens from .env
    src/plumbing/log.py               setup(job): per-job log + all.log + stdout, token redaction
    src/plumbing/data/prices.py       eodhd_get, fetch_bulk_prices, write_day, read_series, last_stored_date,
                                 repull, corporate_actions
    src/plumbing/data/master.py       security master: figi_for, path_for, add_ticker, master_tickers,
                                 and the builders (symbol_list, openfigi, fundamentals, gics_codes)
    src/plumbing/jobs/refresh.py      run() = the daily job; repull_all()
    src/plumbing/jobs/compact.py      run() = the weekly fold
    src/plumbing/scripts/seed_master.py   one-time master build

Data lives outside the repo, whatever `DATA_DIR` is set to in the config:

    parquet/prices_by_ticker/<FIGI>.parquet   date, open, high, low, close, adjusted_close, volume
    parquet/prices_by_date/<YYYY-MM-DD>.parquet   the inbox, ticker-sorted, folded weekly
    parquet/legacy/                            pre-migration files for delisted names
    security_master.parquet                    the master (moving to Postgres)
    fundamentals/<ticker>.json                 cached EODHD General blocks
    logs/

## Running it

    uv sync
    cp .env.example .env        # EODHD_TOKEN, OPENFIGI_TOKEN, DATA_DIR
    uv run python src/plumbing/scripts/seed_master.py                 # once, ~15 min, ~65k EODHD units
    uv run python -c "from plumbing.jobs.refresh import repull_all; repull_all('all')"   # once, ~1 h, ~51k units
    uv run python -m plumbing.jobs.refresh                            # daily
    uv run python -m plumbing.jobs.compact                            # weekly

Or start from an existing data directory (rsync it) and skip the two one-time steps.

## What I plan on building next

1. **Postgres** for the master, aliases, fundamentals and job runs. The parquet price files stay parquet.
2. **Docker + cron on a home server**, with push-to-deploy from the `production` branch.
3. **A read API** over the tailnet so any machine can call `read_series` against the server's data.
4. **Universes**: a universe is a SQL filter on the master (GICS code, sector, venue, type, market cap, etc) plus an as-of date.
5. **Factors**: a factor is a function of a security's history that produces one number per day. Factor values
   are written back as extra columns in the per-security files, next to OHLCV, so they backfill across all of
   history and read at the same speed as prices.
6. **Backtests**: trading logic that reads factor columns and decides to buy or sell, with a mock portfolio
   underneath. Not designed yet.

## Data notes

- `adjusted_close` is the only price the math should read; `close` is the raw print, kept for charting.
- EODHD's bulk file for a day fills in progressively for a few hours after the close and can gain rows the
  next day; hence the frontier re-pull.
- FIGIs are per security, not per company: a holding-company reorganization gives a new FIGI (XOM, 2025).
  EODHD serves the full history under the current ticker, so reads are unaffected.
- GICS arrives from EODHD as names; a few pre-2023 names don't match the current hierarchy and are coded at
  the industry level until aliased.
