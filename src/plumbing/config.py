import os
from pathlib import Path

from dotenv import load_dotenv

# Must be set before numpy is imported anywhere. 8 on the laptop (P-cores), 6 on giant.
os.environ.setdefault("OPENBLAS_NUM_THREADS", "8")

PROJECT = Path(__file__).resolve().parents[2]  # .../financial_plumbing  (src/plumbing/config.py -> up 3)
load_dotenv(PROJECT / ".env")  # must run before anything below reads os.environ
DATA = Path(os.environ.get("DATA_DIR", PROJECT.parent / "data"))  # default: data/ beside the project
PARQUET_BY_TICKER = DATA / "parquet" / "prices_by_ticker"
PARQUET_BY_DATE = DATA / "parquet" / "prices_by_date"

EODHD_TOKEN = os.environ["EODHD_TOKEN"]
OPENFIGI_TOKEN = os.environ["OPENFIGI_TOKEN"]
