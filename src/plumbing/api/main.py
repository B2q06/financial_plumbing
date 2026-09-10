from fastapi import FastAPI

from plumbing.data.prices import read_series

app = FastAPI()

@app.get("/series/{ticker}")
def series(ticker: str, start: str | None = None, end: str | None = None, all_history: bool = False):
    s = read_series(ticker, start, end, all_history)
    return {"ticker": ticker.upper(), "dates": [d.isoformat() for d in s.index], "close": s.tolist()}

