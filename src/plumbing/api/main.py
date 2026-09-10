from fastapi import FastAPI, HTTPException

from plumbing.data.master import TickerNotFound
from plumbing.data.prices import read_series

app = FastAPI(title="financial_plumbing")


@app.get("/series/{ticker}")
def series(ticker: str, start: str | None = None, end: str | None = None, all_history: bool = False):
    """Adjusted closes for one ticker. 404 when the ticker or the window has no bars, 400 for a bad date."""
    try:
        s = read_series(ticker, start, end, all_history)
    except (TickerNotFound, FileNotFoundError) as e:
        raise HTTPException(status_code=404, detail=str(e)) from None
    except ValueError as e:
        raise HTTPException(status_code=404 if "no bars" in str(e) else 400, detail=str(e)) from None
    return {"ticker": ticker.upper(), "dates": [d.date().isoformat() for d in s.index], "close": s.tolist()}
