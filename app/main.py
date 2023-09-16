
import uvicorn
from fastapi import FastAPI

from services.warp_service import WarpService


def start_application():
    app = FastAPI()
    return app


app = start_application()


@app.get("/pairs/")
def get_pairs():
    return WarpService().get_pairs()


@app.get("/tickers/")
def get_pairs():
    return WarpService().get_tickers()

@app.get("/historical_trades/{ticker_id}/")
def get_historical_trades(ticker_id, limit: int = 10, offset: int = 0):
    return WarpService().get_historical_trades(ticker_id, limit, offset)


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
