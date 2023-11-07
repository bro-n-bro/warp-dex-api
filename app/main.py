from urllib.parse import unquote

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


@app.get("/dev/tickers/")
def get_pairs():
    return WarpService().get_tickers(True)


@app.get("/tickers/")
def get_pairs():
    return WarpService().get_tickers(False)


@app.get("/historical_trades/{ticker_id:path}/", name="path-convertor")
def get_historical_trades(ticker_id, limit: int = 10, offset: int = 0, type: str = '', start_time: int = 0, end_time: int = 0):
    return WarpService().get_historical_trades(ticker_id, limit, offset, type, start_time, end_time)


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
