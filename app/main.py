from urllib.parse import unquote

import uvicorn
from fastapi import FastAPI, Query

from services.warp_service import WarpService


def start_application():
    app = FastAPI()
    return app


app = start_application()


@app.get("/v1/spot/summary")
def get_spot_summary():
    """
        Overview of market data for all tickers and all markets.

        returns\n

        - trading_pairs (string): Identifier of a ticker with delimiter to separate base/quote, e.g. BTC-USD (Price of BTC is quoted in USD).\n
        - last_price (decimal): Last transacted price of base currency based on given quote currency.\n
        - lowest_ask (decimal): Lowest Ask price of base currency based on given quote currency.\n
        - highest_bid (decimal): Highest bid price of base currency based on given quote currency.\n
        - base_volume (decimal): 24-hr volume of market pair denoted in BASE currency.\n
        - quote_volume (decimal): 24-hr volume of market pair denoted in QUOTE currency.\n
        - price_change_percent_24h (decimal): 24-hr % price change of market pair.\n
        - highest_price_24h (decimal): Highest price of base currency based on given quote currency in the last 24-hrs.\n
        - lowest_price_24h (decimal): Lowest price of base currency based on given quote currency in the last 24-hrs.\n
    """
    return WarpService().get_spot_summary()


@app.get("/v1/wallet/assets")
def get_spot_ticker():
    """
    In depth details on crypto currencies available on the exchange

    returns\n

    - name (string): Full name of cryptocurrency.\n
    - contractAddress (string): Contract address of the asset on each chain.\n
    """
    return WarpService().get_wallet_assets()


@app.get("/v1/spot/ticker")
def get_spot_ticker():
    """
    24-hour rolling window price change statistics for all markets.

    returns\n

    - last_price (decimal): Last transacted price of base currency based on given quote currency.\n
    - base_volume (decimal): 24-hour trading volume denoted in BASE currency.\n
    - quote_volume (decimal): 24-hour trading volume denoted in QUOTE currency.\n
    """
    return WarpService().get_spot_ticker()


@app.get("/v1/spot/recent")
def get_spot_recent(ticker_root: str ='', limit: int = 10, offset: int = 0, type: str = '', start_time: int = 0, end_time: int = 0):
    """
    Recently completed trades for a given market. 24 hour historical full trades available as minimum requirement.
    :param ticker_root:
    :param limit:
    :param offset:
    :param type:
    :param start_time:
    :param end_time:


    returns\n

    - trade_id (integer): A unique ID associated with the trade for the currency pair transaction.\n
    - price (decimal): Last transacted price of base currency based on given quote currency.\n
    - base_volume (decimal): Transaction amount in BASE currency.\n
    - quote_volume (decimal): Transaction amount in QUOTE currency.\n
    - timestamp (Integer): Unix timestamp in milliseconds for when the transaction occurred.\n
    - type (string): Used to determine whether the transaction originated as a buy or sell.\n
    """
    return WarpService().get_historical_trades(ticker_root, limit, offset, type, start_time, end_time)


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
