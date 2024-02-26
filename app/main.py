from urllib.parse import unquote

import uvicorn
from fastapi import FastAPI, Query
from typing import Annotated, Union

from services.warp_service import WarpService


def start_application():
    app = FastAPI()
    return app


app = start_application()


@app.get("/pairs/")
def get_pairs():
    """
    This method retrieves all trading pairs available on the specified DEX.
    Each trading pair consists of a base asset and a target asset. Additionally, it provides the
    pool ID associated with each trading pair, as well as a unique identifier for
    the ticker, which includes both the base and quote assets with a delimiter
    separating them.

    returns:\n
    - Base asset (base): The denomination of the base asset. \n
    - Target asset (quote): The denomination of the quote asset.\n
    - Pool ID (pool_id): The identifier of the liquidity pool associated with this trading pair.\n
    - Ticker ID (ticker_id): The unique identifier of the ticker, including both base and quote assets
    with a delimiter to separate them (e.g., boot_hydrogen for the price of BOOT quoted in HYDROGEN).
    """
    return WarpService().get_pairs()


@app.get("/dev/tickers/")
def get_pairs():
    """
    ONLY FOR API DEVS PORPOSES AND USAGE!
    FOR INTEGRATION USE /tickers/ endpoint instead


    This method retrieves all available tickers on the specified DEX.
    A ticker represents the current trading information for a specific trading
    pair. Each ticker includes details such as the base currency, target currency,
    pool ID associated with the pair, ticker ID, last price, liquidity in USD,
    base volume (traded volume of base tokens in the last 24 hours), and target
    volume (traded volume of target tokens in the last 24 hours).


    returns\n
    - Base Currency (base_currency): The denomination of the base asset.\n
    - Target Currency (target_currency): The denomination of the target asset.\n
    - Pool ID (pool_id): The identifier of the liquidity pool associated with this trading pair.\n
    - Ticker ID (ticker_id): The unique identifier of the ticker, including both base and target currencies with a
    delimiter to separate them (e.g., boot_hydrogen for the price of BOOT quoted in HYDROGEN).\n
    - Last Price (last_price): The last traded price for this pair.\n
    - Liquidity in USD (liquidity_in_usd): The liquidity of this pair in USD.\n
    - Base Volume (base_volume): The traded volume of base tokens in the last 24 hours.\n
    - Target Volume (target_volume): The traded volume of target tokens in the last 24 hours.
    """
    return WarpService().get_tickers(True)


@app.get("/tickers/")
def get_pairs():
    """
    This method retrieves all available tickers on the specified DEX.
    A ticker represents the current trading information for a specific trading
    pair. Each ticker includes details such as the base currency, target currency,
    pool ID associated with the pair, ticker ID, last price, liquidity in USD,
    base volume (traded volume of base tokens in the last 24 hours), and target
    volume (traded volume of target tokens in the last 24 hours).


    returns\n
    - Base Currency (base_currency): The denomination of the base asset.\n
    - Target Currency (target_currency): The denomination of the target asset.\n
    - Pool ID (pool_id): The identifier of the liquidity pool associated with this trading pair.\n
    - Ticker ID (ticker_id): The unique identifier of the ticker, including both base and target currencies with a
    delimiter to separate them (e.g., boot_hydrogen for the price of BOOT quoted in HYDROGEN).\n
    - Last Price (last_price): The last traded price for this pair.\n
    - Liquidity in USD (liquidity_in_usd): The liquidity of this pair in USD.\n
    - Base Volume (base_volume): The traded volume of base tokens in the last 24 hours.\n
    - Target Volume (target_volume): The traded volume of target tokens in the last 24 hours.
    """
    return WarpService().get_tickers(False)


@app.get("/historical_trades/{ticker_id:path}/", name="path-convertor")
def get_historical_trades(ticker_id, limit: int = 10, offset: int = 0, type: str = '', start_time: int = 0, end_time: int = 0):
    """
    This method retrieves historical trades for the requested trading pair. Each trade
    record includes a unique trade ID, timestamp in Unix format, ticker ID, type of
    trade (buy or sell), base volume, target volume, and trade price.


    params\n
    - ticker_id: str. (e.g., boot_hydrogen for the price of BOOT quoted in HYDROGEN) \n
    - limit: int. default 10. \n
    - offset: int. default 0 \n
    - type: str. buy or sell \n
    - start_time: int. Unix timestamp \n
    - end_time: int. Unix timestamp \n


    returns\n

    - Trade ID (id): A unique identifier for the trade.\n
    - Trade timestamp (trade_timestamp): The timestamp of the trade in Unix format.\n
    - Ticker ID (ticker_id): The unique identifier of the ticker associated with the trade (e.g., boot_hydrogen for the
    price of BOOT quoted in HYDROGEN).
    - Type (type): The type of trade, indicating whether it's a buy or sell.\n
    - Base Volume (base_volume): The volume of base tokens involved in the trade.\n
    - Target Volume (target_volume): The volume of target tokens involved in the trade.\n
    - Trade Price (trade_price): The price at which the trade occurred.
    """
    return WarpService().get_historical_trades(ticker_id, limit, offset, type, start_time, end_time)


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
    return WarpService().get_spot_recent(ticker_root, limit, offset, type, start_time, end_time)

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
