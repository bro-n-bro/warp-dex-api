import json
from datetime import datetime, timedelta

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient


class WarpService:

    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()

    def get_pairs(self):
        result = self.db_client.get_pairs_liquidity_pool()
        return [item._asdict() for item in result]

    def set_exponent_for_liquidity(self, liquidity, denom_traces):
        exponent = 0
        if 'ibc/' in liquidity['denom']:
            denom_trace = next((item.base_denom for item in denom_traces if item.denom_hash in liquidity['denom']), None)
        else:
            denom_trace = liquidity['denom']
        if denom_trace.startswith('u'):
            exponent = -6
        elif denom_trace.startswith('a'):
            exponent = -18
        elif denom_trace.startswith('milli'):
            exponent = -3
        liquidity['exponent'] = exponent

    def convert_liquidity_amount_to_boot(self, liquidity, tickers, hydrogen_to_boot):
        if liquidity['denom'] == 'boot':
            return
        elif liquidity['denom'] != 'hydrogen':
            last_price = next((ticker.last_price for ticker in tickers if
                              ticker.target_currency == liquidity['denom'] and ticker.base_currency == 'hydrogen'),
                             None)
            liquidity['denom'] = 'hydrogen'
            liquidity['amount'] = liquidity['amount'] * 10**liquidity['exponent'] / last_price
        liquidity['denom'] = 'boot'
        liquidity['amount'] = liquidity['amount'] * hydrogen_to_boot

    def get_boot_price(self):
        prices = self.bronbro_api_client.get_exchange_rates()
        boot_exchange_rate = next((rate.get('price') for rate in prices if rate.get('symbol') == 'BOOT'), None)
        return boot_exchange_rate

    def set_total_liquidity(self, boot_price, ticker):
        ticker['liquidity_in_usd'] = boot_price*(ticker['liquidity_a']['amount'] + ticker['liquidity_b']['amount'])

    def update_price_based_on_exponent(self, ticker):
        ticker['last_price'] = ticker['last_price'] / 10**ticker['liquidity_a']['exponent'] * 10**ticker['liquidity_b']['exponent']

    def get_tickers(self):
        boot_price = self.get_boot_price()
        tickers = self.db_client.get_base_for_tickers()
        hydrogen_to_boot = next((ticker.last_price for ticker in tickers if
                                 ticker.base_currency == 'boot' and ticker.target_currency == 'hydrogen'), None)
        ticker_dicts = [ticker._asdict() for ticker in tickers]
        denom_traces = self.db_client.get_denom_traces()
        # TODO: FIX AFTER DB UPDATED
        datetime_24_hour_ago = datetime.now() - timedelta(hours=24)
        datetime_24_hour_ago = datetime.strftime(datetime_24_hour_ago, '%Y-%m-%d %H:%M:%S')
        height = self.db_client.get_height_after_timestamp(datetime_24_hour_ago)
        height_from_search_volume = height.height if height else None
        if height_from_search_volume:
            offer_coins_volume = self.db_client.get_offer_coins_volume(height_from_search_volume)
            demand_coins_volume = self.db_client.get_demand_coins_volume(height_from_search_volume)
        else:
            offer_coins_volume = []
            demand_coins_volume = []
        for ticker in ticker_dicts:
            ticker['liquidity_a'] = json.loads(ticker['liquidity_a'])
            ticker['liquidity_b'] = json.loads(ticker['liquidity_b'])
            self.set_exponent_for_liquidity(ticker['liquidity_a'], denom_traces)
            self.set_exponent_for_liquidity(ticker['liquidity_b'], denom_traces)
            self.update_price_based_on_exponent(ticker)
            self.convert_liquidity_amount_to_boot(ticker['liquidity_a'], tickers, hydrogen_to_boot)
            self.convert_liquidity_amount_to_boot(ticker['liquidity_b'], tickers, hydrogen_to_boot)
            self.set_total_liquidity(boot_price, ticker)
            base_offer_coin_volume = next((item.sum for item in offer_coins_volume if ticker['pool_id'] == item.pool_id and ticker['base_currency'] == item.offer_coin_denom), 0)
            target_offer_coin_volume = next((item.sum for item in offer_coins_volume if ticker['pool_id'] == item.pool_id and ticker['target_currency'] == item.offer_coin_denom), 0)
            base_demand_coin_volume = next((item.sum for item in demand_coins_volume if ticker['pool_id'] == item.pool_id and ticker['base_currency'] == item.demand_coin_denom), 0)
            target_demand_coin_volume = next((item.sum for item in demand_coins_volume if ticker['pool_id'] == item.pool_id and ticker['target_currency'] == item.demand_coin_denom), 0)
            ticker['base_volume'] = base_demand_coin_volume + base_offer_coin_volume
            ticker['target_volume'] = target_demand_coin_volume + target_offer_coin_volume
            ticker.pop('liquidity_a')
            ticker.pop('liquidity_b')
        return ticker_dicts
