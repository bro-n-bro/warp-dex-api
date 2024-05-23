import json
from datetime import datetime, timedelta

from clients.bronbro_api_client import BronbroApiClient
from clients.db_client import DBClient


class WarpService:


    def __init__(self):
        self.db_client = DBClient()
        self.bronbro_api_client = BronbroApiClient()
        self.allowed_pool_ids = [1, 12, 7, 5, 6, 10, 26, 18, 11, 2, 15, 24, 13]

    def get_pairs(self, show_all):
        if show_all:
            result = self.db_client.get_pairs_liquidity_pool(None)
        else:
            result = self.db_client.get_pairs_liquidity_pool(self.allowed_pool_ids)
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
        elif denom_trace == 'gravity0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2':
            exponent = -18
        elif denom_trace == 'aevmos':
            exponent = -18
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

    def get_boot_price(self, tickers):
        prices = self.bronbro_api_client.get_exchange_rates()
        atom_exchange_rate = next((rate.get('price') for rate in prices if rate.get('symbol') == 'ATOM'), None)
        hydrogen_to_atom_price = next(ticker.last_price for ticker in tickers if ticker.ticker_id == 'hydrogen_ibc/15E9C5CF5969080539DB395FA7D9C0868265217EFC528433671AAF9B1912D159')
        hydrogen_price = atom_exchange_rate / hydrogen_to_atom_price / 10**6
        boot_to_hydrogen_price = next(ticker.last_price for ticker in tickers if ticker.ticker_id == 'boot_hydrogen')
        boot_exchange_rate = hydrogen_price / boot_to_hydrogen_price
        return boot_exchange_rate

    def set_total_liquidity(self, boot_price, ticker):
        ticker['liquidity_in_usd'] = boot_price*(ticker['liquidity_a']['amount'] + ticker['liquidity_b']['amount'])

    def update_price_based_on_exponent(self, ticker):
        ticker['last_price'] = ticker['last_price'] / 10**ticker['liquidity_a']['exponent'] * 10**ticker['liquidity_b']['exponent']

    def get_tickers(self, show_all):
        tickers = self.db_client.get_base_for_tickers()
        boot_price = self.get_boot_price(tickers)
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
        if not show_all:
            ticker_dicts = list(filter(lambda x: (x['pool_id'] in self.allowed_pool_ids), ticker_dicts))
        return ticker_dicts

    def get_exponent(self, denom, denom_traces):
        exponent = 0
        if 'ibc/' in denom:
            denom_trace = next((item.base_denom for item in denom_traces if item.denom_hash in denom), None)
        else:
            denom_trace = denom
        if denom_trace.startswith('u'):
            exponent = -6
        elif denom_trace.startswith('a'):
            exponent = -18
        elif denom_trace.startswith('milli'):
            exponent = -3
        elif denom_trace == 'gravity0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2':
            exponent = -18
        elif denom_trace == 'aevmos':
            exponent = -18
        return exponent

    def convert_volume_to_usd(self, denom, exponent, amount, tickers, hydrogen_to_boot, boot_price):
        if denom == 'boot':
            pass
        elif denom != 'hydrogen':
            last_price = next((ticker.last_price for ticker in tickers if
                               ticker.target_currency == denom and ticker.base_currency == 'hydrogen'),
                              None)
            denom = 'hydrogen'
            amount = amount * 10 ** exponent * last_price

        if denom == 'hydrogen':
            amount = amount * hydrogen_to_boot
        return amount*boot_price

    def get_24_volume_usd(self):
        tickers = self.db_client.get_base_for_tickers()
        boot_price = self.get_boot_price(tickers)
        hydrogen_to_boot = next((ticker.last_price for ticker in tickers if
                                 ticker.base_currency == 'boot' and ticker.target_currency == 'hydrogen'), None)
        denom_traces = self.db_client.get_denom_traces()
        # TODO: FIX AFTER DB UPDATED
        datetime_24_hour_ago = datetime.now() - timedelta(hours=24)
        datetime_24_hour_ago = datetime.strftime(datetime_24_hour_ago, '%Y-%m-%d %H:%M:%S')
        height = self.db_client.get_height_after_timestamp(datetime_24_hour_ago)
        height_from_search_volume = height.height
        pairs = self.db_client.get_last_24_hours_volume_pairs(height_from_search_volume, self.allowed_pool_ids)
        pair_dicts = [ticker._asdict() for ticker in pairs]
        result = 0
        for ticker in pair_dicts:
            ticker['base_exponent'] = self.get_exponent(ticker['base_currency'], denom_traces)
            ticker['target_exponent'] = self.get_exponent(ticker['target_currency'], denom_traces)
            ticker['base_volume_usd'] = self.convert_volume_to_usd(ticker['base_currency'], ticker['base_exponent'], ticker['base_volume'], tickers, hydrogen_to_boot, boot_price)
            ticker['target_volume_usd'] = self.convert_volume_to_usd(ticker['target_currency'], ticker['target_exponent'], ticker['target_volume'], tickers, hydrogen_to_boot, boot_price)
            result += ticker['base_volume_usd']
            result += ticker['target_volume_usd']
        return {'value': result}


    def get_historical_trades(self, ticker_id, limit, offset, type, start_time, end_time):
        return [item._asdict() for item in self.db_client.get_historical_trades(ticker_id, limit, offset, type, start_time, end_time)]

    def get_spot_recent(self, ticker_id, limit, offset, type, start_time, end_time):
        return [item._asdict() for item in self.db_client.get_spot_recent(ticker_id, limit, offset, type, start_time, end_time)]

    def get_spot_summary(self, show_all):
        datetime_24_hour_ago = datetime.now() - timedelta(hours=24)
        datetime_24_hour_ago = datetime.strftime(datetime_24_hour_ago, '%Y-%m-%d %H:%M:%S')
        height = self.db_client.get_height_after_timestamp(datetime_24_hour_ago)
        if show_all:
            result = self.db_client.get_spot_summary(height.height, None)
        else:
            result = self.db_client.get_spot_summary(height.height, self.allowed_pool_ids)
        result = [item._asdict() for item in result]
        for item in result:
            item['price_change_percent_24h'] = abs((item['last_price']/item['first_price'] - 1) * 100) if item['first_price'] and item['last_price'] else 0
            item.pop('pool_id')
            item.pop('first_price')
        return result

    def get_spot_ticker(self, show_all):
        datetime_24_hour_ago = datetime.now() - timedelta(hours=24)
        datetime_24_hour_ago = datetime.strftime(datetime_24_hour_ago, '%Y-%m-%d %H:%M:%S')
        height = self.db_client.get_height_after_timestamp(datetime_24_hour_ago)
        if show_all:
            result_list = self.db_client.get_spot_ticker(height.height, None)
        else:
            result_list = self.db_client.get_spot_ticker(height.height, self.allowed_pool_ids)
        result = {}
        for item in result_list:
            item_dict = item._asdict()
            item_dict.pop('pool_id')
            item_dict.pop('trading_pairs')
            result[item.trading_pairs] = item_dict
        return result

    def get_wallet_assets(self):
        db_response = self.db_client.get_denom_traces()
        result = {}
        for item in db_response:
            result[item.base_denom] = {
                'name': item.base_denom,
                'contractAddress': item.denom_hash
            }
        return result
