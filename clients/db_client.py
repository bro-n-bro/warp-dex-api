from datetime import timedelta, datetime
from typing import Optional, List

import clickhouse_connect

from common.db_connector import DBConnector
from collections import namedtuple

from common.decorators import get_first_if_exists


class DBClient:

    def __init__(self):
        self.connection = DBConnector().clickhouse_client

    def fix_column_names(self, column_names: List[str]) -> List[str]:
        res = []
        for column_name in column_names:
            new_column_name = column_name.replace('(', '_').replace(')', '_').replace('.', '_')
            res.append(new_column_name)
        return res

    def make_query(self, query: str) -> List[namedtuple]:
        query = self.connection.query(query)
        Record = namedtuple("Record", self.fix_column_names(query.column_names))
        result = [Record(*item) for item in query.result_rows]
        return result

    def get_pairs_liquidity_pool(self):
        return self.make_query(f"""
            SELECT a_denom AS base, b_denom AS target, pool_id, CONCAT(a_denom, '_',  b_denom) AS ticker_id  FROM spacebox.liquidity_pool FINAL
        """)

    def get_base_for_tickers(self):
        return self.make_query(f"""
            SELECT 
                a_denom AS base_currency, 
                b_denom AS target_currency, 
                pool_id, 
                CONCAT(a_denom, '_' ,b_denom) AS ticker_id, 
                liquidity_a, 
                liquidity_b, 
                s.swap_price as last_price 
            FROM spacebox.liquidity_pool as lp FINAL
            LEFT JOIN (
                select * from (
                    select 
                        *, 
                        ROW_NUMBER() OVER (PARTITION BY pool_id ORDER BY height desc) AS ROWNUM 
                    from spacebox.swap FINAL WHERE success = 1
                ) 
                where ROWNUM = 1
            ) as s on s.pool_id = lp.pool_id
        """)

    def get_denom_traces(self):
        return self.make_query(f"""
            select * from spacebox.denom_trace FINAL
        """)

    @get_first_if_exists
    def get_height_after_timestamp(self, timestamp):
        return self.make_query(f"""
            select height from spacebox.block FINAL where `timestamp` > '{timestamp}' order by height ASC LIMIT 1
        """)

    def get_offer_coins_volume(self, height):
        return self.make_query(f"""
            select 
                pool_id, 
                offer_coin_denom, 
                sum(offer_coin_amount) as sum 
            from spacebox.swap FINAL 
            where height > {height} 
            and success = 1
            GROUP by pool_id, offer_coin_denom 
        """)

    def get_demand_coins_volume(self, height):
        return self.make_query(f"""
            select 
                pool_id, 
                demand_coin_denom, 
                sum(exchanged_demand_coin_amount) as sum 
            from spacebox.swap FINAL 
            where height > {height} 
            and success = 1
            GROUP by pool_id, demand_coin_denom  
        """)

    def build_filter_for_historical_trades(self, type, start_time, end_time):
        filter_string = ''
        if type:
            filter_string = f"WHERE type = '{type}'"
        if start_time:
            filter_string = f"{filter_string} AND trade_timestamp > {start_time}" if filter_string else f"WHERE trade_timestamp > {start_time}"
        if end_time:
            filter_string = f"{filter_string} AND trade_timestamp < {end_time}" if filter_string else f"WHERE trade_timestamp < {end_time}"
        return filter_string

    def get_historical_trades(self, ticker_id, limit, offset, type, start_time, end_time):
        return self.make_query(f"""
            select 
                msg_index as id,
                toUnixTimestamp(b.timestamp) as trade_timestamp, 
                ticker_id,
                if(offer_coin_denom = a_denom, 'sell', 'buy') as type,
                if(offer_coin_denom = a_denom, offer_coin_amount, exchanged_demand_coin_amount) as base_volume,
                if(offer_coin_denom = b_denom, offer_coin_amount, exchanged_demand_coin_amount) as target_volume,
                swap_price as price
            from (
                SELECT s.*, pool_id, CONCAT(a_denom, '_',  b_denom) as ticker_id, a_denom , b_denom  
                FROM spacebox.liquidity_pool as lp FINAL
                LEFT JOIN (select * from spacebox.swap FINAL) as s on s.pool_id = lp.pool_id
                where ticker_id = '{ticker_id}' and s.success = True order by height DESC LIMIT {limit} OFFSET {offset}
            ) as d
            left join (select * from spacebox.block FINAL) as b on d.height = b.height
            {self.build_filter_for_historical_trades(type, start_time, end_time)}
        """)

