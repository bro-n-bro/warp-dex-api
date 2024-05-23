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

    def get_pairs_liquidity_pool(self, allowed_pool_ids):
        filter = ''
        if allowed_pool_ids:
            filter = f'WHERE pool_id IN ({", ".join(map(str, allowed_pool_ids))})'
        return self.make_query(f"""
            SELECT a_denom AS base, b_denom AS target, pool_id, CONCAT(a_denom, '_',  b_denom) AS ticker_id  FROM spacebox.liquidity_pool FINAL
            {filter}
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

    def get_spot_recent(self, ticker_id, limit, offset, type, start_time, end_time):
        return self.make_query(f"""
            select 
                msg_index as trade_id,
                toUnixTimestamp(b.timestamp) as timestamp, 
                if(offer_coin_denom = a_denom, 'sell', 'buy') as type,
                if(offer_coin_denom = a_denom, offer_coin_amount, exchanged_demand_coin_amount) as base_volume,
                if(offer_coin_denom = b_denom, offer_coin_amount, exchanged_demand_coin_amount) as quote_volume,
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
    def get_spot_summary(self, height, allowed_pool_ids):
        filter = ''
        if allowed_pool_ids:
            filter = f'WHERE pool_id IN ({", ".join(map(str, allowed_pool_ids))})'
        return self.make_query(f"""
            SELECT DISTINCT *
            FROM
              (SELECT b.pool_id AS pool_id,
                      CONCAT(a_denom, '_', b_denom) AS trading_pairs,
                      lp_t.last_price AS last_price,
                      min_max.lowest_price_24h AS lowest_price_24h,
                      min_max.highest_price_24h AS highest_price_24h,
                      fp.first_price AS first_price,
                      volumes.base_volume AS base_volume,
                      volumes.quote_volume AS quote_volume,
                      hb.highest_bid AS highest_bid,
                      la.lowest_ask AS lowest_ask
               FROM spacebox.liquidity_pool AS b
               LEFT JOIN
                 (SELECT pool_id,
                         swap_price AS last_price
                  FROM
                    (SELECT *,
                            RANK () OVER (PARTITION BY pool_id
                                          ORDER BY height DESC) AS custon_rank
                     FROM spacebox.swap
                     WHERE height > {height})
                  WHERE custon_rank = 1 ) AS lp_t ON b.pool_id = lp_t.pool_id
               LEFT JOIN
                 (SELECT pool_id,
                         min(swap_price) AS lowest_price_24h,
                         max(swap_price) AS highest_price_24h
                  FROM spacebox.swap
                  WHERE height > {height}
                  GROUP BY pool_id) AS min_max ON min_max.pool_id = b.pool_id
               LEFT JOIN
                 (SELECT pool_id,
                         swap_price AS first_price
                  FROM
                    (SELECT *,
                            RANK () OVER (PARTITION BY pool_id
                                          ORDER BY height ASC) AS custon_rank
                     FROM spacebox.swap
                     WHERE height > {height} )
                  WHERE custon_rank = 1 ) AS fp ON fp.pool_id = b.pool_id
               LEFT JOIN
                 (SELECT sum(base_volume) AS base_volume,
                         sum(target_volume) AS quote_volume,
                         pool_id
                  FROM
                    (SELECT pool_id,
                            if(offer_coin_denom = a_denom, offer_coin_amount, exchanged_demand_coin_amount) AS base_volume,
                            if(offer_coin_denom = b_denom, offer_coin_amount, exchanged_demand_coin_amount) AS target_volume
                     FROM
                       (SELECT s.*,
                               pool_id,
                               CONCAT(a_denom, '_', b_denom) AS ticker_id,
                               a_denom,
                               b_denom
                        FROM spacebox.liquidity_pool AS lp FINAL
                        LEFT JOIN
                          (SELECT *
                           FROM spacebox.swap FINAL) AS s ON s.pool_id = lp.pool_id
                        WHERE s.success = TRUE
                          AND s.height > {height} ))
                  GROUP BY pool_id) AS volumes ON volumes.pool_id = b.pool_id
               LEFT JOIN
                 (SELECT max(price) AS highest_bid,
                         pool_id
                  FROM
                    (SELECT pool_id,
                            if(offer_coin_denom = a_denom, 'sell', 'buy') AS TYPE,
                            swap_price AS price
                     FROM
                       (SELECT s.*,
                               pool_id,
                               a_denom,
                               b_denom
                        FROM spacebox.liquidity_pool AS lp FINAL
                        LEFT JOIN
                          (SELECT *
                           FROM spacebox.swap FINAL) AS s ON s.pool_id = lp.pool_id
                        WHERE s.success = TRUE
                          AND s.height > {height} )
                     WHERE TYPE = 'sell' )
                  GROUP BY pool_id) AS hb ON hb.pool_id = b.pool_id
               LEFT JOIN
                 (SELECT min(price) AS lowest_ask,
                         pool_id
                  FROM
                    (SELECT pool_id,
                            if(offer_coin_denom = a_denom, 'sell', 'buy') AS TYPE,
                            swap_price AS price
                     FROM
                       (SELECT s.*,
                               pool_id,
                               a_denom,
                               b_denom
                        FROM spacebox.liquidity_pool AS lp FINAL
                        LEFT JOIN
                          (SELECT *
                           FROM spacebox.swap FINAL) AS s ON s.pool_id = lp.pool_id
                        WHERE s.success = TRUE
                          AND s.height > {height} )
                     WHERE TYPE = 'buy' )
                  GROUP BY pool_id) AS la ON la.pool_id = b.pool_id)
                  {filter}
        """)

    def get_spot_ticker(self, height, allowed_pool_ids):
        filter = ''
        if allowed_pool_ids:
            filter = f'WHERE pool_id IN ({", ".join(map(str, allowed_pool_ids))})'
        return self.make_query(f"""
            SELECT DISTINCT *
            FROM
              (SELECT b.pool_id AS pool_id,
                      CONCAT(a_denom, '_', b_denom) AS trading_pairs,
                      lp_t.last_price AS last_price,
                      volumes.base_volume AS base_volume,
                      volumes.quote_volume AS quote_volume
               FROM spacebox.liquidity_pool AS b
               LEFT JOIN
                 (SELECT pool_id,
                         swap_price AS last_price
                  FROM
                    (SELECT *,
                            RANK () OVER (PARTITION BY pool_id
                                          ORDER BY height DESC) AS custon_rank
                     FROM spacebox.swap
                     WHERE height > {height})
                  WHERE custon_rank = 1 ) AS lp_t ON b.pool_id = lp_t.pool_id
               LEFT JOIN
                 (SELECT sum(base_volume) AS base_volume,
                         sum(target_volume) AS quote_volume,
                         pool_id
                  FROM
                    (SELECT pool_id,
                            if(offer_coin_denom = a_denom, offer_coin_amount, exchanged_demand_coin_amount) AS base_volume,
                            if(offer_coin_denom = b_denom, offer_coin_amount, exchanged_demand_coin_amount) AS target_volume
                     FROM
                       (SELECT s.*,
                               pool_id,
                               CONCAT(a_denom, '_', b_denom) AS ticker_id,
                               a_denom,
                               b_denom
                        FROM spacebox.liquidity_pool AS lp FINAL
                        LEFT JOIN
                          (SELECT *
                           FROM spacebox.swap FINAL) AS s ON s.pool_id = lp.pool_id
                        WHERE s.success = TRUE
                          AND s.height > {height} ))
                  GROUP BY pool_id) AS volumes ON volumes.pool_id = b.pool_id
            )
            {filter}
        """)

    def get_last_24_hours_volume_pairs(self, height, allowed_pool_ids):
        filter = f'({", ".join(map(str, allowed_pool_ids))})'
        return self.make_query(f"""
            select 
                offer_coin_denom as base_currency, 
                demand_coin_denom as target_currency, 
                sum(offer_coin_amount) as base_volume, 
                sum(exchanged_demand_coin_amount) as target_volume 
            from spacebox.swap
            where height  > {height} and success = TRUE and pool_id in {filter}
            group by offer_coin_denom , demand_coin_denom
        """)
