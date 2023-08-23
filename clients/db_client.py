from datetime import timedelta, datetime
from typing import Optional, List

import clickhouse_connect

from common.db_connector import DBConnector
from collections import namedtuple


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
            SELECT a_denom AS base, b_denom AS target, pool_id, CONCAT(a_denom, b_denom) AS ticker_id  FROM spacebox.liquidity_pool FINAL
        """)
