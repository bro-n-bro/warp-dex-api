from clients.db_client import DBClient


class WarpService:

    def __init__(self):
        self.db_client = DBClient()

    def get_pairs(self):
        result = self.db_client.get_pairs_liquidity_pool()
        return [item._asdict() for item in result]
