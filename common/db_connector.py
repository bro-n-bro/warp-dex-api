import clickhouse_connect

from config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USERNAME, CLICKHOUSE_PASSWORD


class DBConnector(object):

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DBConnector, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USERNAME,
            password=CLICKHOUSE_PASSWORD
        )

    def __del__(self):
        self.clickhouse_client.close()
