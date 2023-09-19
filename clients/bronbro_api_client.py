import asyncio

import requests

from common.decorators import response_decorator
from config import PRICE_FEED_API
from typing import Optional, List
from urllib.parse import urljoin


class BronbroApiClient:

    def __init__(self):
        self.price_feed_api_url = PRICE_FEED_API

    @response_decorator
    def rpc_get(self, url):
        url = urljoin(self.price_feed_api_url, url)
        return requests.get(url)

    def get_exchange_rates(self) -> List[dict]:
        return self.rpc_get('price_feed_api/tokens/')
