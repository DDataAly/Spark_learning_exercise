import requests
import logging

class TradingPlatform:
    def __init__(
        self,
        name: str,
        url: str,
        tickers: list,
        interval: str
    ):
        self.name = name
        self.url = url
        self.tickers = tickers 
        self.interval = interval


    def _build_request_params(self, **kwargs): # Using kwargs to show that children implementations of this method will have some parameters
        raise NotImplementedError("Method must be implemented in subclasses")



    def request_candlestick_data(self, params):
        try:
            snapshot = requests.get(self.url, params = params)
            snapshot.raise_for_status()  # raises a requests.exceptions.HTTPError if the response contains a 4xx or 5xx status code.
            return snapshot.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"{self.name}: HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"{self.name}: A request error occurred: {e}")
        return None

