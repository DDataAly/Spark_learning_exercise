from .base_api_fetcher import TradingPlatform

class Binance(TradingPlatform):

    def _build_request_params(self, ticker, interval, limit=1000):
        return {
            "symbol": ticker,
            "interval": interval,
            "limit": limit
        }
    
    
    def get_ticker_data(self, ticker, interval=None, limit=1000):
        # Setting up interval = None gives us flexibility:
        # If it's None, interval = self.interval which reads the config value of interval for Binance
        # If it's not None, we can call the function with whatever value we want at the point of call
        interval = interval or self.interval 
        params = self._build_request_params(ticker, interval, limit)
        return self.request_candlestick_data(params)

