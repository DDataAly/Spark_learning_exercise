import time
from datetime import datetime, timedelta
from base_api_fetcher import TradingPlatform
from src.utils.config_loader import load_config


class Binance(TradingPlatform):
    def _build_request_params(self, ticker, interval, limit=1000, start_time=None, end_time=None):
        # Building dictionary with mandatory parameters
        params = {
            "symbol": ticker,
            "interval": interval,
            "limit": limit
        }
        # Optional startTime parameter for fetching historical data
        # EndTime is not required as we have startTime and limit
        # Defined outside the initial dictionary as not always required, and passing None would cause BadRequest error
        # Binance expects time in milliseconds - so int = num of milliseconds from 1 Jan 1970
        if start_time:
            params['startTime'] = int(start_time)   # int(start_time) in case we got start_time as a float after conversion
        return params    


    def get_ticker_data(self, ticker, interval=None, limit=1000, start_time=None, end_time=None):
        interval = interval or self.interval # this allow us to call function with different interval without changing the config file
        params = self._build_request_params(ticker, interval, limit, start_time, end_time)
        return self.request_candlestick_data(params)
    
    #TODO the goal is to produce N chunks of data of M volume (probably 1 month) each - need to do a separate function to handle the N chunks loop logic
    def get_historical_data(self, ticker, num_months =1):
        data_chunk =[]

        end_time = datetime.now()
        start_time = end_time - timedelta(days = 30*num_months)
        current_start_time = int(start_time.timestamp() *1000)

        print(f'Fetching data for {ticker} from {start_time} to {end_time}')

        while True:
            data  = self.get_ticker_data (ticker, start_time = current_start_time)
            if not data:
                break
            data_chunk.extend(data)
            current_start_time = data[-1][0] + 1
            if datetime.fromtimestamp(current_start_time/1000) >= end_time:
                break
            time.sleep(0.2)
        return data_chunk


if __name__ == "__main__":
    print ('cats')
    config = load_config()
    binance_config = config ['binance']

    data_source = Binance(
                            name = 'binance',
                            url = binance_config['base_url'],
                            tickers = ["BTCUSDT"],
                            interval = binance_config['interval']
                            )
    
    for ticker in data_source.tickers:
        data = data_source. get_historical_data(ticker = ticker)
        print(len(data))
