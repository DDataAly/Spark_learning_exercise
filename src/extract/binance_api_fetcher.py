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
        print(params)    
        return params    


    def get_ticker_data(self, ticker, interval=None, limit=1000, start_time=None, end_time=None):
        interval = interval or self.interval # this allow us to call function with different interval without changing the config file
        params = self._build_request_params(ticker, interval, limit, start_time, end_time)
        return self.request_candlestick_data(params)
    
    
    #TODO - add a simple continuity check verification to get_historical_data, then refactor everything 

    # Two loops:
    # outer loop defines the length of the historical period
    # It does end_time = datetime.now() logic and calculates start and end of the iteration (=calculates start and end of each cycle) 
    # Inner loop is my current while loops
    # Sample of server response data

    #     [
    #   [
    #     1499040000000,      // Kline open time
    #     "0.01634790",       // Open price
    #     "0.80000000",       // High price
    #     "0.01575800",       // Low price
    #     "0.01577100",       // Close price
    #     "148976.11427815",  // Volume
    #     1499644799999,      // Kline Close time
    #     "2434.19055334",    // Quote asset volume
    #     308,                // Number of trades
    #     "1756.87402397",    // Taker buy base asset volume
    #     "28.46694368",      // Taker buy quote asset volume
    #     "0"                 // Unused field, ignore.
    #   ]
    # ]

    def trim_data_chunk (self, data_chunk, chunk_end_time, overlap_in_sec = 190):
        # Cover the candle where chunk_end_time belongs and 2 following candles
        print(f"This is chunk_end_time the trimmer got: {chunk_end_time}") #1762605502839
        print(f'In human terms it\'s equal {datetime.fromtimestamp(chunk_end_time/1000)}')
        trimmed_data_chunk = [candle for candle in data_chunk if candle[6] <= chunk_end_time + overlap_in_sec*1000] # 2 candles overlap
        return trimmed_data_chunk


    def get_data_chunk (self, ticker, chunk_start_time, chunk_end_time):
        data_chunk =[]
        current_start_time = chunk_start_time
        print(f'This was initial start time {current_start_time}')
        while True:
            print("Fetching data")
            data = self.get_ticker_data (ticker, start_time=current_start_time)
            if data:
                print(
                    f"First candle open: {datetime.fromtimestamp(data[0][0]/1000)}, "
                    f"Last candle open: {datetime.fromtimestamp(data[-1][0]/1000)}"
                )
            if not data:
                break
            data_chunk.extend(data)
            print('Data fetched')
            current_start_time = data[-1][0] + 1
            print(f'This is the current start time {current_start_time}')
            if current_start_time >= chunk_end_time:
                break
            time.sleep(0.2)
        return data_chunk
    

    def get_historical_data (self, ticker, chunk_size_in_months = 0.1, num_months = 0.2):
        end_time = datetime.now()   #2025-11-11 10:08:01.521105
        start_time = end_time - timedelta(days = 30*num_months)

        chunk_start_time = int(start_time.timestamp() *1000)
        chunk_end_time = start_time + timedelta(days = 30*chunk_size_in_months) #2025-10-12 10:10:06.352677
        chunk_end_time = int(chunk_end_time.timestamp() *1000) #1760260311514

        print("Now (end_time):", end_time)
        print("Start of whole period:", start_time)
        print("Chunk start (ms):", chunk_start_time, datetime.fromtimestamp(chunk_start_time/1000))
        print("Chunk end (ms):", chunk_end_time, datetime.fromtimestamp(chunk_end_time/1000))


        while datetime.fromtimestamp(chunk_end_time/1000) <= end_time:
            data_chunk = self.get_data_chunk (ticker, chunk_start_time, chunk_end_time)
            trimmed_data_chunk = self.trim_data_chunk  (data_chunk, chunk_end_time)
            print('After timmming')
            for item in trimmed_data_chunk[-3:]:
                print('bla')
                print(item)
                print(
                    f"candle open: {datetime.fromtimestamp(item[0]/1000)}, "
                    f"candle close: {datetime.fromtimestamp(item[6]/1000)}"
                )
            break
            chunk_start_time = chunk_end_time
            chunk_end_time = datetime.fromtimestamp(chunk_start_time/1000) + timedelta(days = 30*chunk_size_in_months)
            chunk_end_time = int(chunk_end_time.timestamp() *1000)
            print('Loop iteration')
            print(len(data_chunk))


    
    
    # #TODO the goal is to produce N chunks of data of M volume (probably 1 month) each - need to do a separate function to handle the N chunks loop logic
    # def get_historical_data(self, ticker, num_months =1):
    #     data_chunk =[]

    #     end_time = datetime.now()
    #     start_time = end_time - timedelta(days = 30*num_months)
    #     current_start_time = int(start_time.timestamp() *1000)

    #     print(f'Fetching data for {ticker} from {start_time} to {end_time}')

    #     while True:
    #         data  = self.get_ticker_data (ticker, start_time = current_start_time)
    #         if not data:
    #             break
    #         data_chunk.extend(data)
    #         current_start_time = data[-1][0] + 1
    #         if datetime.fromtimestamp(current_start_time/1000) >= end_time:
    #             break
    #         time.sleep(0.2)
    #     return data_chunk


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
        # print(len(data))
    print('Completed successfully')    

