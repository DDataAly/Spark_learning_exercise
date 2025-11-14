# TODO:
# - add logging
# - raise error in case of missing data
# - add consistent docstrings



import time
from datetime import datetime, timedelta
from .base_api_fetcher import TradingPlatform
from src.utils.time_conversion import convert_datetime_to_unix_in_ms as dt_to_ms, convert_unix_in_ms_to_datetime as ms_to_dt, calculate_chunk_end_time
from src.utils.save_raw_data import save_json


CANDLE_OPEN_TIME = 0
CANDLE_CLOSE_TIME = 6
CHUNK_OVERLAP_SEC = 190


class Binance(TradingPlatform):    
    '''
        Binance API fetcher.
        Binance candle schema returned by get_ticker_data:
                [
                [
                1499040000000,      // 0 - Kline open time - Unix time from 1st Jan 1970 in milliseconds
                "0.01634790",       // 1 - Open price
                "0.80000000",       // 2 - High price
                "0.01575800",       // 3 - Low price
                "0.01577100",       // 4 - Close price
                "148976.11427815",  // 5 - Volume
                1499644799999,      // 6 - Kline Close time - Unix time from 1st Jan 1970 in milliseconds
                "2434.19055334",    // 7 - Quote asset volume
                308,                // 8 - Number of trades
                "1756.87402397",    // 9 - Taker buy base asset volume
                "28.46694368",      // 10 - Taker buy quote asset volume
                "0"                 // 11 - Unused field, ignore
                ]
            ]
    '''
    def __init__ (self, config):
        super().__init__('binance')
        self.config = config
        self.url = config ['binance']['url']
        self.tickers = config ['binance']['tickers']
        self.interval = config ['binance']['interval']
        self.limit = config ['binance'].get('limit',1000)
        



    def _build_request_params(self, ticker, limit = None, interval = None, start_time=None):
        ''' 
        Builds payload for the API call.  start_time key is added only if the parameter is passed since it's optional.
        int(start_time) conversion just a safety feature in case of any error with start_time conversion from datetetime
        interval and limit parameters could be passed to the function directly to avoid changing the config file
        
        Args: 
            ticker: str,
            limit: int,
            interval: str,
            start_time: int 
        Returns:
            dict: parameters for the API call
        '''
        interval = interval or self.interval 
        limit = limit or self.limit
        params = {
            "symbol": ticker,
            "interval": self.interval,
            "limit": self.limit
        }
        if start_time:
            params['startTime'] = int(start_time)   
        return params    



    def get_ticker_data(self, ticker, interval=None, limit=None, start_time=None):
        params = self._build_request_params(ticker, limit, interval, start_time)
        return self.request_candlestick_data(params)
    
    
    def trim_data_chunk (self, data_chunk, chunk_end_time, chunk_overlap = CHUNK_OVERLAP_SEC):
        '''
        Trim the data chunk at the chunk_end_time plus 2 more candles
        '''
        trimmed_chunk = [candle for candle in data_chunk if candle[CANDLE_CLOSE_TIME] <= chunk_end_time + chunk_overlap*1000] 
        return trimmed_chunk


    def get_data_chunk (self, ticker, chunk_start_time, chunk_end_time):
        raw_chunk =[]
        current_start_time = chunk_start_time

        while True:
            print("Fetching data...")
            data = self.get_ticker_data (ticker, start_time=current_start_time)

            if not data:
                break

            print(
                f"First candle open: {datetime.fromtimestamp(data[0][CANDLE_OPEN_TIME]/1000)}, "
                f"Last candle open: {datetime.fromtimestamp(data[-1][CANDLE_OPEN_TIME]/1000)}"
                )           
            raw_chunk.extend(data)
            current_start_time = data[-1][CANDLE_OPEN_TIME] + 1

            if current_start_time >= chunk_end_time:
                break

            time.sleep(0.2)

        return raw_chunk
    
    def missing_data_detected (self, previous_candle_open_time, chunk_start_time):
        return previous_candle_open_time < chunk_start_time
    

    def get_historical_data (self, ticker, chunk_size_in_months = 0.1, num_months = 0.2):
        end_time = datetime.now()   #2025-11-11 10:08:01.521105
        start_time = end_time - timedelta(days = 30*num_months) #2025-10-11 10:08:01.521105

        chunk_start_time = dt_to_ms(start_time) #1762605502839
        chunk_end_time = calculate_chunk_end_time(chunk_start_time, chunk_size_in_months) #1756605502839

        print("Now (end of the whole period):", end_time)
        print("Start of whole period:", start_time)

        previous_candle_open_time = chunk_start_time


        while chunk_end_time <= dt_to_ms(end_time):
            print('Starting a loop')
            print("Chunk start (ms):", chunk_start_time, "or", ms_to_dt (chunk_start_time))
            print("Chunk end (ms):", chunk_end_time, "or", ms_to_dt(chunk_end_time))

            if self.missing_data_detected (previous_candle_open_time, chunk_start_time):
                print('Data is not continuos')
                break

            raw_chunk = self.get_data_chunk (ticker, chunk_start_time, chunk_end_time)
            trimmed_chunk = self.trim_data_chunk (raw_chunk, chunk_end_time)


            file_name = f"{self.name}_{ticker}_{self.interval}_{trimmed_chunk[-1][CANDLE_OPEN_TIME]}"
            save_json(trimmed_chunk, file_name, self.config)

            previous_candle_open_time = trimmed_chunk[-1][CANDLE_OPEN_TIME]
            chunk_start_time = chunk_end_time
            chunk_end_time = calculate_chunk_end_time(chunk_start_time, chunk_size_in_months)
            
            print(f'Data chunk is saved successfully. The length of the current data chunk is {len(raw_chunk)}')



if __name__ == "__main__":
    from src.utils.config_loader import load_config
    print ('cats')
    config = load_config()
    data_source = Binance(config)
    
    for ticker in data_source.tickers:
        data = data_source. get_historical_data(ticker = ticker)
    print('Completed successfully')   



    # Keeping this as might re-use some print statements when working on logging
    # def get_ticker_data(self, ticker, interval=None, limit=1000, start_time=None, end_time=None):
    #     '''
    #     This function returns data in this format for each candle:
    #             [
    #           [
    #             1499040000000,      // Kline open time
    #             "0.01634790",       // Open price
    #             "0.80000000",       // High price
    #             "0.01575800",       // Low price
    #             "0.01577100",       // Close price
    #             "148976.11427815",  // Volume
    #             1499644799999,      // Kline Close time
    #             "2434.19055334",    // Quote asset volume
    #             308,                // Number of trades
    #             "1756.87402397",    // Taker buy base asset volume
    #             "28.46694368",      // Taker buy quote asset volume
    #             "0"                 // Unused field, ignore
    #           ]
    #         ]

    #     '''
    #     interval = interval or self.interval # this allow us to call function with different interval without changing the config file
    #     params = self._build_request_params(ticker, interval, limit, start_time, end_time)
    #     return self.request_candlestick_data(params)
    
    

    # def trim_data_chunk (self, data_chunk, chunk_end_time, overlap_in_sec = 190):
    #     '''
    #     Trim the data chunk at the chunk_end_time plus 2 more candles
    #     '''
    #     print(f"This is chunk_end_time the trimmer got: {chunk_end_time}") #1762605502839
    #     print(f'In human terms it\'s equal {datetime.fromtimestamp(chunk_end_time/1000)}')
    #     trimmed_data_chunk = [candle for candle in data_chunk if candle[6] <= chunk_end_time + overlap_in_sec*1000] # 2 candles overlap
    #     return trimmed_data_chunk


    # def get_data_chunk (self, ticker, chunk_start_time, chunk_end_time):
    #     data_chunk =[]
    #     current_start_time = chunk_start_time
    #     print(f'This was initial start time {current_start_time}')
    #     while True:
    #         print("Fetching data")
    #         data = self.get_ticker_data (ticker, start_time=current_start_time)
    #         if data:
    #             print(
    #                 f"First candle open: {datetime.fromtimestamp(data[0][0]/1000)}, "
    #                 f"Last candle open: {datetime.fromtimestamp(data[-1][0]/1000)}"
    #             )
    #         if not data:
    #             break
    #         data_chunk.extend(data)
    #         print('Data fetched')
    #         current_start_time = data[-1][0] + 1
    #         print(f'This is the current start time {current_start_time}')
    #         if current_start_time >= chunk_end_time:
    #             break
    #         time.sleep(0.2)
    #     return data_chunk
    

    # def get_historical_data (self, ticker, chunk_size_in_months = 0.1, num_months = 0.2):
    #     end_time = datetime.now()   #2025-11-11 10:08:01.521105
    #     start_time = end_time - timedelta(days = 30*num_months)

    #     chunk_start_time = int(start_time.timestamp() *1000)
    #     chunk_end_time = start_time + timedelta(days = 30*chunk_size_in_months) #2025-10-12 10:10:06.352677
    #     chunk_end_time = int(chunk_end_time.timestamp() *1000) #1760260311514

    #     print("Now (end_time):", end_time)
    #     print("Start of whole period:", start_time)
    #     print("Chunk start (ms):", chunk_start_time, datetime.fromtimestamp(chunk_start_time/1000))
    #     print("Chunk end (ms):", chunk_end_time, datetime.fromtimestamp(chunk_end_time/1000))

    #     trimmed_data_chunk_last_candle_open_time = chunk_start_time


    #     while datetime.fromtimestamp(chunk_end_time/1000) <= end_time:
    #         print('Starting a loop')

    #         if trimmed_data_chunk_last_candle_open_time < chunk_start_time:
    #             print('Data is not continuos')
    #             break

    #         data_chunk = self.get_data_chunk (ticker, chunk_start_time, chunk_end_time)
    #         trimmed_data_chunk = self.trim_data_chunk  (data_chunk, chunk_end_time)

    #         trimmed_data_chunk_last_candle_open_time = trimmed_data_chunk[-1][0]
    #         print(f'Last candle open time: {datetime.fromtimestamp(trimmed_data_chunk_last_candle_open_time/1000)}') 


    #         print('After timmming')
    #         for item in trimmed_data_chunk[-3:]:
    #             print('bla')
    #             print(item)
    #             print(
    #                 f"candle open: {datetime.fromtimestamp(item[0]/1000)}, "
    #                 f"candle close: {datetime.fromtimestamp(item[6]/1000)}"
    #             )

    #         print('Loop iteration is completed')    

    #         chunk_start_time = chunk_end_time
    #         chunk_end_time = datetime.fromtimestamp(chunk_start_time/1000) + timedelta(days = 30*chunk_size_in_months)
    #         chunk_end_time = int(chunk_end_time.timestamp() *1000)
    #         print(f'The length of the current data chunk is {len(data_chunk)}')


    # def convert_datetime_to_unix_in_ms (datetime_time):
    #     return int(datetime_time.timestamp() *1000)
    
    # def convert_unix_in_ms_to_datetime(unix_time_in_ms):
    #     return datetime.fromtimestamp(unix_time_in_ms/1000)

 

