from src.extract.binance_api_fetcher import Binance
from src.utils.save_raw_data import save_json
from src.utils.config_loader import load_config

def main():
    config = load_config()
    binance_config = config ['binance']

    data_source = Binance(
                            name = 'binance',
                            url = binance_config['base_url'],
                            tickers = binance_config ['tickers'],
                            interval = binance_config['interval']
                            )
    for ticker in data_source.tickers:
            data = data_source. get_ticker_data(ticker = ticker)
            file_name = f"{data_source.name}_{ticker}_{data_source.interval}"
            save_json(data, file_name, config)

if __name__ == "__main__":
    main()


