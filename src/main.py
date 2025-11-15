from src.extract.binance_api_fetcher import Binance
from src.utils.config_loader import load_config
import logging
import src.utils.logger

logger=logging.getLogger('main')

def main():
    config = load_config()
    data_source = Binance(config)

    for ticker in data_source.tickers:
            logger.info(f'Starting a fetch for for {ticker} from {data_source.name}')
            data_source. get_historical_data(ticker = ticker)
            logger.info(f' Data for {ticker} from {data_source.name} is fetched and saved successfully')


if __name__ == "__main__":
    main()
    print('cats')



