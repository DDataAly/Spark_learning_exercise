import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from config_loader import load_config
import os

logger = logging.getLogger()
logger.setLevel("DEBUG")
formatter = logging.Formatter("{asctime} - {levelname} - {message}", style = '{', datefmt="%Y-%m-%d %H:%M")

console_handler = logging.StreamHandler()
console_handler.setLevel('INFO')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

config = load_config()
log_folder_path = Path(config['paths']['logs'])
os.makedirs(log_folder_path, exist_ok = True)
log_full_path = log_folder_path / 'debug_level_log.log'

file_handler = TimedRotatingFileHandler(filename = log_full_path, when = 'D', interval = 1, backupCount = 2)  
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


if __name__ == '__main__':
    logger.debug('Badger went hunting')
    logger.info('This is information valuable to everyone')


