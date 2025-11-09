import json
from pathlib import Path

def load_config(file_name = 'config.json'):
    root = Path(__file__).resolve().parent.parent.parent
    path_to_config_file = root / file_name

    with open (path_to_config_file, 'r') as file:
        return json.load(file)



