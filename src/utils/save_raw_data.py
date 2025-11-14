import json
from pathlib import Path

def save_json(data, filename, config):
    raw_data_path = Path(config["paths"]["raw_data_dir"])  # create a path object from a string
    # Filename should have format <exchange_name>_<ticker>_<interval>_<last_candle_open_time>
    full_path = raw_data_path / filename   
    full_path = full_path.with_suffix(".json")   # Add .json suffix
    full_path.parent.mkdir(parents=True, exist_ok=True)    

    with open(full_path, "w") as f:
        json.dump(data, f)

