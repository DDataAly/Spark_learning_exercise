import json
import os
from pathlib import Path

# def save_json(data, filename, config):
#     # Filename in format <exchange_name>_<ticker>_<interval>.json
#     raw_data_path = Path(config ['paths']['raw_data_dir'])
#     os.makedirs(raw_data_path, exist_ok=True)
#     full_path = raw_data_path / filename
   
#     with open(full_path, "w") as f:
#         json.dump(data, f)

def save_json(data, filename, config):
    raw_data_path = Path(config["paths"]["raw_data_dir"])  # convert to Path
    full_path = raw_data_path / filename                   # now / works
    full_path.parent.mkdir(parents=True, exist_ok=True)    # ensure folders exist

    # Add .json if not already present
    if not full_path.suffix == ".json":
        full_path = full_path.with_suffix(".json")

    with open(full_path, "w") as f:
        json.dump(data, f)

