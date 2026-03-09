
import pandas as pd
import glob
import os
from pathlib import Path

def load_all_csv(raw_folder=None):
    if raw_folder is None:
        # Get the project root directory and point to AIQ_datasets
        project_root = Path(__file__).parent.parent.parent.parent
        raw_folder = os.path.join(project_root, "AIQ_datasets")
    
    # Only load air quality historical data (skip city_info and data_dictionary)
    files = glob.glob(os.path.join(raw_folder, "**/air_quality_historical.csv"), recursive=True)
    dfs = []

    for f in files:
        try:
            df = pd.read_csv(f)
            # Extract city name from folder path
            # E.g., "..AIQ_datasets/Bangkok/air_quality_historical.csv" -> "Bangkok"
            path_parts = Path(f).parts
            city = None
            
            # Find the city folder (folder right after AIQ_datasets)
            for i, part in enumerate(path_parts):
                if part == "AIQ_datasets" and i + 1 < len(path_parts):
                    city = path_parts[i + 1]
                    break
            
            df["city"] = city
            df["source_file"] = os.path.basename(f)
            dfs.append(df)
        except Exception as e:
            print(f"Failed to load {f}: {e}")

    if not dfs:
        raise ValueError(f"No air_quality_historical.csv files found in {raw_folder}")

    return pd.concat(dfs, ignore_index=True)
