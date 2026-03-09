import pandas as pd
from pathlib import Path

def export_to_parquet_lake(df, output_dir=None):
    """
    Export data to Parquet Data Lake with partition by city
    Structure: data_lake/city=<city_name>/data_<timestamp>.parquet
    """
    if output_dir is None:
        output_dir = Path(__file__).parent.parent.parent / "data_lake"
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Group by city and save each city's data
    saved_files = []
    for city in df['city'].unique():
        city_data = df[df['city'] == city]
        city_dir = output_dir / f"city={city}"
        city_dir.mkdir(parents=True, exist_ok=True)
        
        # Save parquet with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_file = city_dir / f"data_{timestamp}.parquet"
        
        city_data.to_parquet(parquet_file, index=False)
        saved_files.append(str(parquet_file))
        print(f"Saved {len(city_data)} records to {parquet_file}")
    
    return saved_files


def read_parquet_lake(data_lake_dir=None, city=None):
    """Read parquet data from data lake, optionally filtered by city"""
    if data_lake_dir is None:
        data_lake_dir = Path(__file__).parent.parent.parent / "data_lake"
    
    data_lake_dir = Path(data_lake_dir)
    
    if city:
        # Read specific city
        city_dir = data_lake_dir / f"city={city}"
        if not city_dir.exists():
            raise ValueError(f"City {city} not found in data lake")
        
        parquet_files = list(city_dir.glob("*.parquet"))
    else:
        # Read all cities
        parquet_files = list(data_lake_dir.glob("city=*/data_*.parquet"))
    
    if not parquet_files:
        raise ValueError(f"No parquet files found")
    
    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
