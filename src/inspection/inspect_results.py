import sys
from pathlib import Path
import pandas as pd
import duckdb

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

def inspect_data_lake():
    """Inspect Parquet Data Lake"""
    print("=" * 80)
    print("PARQUET DATA LAKE INSPECTION")
    print("=" * 80)
    
    data_lake_path = Path(__file__).parent.parent.parent / "data_lake"
    
    # List all parquet files
    parquet_files = list(data_lake_path.glob("**/part-*.parquet"))
    print(f"\nTotal parquet files: {len(parquet_files)}\n")
    
    # Inspect each city partition
    for city_folder in sorted(data_lake_path.glob("city=*")):
        city_name = city_folder.name.replace("city=", "")
        parquet_files_in_city = list(city_folder.glob("part-*.parquet"))
        
        if parquet_files_in_city:
            # Read first parquet file to show structure
            df = pd.read_parquet(parquet_files_in_city[0])
            print(f"📁 {city_name}")
            print(f"   Files: {len(parquet_files_in_city)}")
            print(f"   Rows: {len(df):,}")
            print(f"   Columns: {list(df.columns)}")
            print(f"   Date range: {df['date'].min()} to {df['date'].max()}")
            print()


def inspect_duckdb_warehouse():
    """Inspect DuckDB Warehouse"""
    print("=" * 80)
    print("DUCKDB WAREHOUSE INSPECTION")
    print("=" * 80)
    
    warehouse_path = Path(__file__).parent.parent.parent / "warehouse" / "air_quality.duckdb"
    
    conn = duckdb.connect(str(warehouse_path), read_only=True)
    
    try:
        # List tables
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        print(f"\nTables in warehouse: {[t[0] for t in tables]}\n")
        
        # Inspect each table
        for table_name, in tables:
            print(f"📊 Table: {table_name}")
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
            print(f"   Rows: {row_count:,}")
            
            # Get columns
            schema = conn.execute(f"SELECT * FROM {table_name} LIMIT 0").description
            columns = [(col[0], col[1]) for col in schema]
            print(f"   Columns: {[c[0] for c in columns]}")
            print(f"   Types: {dict(columns)}")
            
            # Show sample data
            print(f"   Sample (first 3 rows):")
            sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
            for i, row in enumerate(sample):
                print(f"      {i+1}: {row}")
            print()
    
    finally:
        conn.close()


def inspect_processed_data():
    """Inspect Clean Dataset CSV"""
    print("=" * 80)
    print("PROCESSED CLEAN DATASET INSPECTION")
    print("=" * 80)
    
    processed_path = Path(__file__).parent.parent.parent / "data" / "processed" / "clean_dataset.csv"
    
    if processed_path.exists():
        df = pd.read_csv(processed_path)
        print(f"\nFile: {processed_path}")
        print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        print(f"\nColumns:\n{df.dtypes}")
        print(f"\nBasic Statistics:")
        print(df.describe())
        print(f"\nCities in dataset: {sorted(df['city'].unique())}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    else:
        print(f"File not found: {processed_path}")


if __name__ == "__main__":
    inspect_data_lake()
    print("\n\n")
    inspect_duckdb_warehouse()
    print("\n\n")
    inspect_processed_data()
