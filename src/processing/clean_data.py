
import pandas as pd

def clean_data(df):
    initial_rows = len(df)
    
    # Drop duplicates
    df = df.drop_duplicates()
    
    # Handle date columns - convert to consistent ISO format
    for col in df.columns:
        if "date" in col.lower():
            # Try to parse with different formats and merge results
            # Most files have M/D/YYYY, but some might have YYYY-MM-DD
            
            # Try M/D/YYYY format first (8/1/2022)
            parsed = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
            
            # For any rows that failed, try YYYY-MM-DD format (2022-08-01)
            failed_mask = parsed.isna()
            if failed_mask.any():
                parsed_alt = pd.to_datetime(df.loc[failed_mask, col], format='%Y-%m-%d', errors='coerce')
                parsed.loc[failed_mask] = parsed_alt
            
            # Update dataframe with parsed dates
            df[col] = parsed
            
            # Format back to ISO string format (YYYY-MM-DD)
            df[col] = df[col].dt.strftime('%Y-%m-%d')
    
    # Drop rows with ANY invalid date (only after trying all formats)
    if 'date' in df.columns:
        df = df[df['date'] != 'NaT'].dropna(subset=['date']).copy()
    
    rows_removed = initial_rows - len(df)
    if rows_removed > 0:
        print(f"⚠️  Removed {rows_removed} rows with invalid/missing data")
    
    return df
