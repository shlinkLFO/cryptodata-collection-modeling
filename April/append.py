import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

# Database connection
DB_CONNECTION = 'postgresql+psycopg2://postgres:bubZ$tep433@localhost:5432/ohlcv_db'

# CSV file path
CSV_FILE = r'C:\Users\mason\AVP\Bitstamp_BTCUSD_1h.csv'

def main():
    print(f"Loading data from {CSV_FILE}...")
    
    # Read CSV file
    df = pd.read_csv(CSV_FILE)
    
    # Print the first few rows and info to verify data
    print("First few rows of the CSV file:")
    print(df.head())
    print("\nCSV file info:")
    print(df.info())
    
    # Convert unix timestamp to datetime format matching the API
    df['timestamp'] = pd.to_datetime(df['unix'], unit='s')
    
    # Ensure symbol is consistent
    df['symbol'] = 'BTC'
    
    # Rename volume columns to match database schema
    df = df.rename(columns={
        'Volume BTC': 'volumefrom',
        'Volume USD': 'volumeto'
    })
    
    # Select and reorder columns to match database schema
    df_to_insert = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']]
    
    # Check for and handle any NaN values
    if df_to_insert.isna().any().any():
        print("Warning: Found NaN values in the data. Filling with appropriate values...")
        # Fill numeric columns with appropriate values
        numeric_cols = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        for col in numeric_cols:
            # Forward fill, then backward fill to handle any remaining NaNs
            df_to_insert[col] = df_to_insert[col].fillna(method='ffill').fillna(method='bfill')
    
    # Check for and handle any infinite values
    if np.isinf(df_to_insert.select_dtypes(include=[np.number]).values).any():
        print("Warning: Found infinite values in the data. Replacing with NaN and then filling...")
        df_to_insert = df_to_insert.replace([np.inf, -np.inf], np.nan)
        # Fill numeric columns with appropriate values
        numeric_cols = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        for col in numeric_cols:
            df_to_insert[col] = df_to_insert[col].fillna(method='ffill').fillna(method='bfill')
    
    # Connect to database
    print("Connecting to database...")
    engine = create_engine(DB_CONNECTION)
    
    # Check existing data to avoid duplicates
    print("Checking for existing data...")
    min_date = df_to_insert['timestamp'].min()
    max_date = df_to_insert['timestamp'].max()
    
    query = f"""
    SELECT timestamp FROM public.ohlcv 
    WHERE symbol = 'BTC' 
    AND timestamp BETWEEN '{min_date}' AND '{max_date}'
    """
    
    existing_timestamps = pd.read_sql(query, engine)['timestamp'].tolist()
    
    # Filter out rows that already exist in the database
    if existing_timestamps:
        print(f"Found {len(existing_timestamps)} existing entries in this date range.")
        df_to_insert = df_to_insert[~df_to_insert['timestamp'].isin(existing_timestamps)]
    
    # Insert data
    if len(df_to_insert) > 0:
        print(f"Inserting {len(df_to_insert)} new rows into database...")
        
        # Insert in chunks to avoid memory issues with large datasets
        chunk_size = 1000
        total_rows = len(df_to_insert)
        
        for i in range(0, total_rows, chunk_size):
            end_idx = min(i + chunk_size, total_rows)
            chunk = df_to_insert.iloc[i:end_idx]
            chunk.to_sql('ohlcv', engine, if_exists='append', index=False)
            print(f"Inserted chunk {i//chunk_size + 1}/{(total_rows-1)//chunk_size + 1} ({end_idx}/{total_rows} rows)")
        
        print("Data insertion complete!")
    else:
        print("No new data to insert.")
    
    # Verify insertion
    query = "SELECT COUNT(*) FROM public.ohlcv WHERE symbol = 'BTC'"
    result = pd.read_sql(query, engine)
    print(f"Total BTC entries in database: {result.iloc[0, 0]}")
    
    # Show some sample data from different time periods
    print("\nSample of earliest data:")
    query = """
    SELECT * FROM public.ohlcv 
    WHERE symbol = 'BTC' 
    ORDER BY timestamp ASC 
    LIMIT 5
    """
    print(pd.read_sql(query, engine))
    
    print("\nSample of latest data:")
    query = """
    SELECT * FROM public.ohlcv 
    WHERE symbol = 'BTC' 
    ORDER BY timestamp DESC 
    LIMIT 5
    """
    print(pd.read_sql(query, engine))

if __name__ == "__main__":
    main()