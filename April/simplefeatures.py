import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import psycopg2

# Configuration
DB_CONNECTION = 'postgresql+psycopg2://postgres:bubZ$tep433@localhost:5432/ohlcv_db'
SYMBOLS = ['BTC', 'SOL']

def get_ohlcv_data(engine, symbol):
    """Retrieve all OHLCV data for a symbol from the database"""
    query = f"""
    SELECT * FROM public.ohlcv 
    WHERE symbol = '{symbol}'
    ORDER BY timestamp ASC
    """
    
    conn = engine.connect()
    result = conn.execute(text(query))
    
    # Convert result to DataFrame manually
    columns = result.keys()
    data = [dict(zip(columns, row)) for row in result]
    conn.close()
    
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Convert timestamp to datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df

def calculate_simple_features(df):
    """Calculate very basic features that work with minimal data"""
    if df is None or len(df) < 3:  # Need at least 3 rows
        print(f"Not enough data to calculate features. Need at least 3 rows, got {len(df) if df is not None else 0}")
        return None
    
    # Create a copy to avoid fragmentation
    df = df.copy()
    
    # Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Basic features that don't require much history
    
    # Rename volume columns for clarity
    df['volume_btc'] = df['volumefrom']  # For BTC/USD, volumefrom is BTC
    df['volume_usd'] = df['volumeto']    # For BTC/USD, volumeto is USD
    
    # Price range percentage
    df['price_range_pct'] = (df['high'] - df['low']) / df['low']
    
    # Open-close change percentage
    df['oc_change_pct'] = (df['close'] - df['open']) / df['open']
    
    # Simple moving averages (use small windows)
    df['ma_3h'] = df['close'].rolling(window=min(3, len(df))).mean()
    
    # Only calculate 6h and 12h if we have enough data
    if len(df) >= 6:
        df['ma_6h'] = df['close'].rolling(window=6).mean()
    else:
        df['ma_6h'] = df['close']
        
    if len(df) >= 12:
        df['ma_12h'] = df['close'].rolling(window=12).mean()
    else:
        df['ma_12h'] = df['close']
    
    # Standard deviations (use small windows)
    df['rolling_std_3h'] = df['close'].rolling(window=min(3, len(df))).std()
    
    if len(df) >= 6:
        df['rolling_std_6h'] = df['close'].rolling(window=6).std()
    else:
        df['rolling_std_6h'] = 0
        
    if len(df) >= 12:
        df['rolling_std_12h'] = df['close'].rolling(window=12).std()
    else:
        df['rolling_std_12h'] = 0
    
    # Volume-based features
    df['volume_return_1h'] = df['volume_btc'].pct_change()
    
    # Derived features
    df['volume_btc_x_range'] = df['volume_btc'] * df['price_range_pct']
    df['rolling_std_3h_sq'] = df['rolling_std_3h'] ** 2
    
    # Price return (current period)
    df['price_return_1h'] = df['close'].pct_change()
    df['price_return_1h_sq'] = df['price_return_1h'] ** 2
    
    # Only calculate sqrt if we have 12h data
    if len(df) >= 12:
        df['rolling_std_12h_sqrt'] = np.sqrt(np.abs(df['rolling_std_12h']))
    else:
        df['rolling_std_12h_sqrt'] = 0
    
    # Target: Future return (1-hour ahead)
    df['target_1h_return'] = df['close'].shift(-1).pct_change(1)
    
    # Fill all remaining NaN values with 0
    df = df.fillna(0)
    
    # Set all columns that we're not calculating to 0
    all_columns = [
        'timestamp', 'symbol', 'open', 'high', 'low', 'close', 
        'volume_btc', 'volume_usd', 'garman_klass_12h', 'price_range_pct', 
        'oc_change_pct', 'parkinson_3h', 'ma_3h', 'rolling_std_3h', 
        'lag_3h_price_return', 'lag_6h_price_return', 'lag_12h_price_return', 
        'lag_24h_price_return', 'lag_48h_price_return', 'lag_72h_price_return', 
        'lag_168h_price_return', 'volume_return_1h', 'lag_3h_volume_return', 
        'lag_6h_volume_return', 'lag_12h_volume_return', 'lag_24h_volume_return', 
        'ma_6h', 'ma_12h', 'ma_24h', 'ma_48h', 'ma_72h', 'ma_168h', 
        'rolling_std_6h', 'rolling_std_12h', 'rolling_std_24h', 'rolling_std_48h', 
        'rolling_std_72h', 'rolling_std_168h', 'atr_14h', 'atr_24h', 'atr_48h', 
        'close_div_ma_24h', 'close_div_ma_48h', 'close_div_ma_168h', 
        'ma12_div_ma48', 'ma24_div_ma168', 'std12_div_std72', 
        'volume_btc_x_range', 'rolling_std_3h_sq', 'price_return_1h_sq', 
        'rolling_std_12h_sqrt', 'target_1h_return'
    ]
    
    # Add any missing columns with zeros
    for col in all_columns:
        if col not in df.columns:
            df[col] = 0
    
    return df[all_columns]

def insert_features_direct(df, db_connection_string):
    """Insert features directly using psycopg2"""
    if df is None or len(df) == 0:
        return 0
    
    # Replace NaN with None for SQL compatibility
    df = df.replace({np.nan: None})
    
    # Connect directly to PostgreSQL
    conn_params = {}
    for param in db_connection_string.split('://')[1].split('@')[0].split(':'):
        if '=' in param:
            key, value = param.split('=')
            conn_params[key] = value
    
    username = db_connection_string.split('://')[1].split(':')[0]
    password = db_connection_string.split('://')[1].split(':')[1].split('@')[0]
    host = db_connection_string.split('@')[1].split(':')[0]
    port = db_connection_string.split('@')[1].split(':')[1].split('/')[0]
    dbname = db_connection_string.split('/')[-1]
    
    conn = psycopg2.connect(
        dbname=dbname,
        user=username,
        password=password,
        host=host,
        port=port
    )
    
    cursor = conn.cursor()
    
    try:
        # Clear existing data first (optional)
        cursor.execute("DELETE FROM advanced_features")
        
        # Get column names
        columns = df.columns
        
        # Create placeholders for SQL query
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Create SQL query
        query = f"INSERT INTO advanced_features ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT (timestamp, symbol) DO NOTHING"
        
        # Prepare data for insertion
        data = [tuple(row) for row in df.values]
        
        # Execute in batches
        batch_size = 1000
        inserted = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(query, batch)
            inserted += cursor.rowcount
        
        conn.commit()
        return inserted
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to create simple features"""
    print(f"[{datetime.now()}] Starting simple feature creation...")
    
    # Connect to database
    engine = create_engine(DB_CONNECTION)
    
    all_features = []
    
    # Process each symbol
    for symbol in SYMBOLS:
        try:
            print(f"Processing data for {symbol}...")
            
            # Get all OHLCV data
            df = get_ohlcv_data(engine, symbol)
            
            if len(df) > 0:
                print(f"Retrieved {len(df)} rows for {symbol}")
                
                # Calculate simple features
                features_df = calculate_simple_features(df)
                
                if features_df is not None and len(features_df) > 0:
                    print(f"Generated {len(features_df)} rows of features for {symbol}")
                    all_features.append(features_df)
                else:
                    print(f"No valid features generated for {symbol}")
            else:
                print(f"No data found for {symbol}")
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
    
    # Combine all features
    if all_features:
        combined_features = pd.concat(all_features, ignore_index=False)
        print(f"Total features: {len(combined_features)} rows")
        
        # Insert into database
        inserted = insert_features_direct(combined_features, DB_CONNECTION)
        print(f"Inserted {inserted} rows into advanced_features table")
    else:
        print("No features to insert")
    
    print(f"[{datetime.now()}] Simple feature creation complete!")

if __name__ == "__main__":
    main()