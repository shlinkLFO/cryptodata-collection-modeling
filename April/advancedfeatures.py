import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, DateTime, MetaData, text
import time
from datetime import datetime, timedelta
import os
import warnings

# Suppress pandas warnings
warnings.filterwarnings('ignore')

# Configuration
DB_CONNECTION = 'postgresql+psycopg2://postgres:bubZ$tep433@localhost:5432/ohlcv_db'
SYMBOLS = ['BTC', 'SOL']
BATCH_SIZE = 10000  # Process this many rows at a time

def create_advanced_features_table(engine):
    """Create the advanced features table if it doesn't exist"""
    metadata = MetaData()

    # Define advanced features table with all the requested columns
    features_table = Table(
        'advanced_features', metadata,
        Column('timestamp', DateTime, primary_key=True),
        Column('symbol', String, primary_key=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume_btc', Float),  # Volume BTC
        Column('volume_usd', Float),  # Volume USD
        # Existing Features
        Column('garman_klass_12h', Float),
        Column('price_range_pct', Float),
        Column('oc_change_pct', Float),
        Column('parkinson_3h', Float),
        Column('ma_3h', Float),
        Column('rolling_std_3h', Float),
        Column('lag_3h_price_return', Float),
        Column('lag_6h_price_return', Float),
        Column('lag_12h_price_return', Float),
        Column('lag_24h_price_return', Float),
        Column('lag_48h_price_return', Float),
        Column('lag_72h_price_return', Float),
        Column('lag_168h_price_return', Float),
        Column('volume_return_1h', Float),
        Column('lag_3h_volume_return', Float),
        Column('lag_6h_volume_return', Float),
        Column('lag_12h_volume_return', Float),
        Column('lag_24h_volume_return', Float),
        Column('ma_6h', Float),
        Column('ma_12h', Float),
        Column('ma_24h', Float),
        Column('ma_48h', Float),
        Column('ma_72h', Float),
        Column('ma_168h', Float),
        Column('rolling_std_6h', Float),
        Column('rolling_std_12h', Float),
        Column('rolling_std_24h', Float),
        Column('rolling_std_48h', Float),
        Column('rolling_std_72h', Float),
        Column('rolling_std_168h', Float),
        Column('atr_14h', Float),
        Column('atr_24h', Float),
        Column('atr_48h', Float),
        Column('close_div_ma_24h', Float),
        Column('close_div_ma_48h', Float),
        Column('close_div_ma_168h', Float),
        Column('ma12_div_ma48', Float),
        Column('ma24_div_ma168', Float),
        Column('std12_div_std72', Float),
        Column('volume_btc_x_range', Float),
        Column('rolling_std_3h_sq', Float),
        Column('price_return_1h_sq', Float),
        Column('rolling_std_12h_sqrt', Float),
        # Time-Based Features - Dummy Encoded
        # Hour of Day (0-23) - Dummy Encoded
        Column('hour_0', Integer),
        Column('hour_1', Integer),
        Column('hour_2', Integer),
        Column('hour_3', Integer),
        Column('hour_4', Integer),
        Column('hour_5', Integer),
        Column('hour_6', Integer),
        Column('hour_7', Integer),
        Column('hour_8', Integer),
        Column('hour_9', Integer),
        Column('hour_10', Integer),
        Column('hour_11', Integer),
        Column('hour_12', Integer),
        Column('hour_13', Integer),
        Column('hour_14', Integer),
        Column('hour_15', Integer),
        Column('hour_16', Integer),
        Column('hour_17', Integer),
        Column('hour_18', Integer),
        Column('hour_19', Integer),
        Column('hour_20', Integer),
        Column('hour_21', Integer),
        Column('hour_22', Integer),
        Column('hour_23', Integer),
        # Day of Week (0-6) - Dummy Encoded
        Column('day_0', Integer),  # Monday
        Column('day_1', Integer),  # Tuesday
        Column('day_2', Integer),  # Wednesday
        Column('day_3', Integer),  # Thursday
        Column('day_4', Integer),  # Friday
        Column('day_5', Integer),  # Saturday
        Column('day_6', Integer),  # Sunday
        # New Momentum Indicators
        Column('rsi_14h', Float),
        Column('macd', Float),
        Column('macd_signal', Float),
        Column('macd_hist', Float),
        # New Volume-Based Indicators
        Column('volume_ma_12h', Float),
        Column('volume_ma_24h', Float),
        Column('volume_ma_72h', Float),
        Column('volume_ma_168h', Float),
        Column('volume_div_ma_24h', Float),
        Column('obv', Float),
        Column('obv_ma_24h', Float),
        # New Volatility Ratios
        Column('atr_14_div_atr_48', Float),
        # New Normalized Price Position
        Column('stoch_k', Float),
        Column('stoch_d', Float),
        Column('z_score_24h', Float),
        # New Higher-Order Statistics
        Column('rolling_skew_24h', Float),
        Column('rolling_kurt_24h', Float),
        # Target column for prediction
        Column('target_1h_return', Float)
    )

    # Create table if it doesn't exist
    metadata.create_all(engine)

    return features_table

def get_ohlcv_data(engine, symbol, start_date=None, end_date=None, limit=None):
    """Retrieve OHLCV data for a symbol from the database"""
    query = f"""
    SELECT * FROM public.ohlcv
    WHERE symbol = '{symbol}'
    """

    if start_date:
        query += f" AND timestamp >= '{start_date}'"

    if end_date:
        query += f" AND timestamp <= '{end_date}'"

    query += " ORDER BY timestamp ASC"

    if limit:
        query += f" LIMIT {limit}"

    # Use direct connection to avoid SQLite dependency
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
    
    # Convert Decimal objects to float to avoid issues with numpy functions
    numeric_columns = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(float)

    return df

def calculate_advanced_features(df):
    """Calculate all the advanced features from OHLCV data"""
    # Reduced minimum length requirement to allow processing with limited data
    if df is None or len(df) < 3:  # Require at least 3 hours for basic calculations
        print("Not enough data to calculate any features (need at least 3 hours)")
        return None

    # Create a copy to avoid fragmentation
    df = df.copy()

    # Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Rename volume columns for clarity
    df['volume_btc'] = df['volumefrom']  # For BTC/USD, volumefrom is BTC
    df['volume_usd'] = df['volumeto']    # For BTC/USD, volumeto is USD

    # --- Time-Based Features ---
    # Extract hour and day of week for dummy encoding
    hour_of_day = df['timestamp'].dt.hour
    day_of_week = df['timestamp'].dt.dayofweek
    
    # Create dummy variables for hour of day (0-23)
    for hour in range(24):
        df[f'hour_{hour}'] = (hour_of_day == hour).astype(int)
    
    # Create dummy variables for day of week (0-6)
    for day in range(7):
        df[f'day_{day}'] = (day_of_week == day).astype(int)

    # --- Basic Price & Volume Changes ---
    # These can be calculated with minimal data
    df['price_return_1h'] = df['close'].pct_change()
    df['oc_change_pct'] = (df['close'] - df['open']) / df['open']
    df['price_range_pct'] = (df['high'] - df['low']) / df['low']
    df['volume_return_1h'] = df['volume_btc'].pct_change()

    # --- Moving Averages (Price) ---
    # Calculate only those that can be computed with available data
    available_hours = len(df)
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        if available_hours >= hours:
            df[f'ma_{hours}h'] = df['close'].rolling(window=hours).mean()
        else:
            df[f'ma_{hours}h'] = np.nan  # Set to NaN if not enough data

    # --- Standard Deviations (Price) ---
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        if available_hours >= hours:
            df[f'rolling_std_{hours}h'] = df['close'].rolling(window=hours).std()
        else:
            df[f'rolling_std_{hours}h'] = np.nan

    # --- Lagged Price Returns ---
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        if available_hours > hours:
            df[f'lag_{hours}h_price_return'] = df['close'].pct_change(periods=hours)
        else:
            df[f'lag_{hours}h_price_return'] = np.nan

    # --- Lagged Volume Returns ---
    for hours in [3, 6, 12, 24]:
        if available_hours > hours:
            df[f'lag_{hours}h_volume_return'] = df['volume_btc'].pct_change(periods=hours)
        else:
            df[f'lag_{hours}h_volume_return'] = np.nan

    # --- Volatility Measures ---
    # Garman-Klass volatility (12-hour)
    if available_hours >= 12:
        log_hl = np.log(df['high'] / df['low'])**2
        log_co = np.log(df['close'] / df['open'])**2
        df['garman_klass_12h'] = np.sqrt(0.5 * log_hl.rolling(12).sum() - (2*np.log(2)-1) * log_co.rolling(12).sum())
    else:
        df['garman_klass_12h'] = np.nan

    # Parkinson volatility (3-hour)
    if available_hours >= 3:
        df['parkinson_3h'] = np.sqrt((1 / (4 * np.log(2))) * ((np.log(df['high'] / df['low'])**2).rolling(3).sum()))
    else:
        df['parkinson_3h'] = np.nan

    # Average True Range (ATR)
    df['tr'] = np.maximum(
        df['high'] - df['low'],
        np.maximum(
            abs(df['high'] - df['close'].shift(1)),
            abs(df['low'] - df['close'].shift(1))
        )
    )
    for hours in [14, 24, 48]:
        if available_hours >= hours:
            df[f'atr_{hours}h'] = df['tr'].rolling(hours).mean()
        else:
            df[f'atr_{hours}h'] = np.nan

    # --- Momentum Indicators ---
    # RSI (14 periods)
    if available_hours >= 14:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi_14h'] = 100 - (100 / (1 + rs))
    else:
        df['rsi_14h'] = np.nan

    # MACD (12-period EMA, 26-period EMA, 9-period Signal EMA)
    if available_hours >= 26:
        ema_12 = df['close'].ewm(span=12, adjust=False).mean()
        ema_26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema_12 - ema_26
        if available_hours >= 35:  # 26 + 9
            df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
            df['macd_hist'] = df['macd'] - df['macd_signal']
        else:
            df['macd_signal'] = np.nan
            df['macd_hist'] = np.nan
    else:
        df['macd'] = np.nan
        df['macd_signal'] = np.nan
        df['macd_hist'] = np.nan

    # --- Volume-Based Indicators ---
    # Rolling Volume MA
    for hours in [12, 24, 72, 168]:
        if available_hours >= hours:
            df[f'volume_ma_{hours}h'] = df['volume_btc'].rolling(window=hours).mean()
        else:
            df[f'volume_ma_{hours}h'] = np.nan
    
    # Volume Ratio
    if available_hours >= 24:
        df['volume_div_ma_24h'] = df['volume_btc'] / df[f'volume_ma_24h']
    else:
        df['volume_div_ma_24h'] = np.nan

    # OBV (On-Balance Volume)
    obv = (np.sign(df['close'].diff()) * df['volume_btc']).fillna(0).cumsum()
    df['obv'] = obv
    # OBV MA
    if available_hours >= 24:
        df['obv_ma_24h'] = df['obv'].rolling(window=24).mean()
    else:
        df['obv_ma_24h'] = np.nan

    # --- Ratio Features ---
    # Only calculate ratios if both components are available
    if available_hours >= 24:
        df['close_div_ma_24h'] = df['close'] / df['ma_24h']
    else:
        df['close_div_ma_24h'] = np.nan
        
    if available_hours >= 48:
        df['close_div_ma_48h'] = df['close'] / df['ma_48h']
    else:
        df['close_div_ma_48h'] = np.nan
        
    if available_hours >= 168:
        df['close_div_ma_168h'] = df['close'] / df['ma_168h']
    else:
        df['close_div_ma_168h'] = np.nan

    if available_hours >= 48:
        df['ma12_div_ma48'] = df['ma_12h'] / df['ma_48h']
    else:
        df['ma12_div_ma48'] = np.nan
        
    if available_hours >= 168:
        df['ma24_div_ma168'] = df['ma_24h'] / df['ma_168h']
    else:
        df['ma24_div_ma168'] = np.nan
        
    if available_hours >= 72:
        df['std12_div_std72'] = df['rolling_std_12h'] / df['rolling_std_72h']
    else:
        df['std12_div_std72'] = np.nan

    # New Volatility Ratio
    if available_hours >= 48:
        df['atr_14_div_atr_48'] = df['atr_14h'] / df['atr_48h']
    else:
        df['atr_14_div_atr_48'] = np.nan

    # --- Normalized Price Position ---
    # Stochastic Oscillator (14 periods, 3 period smoothing for %D)
    if available_hours >= 14:
        low_14 = df['low'].rolling(window=14).min()
        high_14 = df['high'].rolling(window=14).max()
        df['stoch_k'] = 100 * (df['close'] - low_14) / (high_14 - low_14)
        if available_hours >= 17:  # 14 + 3
            df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()
        else:
            df['stoch_d'] = np.nan
    else:
        df['stoch_k'] = np.nan
        df['stoch_d'] = np.nan

    # Rolling Z-Score (24h)
    if available_hours >= 24:
        df['z_score_24h'] = (df['close'] - df['ma_24h']) / df['rolling_std_24h']
    else:
        df['z_score_24h'] = np.nan

    # --- Higher-Order Statistics (Rolling) ---
    if available_hours >= 24:
        df['rolling_skew_24h'] = df['price_return_1h'].rolling(window=24).skew()
        df['rolling_kurt_24h'] = df['price_return_1h'].rolling(window=24).kurt()
    else:
        df['rolling_skew_24h'] = np.nan
        df['rolling_kurt_24h'] = np.nan

    # --- Interaction Features ---
    df['volume_btc_x_range'] = df['volume_btc'] * df['price_range_pct']

    # --- Non-linear Transformations ---
    if available_hours >= 3:
        df['rolling_std_3h_sq'] = df['rolling_std_3h'] ** 2
    else:
        df['rolling_std_3h_sq'] = np.nan
        
    df['price_return_1h_sq'] = df['price_return_1h'] ** 2
    
    if available_hours >= 12:
        df['rolling_std_12h_sqrt'] = np.sqrt(np.abs(df['rolling_std_12h']))
    else:
        df['rolling_std_12h_sqrt'] = np.nan

    # --- Target ---
    # Target: 1-hour future return (shifted before dropping NaNs)
    # Use shift(-1) on close, then calculate pct_change to get future return relative to current close
    df['target_1h_return'] = df['close'].shift(-1).pct_change(1)

    # --- Cleanup and Selection ---
    # Replace infinite values that might arise from divisions
    df = df.replace([np.inf, -np.inf], np.nan)

    # Define the full list of columns to keep
    columns = [
        # Identifiers & Base Data
        'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume_btc', 'volume_usd',
        # Existing Features
        'garman_klass_12h', 'price_range_pct', 'oc_change_pct', 'parkinson_3h',
        'ma_3h', 'rolling_std_3h',
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
        'rolling_std_12h_sqrt',
        # Hour of Day (0-23) - Dummy Encoded
        'hour_0', 'hour_1', 'hour_2', 'hour_3', 'hour_4', 'hour_5', 
        'hour_6', 'hour_7', 'hour_8', 'hour_9', 'hour_10', 'hour_11',
        'hour_12', 'hour_13', 'hour_14', 'hour_15', 'hour_16', 'hour_17',
        'hour_18', 'hour_19', 'hour_20', 'hour_21', 'hour_22', 'hour_23',
        # Day of Week (0-6) - Dummy Encoded
        'day_0', 'day_1', 'day_2', 'day_3', 'day_4', 'day_5', 'day_6',
        # New Momentum Indicators
        'rsi_14h', 'macd', 'macd_signal', 'macd_hist',
        # New Volume-Based Indicators
        'volume_ma_12h', 'volume_ma_24h', 'volume_ma_72h', 'volume_ma_168h',
        'volume_div_ma_24h', 'obv', 'obv_ma_24h',
        # New Volatility Ratios
        'atr_14_div_atr_48',
        # New Normalized Price Position
        'stoch_k', 'stoch_d', 'z_score_24h',
        # New Higher-Order Statistics
        'rolling_skew_24h', 'rolling_kurt_24h',
        # Target
        'target_1h_return'
    ]

    # Select columns but don't drop NaN rows - we want to keep all data points
    # even if some features can't be calculated
    return df[columns]

def process_symbol_data(engine, symbol):
    """Process all data for a symbol and insert into advanced_features table"""
    print(f"Processing data for {symbol}...")

    # Get the latest timestamp in the advanced_features table for this symbol
    latest_timestamp_db = get_latest_timestamp(engine, symbol, 'advanced_features')

    # We'll process all available data since we have limited history
    start_date_fetch = None

    # Get total count of rows available in OHLCV
    conn = engine.connect()
    count_query = f"SELECT COUNT(*) FROM public.ohlcv WHERE symbol = '{symbol}'"
    if latest_timestamp_db:
        # Count only rows strictly newer than the last one we processed
        count_query += f" AND timestamp > '{latest_timestamp_db}'"

    result = conn.execute(text(count_query))
    total_new_rows_available = result.scalar()
    conn.close()

    print(f"Total new rows available to process: {total_new_rows_available}")

    if total_new_rows_available == 0 and latest_timestamp_db is not None:
        print(f"No new OHLCV data to process for {symbol} since {latest_timestamp_db}")
        return 0

    # Fetch all available data for this symbol
    batch_df = get_ohlcv_data(engine, symbol)

    if len(batch_df) == 0:
        print("No data fetched. Stopping.")
        return 0

    print(f"Processing batch of {len(batch_df)} raw rows")

    # Calculate features for the entire fetched batch
    features_df = calculate_advanced_features(batch_df)

    if features_df is None or len(features_df) == 0:
        print("No valid features generated from batch. Stopping.")
        return 0

    # Filter out features for timestamps we've already processed
    if latest_timestamp_db:
        features_to_insert = features_df[features_df['timestamp'] > latest_timestamp_db].copy()
    else:
        features_to_insert = features_df.copy()

    if len(features_to_insert) > 0:
        print(f"Attempting to insert {len(features_to_insert)} new feature rows...")
        inserted_count = insert_features_batch(engine, features_to_insert)
        print(f"Successfully processed/inserted {inserted_count} feature rows for {symbol}")
        return len(features_to_insert)
    else:
        print("No new features to insert after filtering already processed timestamps.")
        return 0

def get_latest_timestamp(engine, symbol, table_name):
    """Get the latest timestamp for a symbol from a table"""
    try:
        conn = engine.connect()
        query = text(f"SELECT MAX(timestamp) FROM {table_name} WHERE symbol = '{symbol}'")
        result = conn.execute(query)
        latest_timestamp = result.scalar()
        conn.close()
        # Ensure it's a timezone-naive datetime if not None
        if latest_timestamp and latest_timestamp.tzinfo:
             latest_timestamp = latest_timestamp.replace(tzinfo=None)
        return latest_timestamp
    except Exception as e:
        print(f"Error getting latest timestamp: {e}")
        return None

def insert_features_batch(engine, features_df):
    """Insert features into the database using raw SQL to avoid SQLite dependency"""
    if features_df is None or len(features_df) == 0:
        return 0

    # Replace NaN with None for SQL compatibility
    features_df = features_df.replace({np.nan: None, pd.NaT: None})
    # Ensure correct types for integer columns that might contain None
    int_cols = []
    # Add hour and day dummy columns to int_cols
    for hour in range(24):
        int_cols.append(f'hour_{hour}')
    for day in range(7):
        int_cols.append(f'day_{day}')
        
    for col in int_cols:
        if col in features_df.columns:
             features_df[col] = features_df[col].astype('object').where(features_df[col].notna(), None)


    # Get column names and placeholders for SQL query
    # Ensure column names are properly quoted for PostgreSQL
    columns = ', '.join([f'"{col}"' for col in features_df.columns])
    placeholders = ', '.join(['%s'] * len(features_df.columns))

    # Prepare data for insertion
    data = [tuple(row) for row in features_df.values]

    # Insert data using raw SQL
    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        # Create SQL query with ON CONFLICT clause
        # Ensure table and column names are quoted if they might be keywords or contain special chars
        query = f'INSERT INTO "advanced_features" ({columns}) VALUES ({placeholders}) ON CONFLICT (timestamp, symbol) DO NOTHING'

        # Execute in batches to avoid memory issues
        batch_size = 1000
        inserted = 0

        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(query, batch)
            # cursor.rowcount might not be reliable with ON CONFLICT DO NOTHING in all cases/drivers
            # For simplicity, we'll assume it works or accept potential inaccuracy if it doesn't.
            # A more robust way would be to count before/after or use RETURNING, but that adds complexity.
            inserted += len(batch) # Assume all attempted rows were either inserted or conflicted

        conn.commit()
        # A more accurate count would re-query the table, but let's return attempted count for now.
        # print(f"Attempted to insert {len(data)} rows.")
        return len(data) # Return number of rows processed in the batch
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")
        print(f"Failed query structure: {query[:500]}...") # Print start of query
        if data:
             print(f"Sample data row: {data[0]}") # Print first row of data
        return 0
    finally:
        cursor.close()
        conn.close()

def process_symbol_data(engine, symbol):
    """Process all data for a symbol and insert into advanced_features table"""
    print(f"Processing data for {symbol}...")

    # Get the latest timestamp in the advanced_features table for this symbol
    latest_timestamp_db = get_latest_timestamp(engine, symbol, 'advanced_features')

    required_overlap_hours = 175 # Match the minimum needed for feature calculation

    if latest_timestamp_db:
        print(f"Found existing data for {symbol} up to {latest_timestamp_db}")
        # Fetch data starting earlier to have enough history for rolling calculations
        start_date_fetch = latest_timestamp_db - timedelta(hours=required_overlap_hours)
    else:
        print(f"No existing data found for {symbol}. Processing all data.")
        start_date_fetch = None # Fetch all data from the beginning

    # Get total count of rows available in OHLCV *after* the latest processed timestamp
    # This count is just for progress estimation, not for fetching logic.
    conn = engine.connect()
    count_query = f"SELECT COUNT(*) FROM public.ohlcv WHERE symbol = '{symbol}'"
    if latest_timestamp_db:
        # Count only rows strictly newer than the last one we processed
        count_query += f" AND timestamp > '{latest_timestamp_db}'"

    result = conn.execute(text(count_query))
    total_new_rows_available = result.scalar()
    conn.close()

    print(f"Total new rows available to process: {total_new_rows_available}")

    if total_new_rows_available == 0 and latest_timestamp_db is not None:
        print(f"No new OHLCV data to process for {symbol} since {latest_timestamp_db}")
        return 0

    # --- Batch Processing Logic ---
    total_inserted_count = 0
    current_fetch_start = start_date_fetch
    last_processed_timestamp = latest_timestamp_db

    while True:
        print(f"Fetching batch starting around: {current_fetch_start}")
        # Fetch a chunk of data including the overlap needed
        # Limit fetch size to BATCH_SIZE + overlap to manage memory
        batch_df = get_ohlcv_data(
            engine,
            symbol,
            start_date=current_fetch_start,
            limit=BATCH_SIZE + required_overlap_hours
        )

        if len(batch_df) <= required_overlap_hours and current_fetch_start is not None:
             # If we only got back the overlap or less, likely no significantly new data
             print("Fetched batch size not large enough to contain new data beyond overlap. Stopping.")
             break

        if len(batch_df) == 0:
            print("No more data fetched. Stopping.")
            break

        print(f"Processing batch of {len(batch_df)} raw rows (including overlap)")

        # Calculate features for the entire fetched batch
        features_df = calculate_advanced_features(batch_df)

        if features_df is None or len(features_df) == 0:
            print("No valid features generated from batch. Stopping.")
            # This might happen if the batch wasn't long enough after all, or other data issues.
            break

        # Filter out features for timestamps we've already processed
        if last_processed_timestamp:
            features_to_insert = features_df[features_df['timestamp'] > last_processed_timestamp].copy()
        else:
            features_to_insert = features_df.copy() # Insert all if it's the first run

        if len(features_to_insert) > 0:
            print(f"Attempting to insert {len(features_to_insert)} new feature rows...")
            inserted_count = insert_features_batch(engine, features_to_insert)
            print(f"Successfully processed/inserted {inserted_count} feature rows for {symbol}")
            total_inserted_count += len(features_to_insert) # Count rows we attempted to insert

            # Update the last processed timestamp based on the *newly inserted* data
            last_processed_timestamp = features_to_insert['timestamp'].max()

            # Set the start for the next fetch slightly *before* the last timestamp of the *raw* batch
            # to ensure overlap, but avoid re-fetching the *entire* previous batch.
            # A safe overlap is the required_overlap_hours before the last raw timestamp.
            current_fetch_start = batch_df['timestamp'].max() - timedelta(hours=required_overlap_hours - 1)

        else:
            print("No new features to insert after filtering already processed timestamps. Stopping.")
            break

        # Safety break: If the last processed timestamp isn't advancing, stop.
        if last_processed_timestamp == latest_timestamp_db and latest_timestamp_db is not None:
             print("Warning: Last processed timestamp did not advance. Stopping to prevent infinite loop.")
             break
        latest_timestamp_db = last_processed_timestamp # Update for the next loop iteration's check


        # Optional: Add a condition to stop if total_inserted_count exceeds total_new_rows_available
        # This acts as a safeguard against potential infinite loops if counting/fetching logic has issues.
        # if total_inserted_count >= total_new_rows_available and latest_timestamp_db is not None:
        #    print(f"Processed approximately all new rows ({total_inserted_count}/{total_new_rows_available}). Stopping.")
        #    break


    print(f"Finished processing for {symbol}. Total new rows inserted/processed in this run: {total_inserted_count}")
    return total_inserted_count


def main():
    """Main function to create advanced features"""
    print(f"[{datetime.now()}] Starting advanced feature creation...")

    # Connect to database
    engine = create_engine(DB_CONNECTION)

    # Create advanced features table if it doesn't exist
    create_advanced_features_table(engine)

    # Process each symbol
    for symbol in SYMBOLS:
        try:
            rows_processed = process_symbol_data(engine, symbol)
            print(f"Completed processing for {symbol}. Rows processed in this run: {rows_processed}")
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            import traceback
            traceback.print_exc() # Print full traceback for debugging

    print(f"[{datetime.now()}] Advanced feature creation complete!")

if __name__ == "__main__":
    main()