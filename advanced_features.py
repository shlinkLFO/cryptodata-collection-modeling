import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, DateTime, MetaData, text
import time
from datetime import datetime, timedelta
import os
import warnings
import traceback # Import traceback for detailed error logging

# Suppress pandas warnings
warnings.filterwarnings('ignore')

# --- Configuration ---
DB_CONNECTION = 'postgresql+psycopg2://postgres:bubZ$tep433@localhost:5432/ohlcv_db'
SYMBOLS = ['BTC', 'SOL']
BATCH_SIZE = 10000  # Process this many rows of *new* data check at a time (adjust based on memory)
# How much historical data (in hours) needed *before* the new data point to calculate features
# Should be at least the longest window used (e.g., 168 hours) + a small buffer.
REQUIRED_OVERLAP_HOURS = 175 
LOOP_INTERVAL_SECONDS = 120 # 2 minutes

# --- Function Definitions (Keep create_advanced_features_table, get_ohlcv_data, calculate_advanced_features, get_latest_timestamp, insert_features_batch as they are) ---

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
    # Add timestamp filtering ONLY if start_date is provided
    if start_date:
        # Ensure start_date is timezone-naive for comparison if needed,
        # Assuming DB timestamps are timezone-naive or consistently handled.
        if hasattr(start_date, 'tzinfo') and start_date.tzinfo:
             start_date = start_date.replace(tzinfo=None)
        query += f" AND timestamp >= '{start_date}'"

    if end_date:
        if hasattr(end_date, 'tzinfo') and end_date.tzinfo:
             end_date = end_date.replace(tzinfo=None)
        query += f" AND timestamp <= '{end_date}'"

    query += " ORDER BY timestamp ASC"

    if limit:
        query += f" LIMIT {limit}"

    try:
        # Use context manager for connection
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result]

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            # Ensure timezone-naive if necessary for consistency
            if df['timestamp'].dt.tz:
                df['timestamp'] = df['timestamp'].dt.tz_localize(None)


        # Convert Decimal objects to float to avoid issues with numpy functions
        numeric_columns = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        return df

    except Exception as e:
        print(f"Error getting OHLCV data for {symbol}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error


def calculate_advanced_features(df):
    """Calculate all the advanced features from OHLCV data"""
    if df is None or len(df) < 3: # Minimal check
        # print("Not enough data points in the provided batch to calculate basic features.")
        return pd.DataFrame() # Return empty DF if not enough data

    # Create a copy to avoid fragmentation and SettingWithCopyWarning
    df = df.copy()

    # Ensure timestamp is datetime and index for easier rolling calculations
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').set_index('timestamp', drop=False) # Keep timestamp column

    # Rename volume columns for clarity
    df['volume_btc'] = df['volumefrom']  # Adjust if base/quote differs
    df['volume_usd'] = df['volumeto']

    # Determine max available history within this batch for calculation checks
    # This isn't perfect if batches don't overlap perfectly, but gives a lower bound.
    available_hours = len(df)

    # --- Time-Based Features ---
    hour_of_day = df.index.hour # Use DatetimeIndex
    day_of_week = df.index.dayofweek # Use DatetimeIndex

    for hour in range(24):
        df[f'hour_{hour}'] = (hour_of_day == hour).astype(int)
    for day in range(7):
        df[f'day_{day}'] = (day_of_week == day).astype(int)

    # --- Basic Price & Volume Changes ---
    df['price_return_1h'] = df['close'].pct_change()
    df['oc_change_pct'] = (df['close'] - df['open']) / df['open']
    df['price_range_pct'] = (df['high'] - df['low']) / df['low']
    df['volume_return_1h'] = df['volume_btc'].pct_change()

    # --- Moving Averages (Price) ---
    min_periods_base = 2 # Need at least 2 periods for std, etc.
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        df[f'ma_{hours}h'] = df['close'].rolling(window=hours, min_periods=min_periods_base).mean()

    # --- Standard Deviations (Price) ---
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        df[f'rolling_std_{hours}h'] = df['close'].rolling(window=hours, min_periods=min_periods_base).std()

    # --- Lagged Price Returns ---
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        df[f'lag_{hours}h_price_return'] = df['close'].pct_change(periods=hours)

    # --- Lagged Volume Returns ---
    for hours in [3, 6, 12, 24]:
        df[f'lag_{hours}h_volume_return'] = df['volume_btc'].pct_change(periods=hours)

    # --- Volatility Measures ---
    # Garman-Klass volatility (12-hour)
    log_hl = np.log(df['high'] / df['low'])**2
    log_co = np.log(df['close'] / df['open'])**2
    df['garman_klass_12h'] = np.sqrt(0.5 * log_hl.rolling(12, min_periods=12).sum() - (2*np.log(2)-1) * log_co.rolling(12, min_periods=12).sum())

    # Parkinson volatility (3-hour)
    df['parkinson_3h'] = np.sqrt((1 / (4 * np.log(2))) * ((np.log(df['high'] / df['low'])**2).rolling(3, min_periods=3).sum()))

    # Average True Range (ATR)
    df['tr'] = np.maximum(
        df['high'] - df['low'],
        np.maximum(
            abs(df['high'] - df['close'].shift(1)),
            abs(df['low'] - df['close'].shift(1))
        )
    )
    for hours in [14, 24, 48]:
        df[f'atr_{hours}h'] = df['tr'].rolling(hours, min_periods=hours).mean()

    # --- Momentum Indicators ---
    # RSI (14 periods)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=14).mean()
    rs = gain / loss
    df['rsi_14h'] = 100 - (100 / (1 + rs))
    # Handle potential division by zero if loss is zero for 14 periods
    df['rsi_14h'] = df['rsi_14h'].replace([np.inf, -np.inf], 100) # If loss is 0, RSI is 100
    df['rsi_14h'] = df['rsi_14h'].fillna(50) # Fill initial NaNs - debatable, could leave NaN


    # MACD (12-period EMA, 26-period EMA, 9-period Signal EMA)
    # Use min_periods=0 for EWM to start calculation immediately, adjust=False common practice
    ema_12 = df['close'].ewm(span=12, adjust=False, min_periods=12).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False, min_periods=26).mean()
    df['macd'] = ema_12 - ema_26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False, min_periods=9).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']

    # --- Volume-Based Indicators ---
    # Rolling Volume MA
    for hours in [12, 24, 72, 168]:
        df[f'volume_ma_{hours}h'] = df['volume_btc'].rolling(window=hours, min_periods=min_periods_base).mean()

    # Volume Ratio
    df[f'volume_div_ma_24h'] = df['volume_btc'] / df[f'volume_ma_24h']

    # OBV (On-Balance Volume)
    obv = (np.sign(df['close'].diff()) * df['volume_btc']).fillna(0).cumsum()
    df['obv'] = obv
    # OBV MA
    df['obv_ma_24h'] = df['obv'].rolling(window=24, min_periods=min_periods_base).mean()

    # --- Ratio Features ---
    df['close_div_ma_24h'] = df['close'] / df['ma_24h']
    df['close_div_ma_48h'] = df['close'] / df['ma_48h']
    df['close_div_ma_168h'] = df['close'] / df['ma_168h']
    df['ma12_div_ma48'] = df['ma_12h'] / df['ma_48h']
    df['ma24_div_ma168'] = df['ma_24h'] / df['ma_168h']
    df['std12_div_std72'] = df['rolling_std_12h'] / df['rolling_std_72h']
    df['atr_14_div_atr_48'] = df['atr_14h'] / df['atr_48h']

    # --- Normalized Price Position ---
    # Stochastic Oscillator (%K: 14 periods, %D: 3 period SMA of %K)
    low_14 = df['low'].rolling(window=14, min_periods=14).min()
    high_14 = df['high'].rolling(window=14, min_periods=14).max()
    df['stoch_k'] = 100 * (df['close'] - low_14) / (high_14 - low_14)
    df['stoch_d'] = df['stoch_k'].rolling(window=3, min_periods=3).mean()
    # Handle edge case where high == low
    df['stoch_k'] = df['stoch_k'].fillna(50) # Fill NaN where high_14 == low_14

    # Rolling Z-Score (24h)
    df['z_score_24h'] = (df['close'] - df['ma_24h']) / df['rolling_std_24h']

    # --- Higher-Order Statistics (Rolling) ---
    df['rolling_skew_24h'] = df['price_return_1h'].rolling(window=24, min_periods=24).skew()
    df['rolling_kurt_24h'] = df['price_return_1h'].rolling(window=24, min_periods=24).kurt()

    # --- Interaction Features ---
    df['volume_btc_x_range'] = df['volume_btc'] * df['price_range_pct']

    # --- Non-linear Transformations ---
    df['rolling_std_3h_sq'] = df['rolling_std_3h'] ** 2
    df['price_return_1h_sq'] = df['price_return_1h'] ** 2
    df['rolling_std_12h_sqrt'] = np.sqrt(df['rolling_std_12h'].abs()) # Use abs for safety

    # --- Target ---
    # Target: 1-hour future return. Shift needs careful handling at batch edges.
    # Calculate based on the *next* row's close relative to the *current* row's close
    # This might be inaccurate at the very end of a batch if the next row isn't available.
    # The model training script should handle this, perhaps by dropping the last row or re-calculating.
    df['target_1h_return'] = df['close'].shift(-1).pct_change(1) # Shift is relative to index here

    # --- Cleanup and Selection ---
    # Reset index before selection if needed, or keep it if preferred downstream
    df = df.reset_index(drop=True) # Drop the temporary timestamp index

    # Replace infinite values that might arise from divisions
    df = df.replace([np.inf, -np.inf], np.nan)

    # Define the full list of columns expected in the DB table
    # (Same list as in create_advanced_features_table)
    columns = [
        'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume_btc', 'volume_usd',
        'garman_klass_12h', 'price_range_pct', 'oc_change_pct', 'parkinson_3h',
        'ma_3h', 'rolling_std_3h', 'lag_3h_price_return', 'lag_6h_price_return',
        'lag_12h_price_return', 'lag_24h_price_return', 'lag_48h_price_return',
        'lag_72h_price_return', 'lag_168h_price_return', 'volume_return_1h',
        'lag_3h_volume_return', 'lag_6h_volume_return', 'lag_12h_volume_return',
        'lag_24h_volume_return', 'ma_6h', 'ma_12h', 'ma_24h', 'ma_48h', 'ma_72h',
        'ma_168h', 'rolling_std_6h', 'rolling_std_12h', 'rolling_std_24h',
        'rolling_std_48h', 'rolling_std_72h', 'rolling_std_168h', 'atr_14h',
        'atr_24h', 'atr_48h', 'close_div_ma_24h', 'close_div_ma_48h',
        'close_div_ma_168h', 'ma12_div_ma48', 'ma24_div_ma168', 'std12_div_std72',
        'volume_btc_x_range', 'rolling_std_3h_sq', 'price_return_1h_sq',
        'rolling_std_12h_sqrt',
        'hour_0', 'hour_1', 'hour_2', 'hour_3', 'hour_4', 'hour_5', 'hour_6',
        'hour_7', 'hour_8', 'hour_9', 'hour_10', 'hour_11', 'hour_12',
        'hour_13', 'hour_14', 'hour_15', 'hour_16', 'hour_17', 'hour_18',
        'hour_19', 'hour_20', 'hour_21', 'hour_22', 'hour_23',
        'day_0', 'day_1', 'day_2', 'day_3', 'day_4', 'day_5', 'day_6',
        'rsi_14h', 'macd', 'macd_signal', 'macd_hist', 'volume_ma_12h',
        'volume_ma_24h', 'volume_ma_72h', 'volume_ma_168h', 'volume_div_ma_24h',
        'obv', 'obv_ma_24h', 'atr_14_div_atr_48', 'stoch_k', 'stoch_d',
        'z_score_24h', 'rolling_skew_24h', 'rolling_kurt_24h', 'target_1h_return'
    ]

    # Ensure all columns exist, adding missing ones as NaN if necessary
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan

    # Return only the defined columns in the correct order
    # Keep NaNs - they indicate insufficient history for that specific feature/row
    return df[columns]


def get_latest_timestamp(engine, symbol, table_name):
    """Get the latest timestamp for a symbol from a table"""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT MAX(timestamp) FROM public.{table_name} WHERE symbol = :symbol")
            result = conn.execute(query, {'symbol': symbol})
            latest_timestamp = result.scalar()

        # Ensure it's a timezone-naive datetime if not None
        if latest_timestamp and latest_timestamp.tzinfo:
             latest_timestamp = latest_timestamp.replace(tzinfo=None)
        return latest_timestamp
    except Exception as e:
        print(f"Error getting latest timestamp for {symbol} from {table_name}: {e}")
        return None

def insert_features_batch(engine, features_df):
    """Insert features into the database using ON CONFLICT DO NOTHING"""
    if features_df is None or len(features_df) == 0:
        return 0

    table_name = 'advanced_features'
    # Replace pandas/numpy NaN/NaT with None for SQL compatibility
    features_df = features_df.replace({np.nan: None, pd.NaT: None})

    # Ensure correct types for integer columns that might contain None
    # Identify potential integer columns (dummies)
    int_cols = [f'hour_{h}' for h in range(24)] + [f'day_{d}' for d in range(7)]
    for col in int_cols:
        if col in features_df.columns:
             # Convert to object first to allow None, then handle potential floats if needed
             features_df[col] = features_df[col].astype('object')

    # Get column names and placeholders for SQL query
    # Quote column names for safety
    columns = ', '.join([f'"{col}"' for col in features_df.columns])
    placeholders = ', '.join(['%s'] * len(features_df.columns))

    # Prepare data for insertion (list of tuples)
    # Ensure timestamp is handled correctly (assuming it's already datetime objects)
    data_tuples = [tuple(row) for row in features_df.itertuples(index=False, name=None)]


    # Use connection pool and handle potential errors
    conn = None
    cursor = None
    inserted_count = 0 # Track actual insertions if possible/needed, otherwise return attempted count
    total_to_insert = len(data_tuples)

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Use psycopg2's execute_batch for potentially better performance
        # Note: executemany might be simpler and sufficient here. Using it for broader compatibility.
        query = f'INSERT INTO public."{table_name}" ({columns}) VALUES ({placeholders}) ON CONFLICT (timestamp, symbol) DO NOTHING'

        # Execute in batches if the dataframe is very large (though BATCH_SIZE might handle this earlier)
        db_batch_size = 5000 # How many rows per single INSERT statement (adjust based on DB/driver)
        for i in range(0, total_to_insert, db_batch_size):
            batch_data = data_tuples[i:min(i + db_batch_size, total_to_insert)]
            if batch_data: # Ensure batch is not empty
                 cursor.executemany(query, batch_data)
                 # cursor.rowcount with ON CONFLICT DO NOTHING might be unreliable
                 # For simplicity, we report the number attempted in this batch
                 inserted_count += len(batch_data)


        conn.commit()
        # Return number of rows we attempted to insert
        return total_to_insert # Or use a more complex method if exact inserted count is critical
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error inserting data batch into {table_name}: {e}")
        print(f"Failed query structure (first 500 chars): {query[:500]}...")
        if data_tuples:
             print(f"Sample data row (first 10 elements): {str(data_tuples[0][:10])}")
        traceback.print_exc() # Print full traceback
        return 0 # Indicate failure / no insertion
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def process_symbol_data_incremental(engine, symbol, required_overlap_hours):
    """
    Processes new data for a symbol incrementally.
    Fetches data including overlap, calculates features, and inserts only new rows.
    """
    print(f"[{datetime.now()}] Processing symbol: {symbol}")

    # 1. Find the latest timestamp already processed for this symbol
    latest_processed_ts = get_latest_timestamp(engine, symbol, 'advanced_features')

    # 2. Determine the start time for fetching raw data
    # Fetch data starting 'required_overlap_hours' before the latest processed timestamp
    # to ensure enough history for rolling calculations on new data points.
    if latest_processed_ts:
        fetch_start_ts = latest_processed_ts - timedelta(hours=required_overlap_hours)
        print(f"Latest feature timestamp found: {latest_processed_ts}. Fetching OHLCV data from {fetch_start_ts}")
    else:
        fetch_start_ts = None # Fetch all data from the beginning
        print(f"No existing features found for {symbol}. Fetching all OHLCV data.")

    # 3. Fetch raw OHLCV data (including overlap)
    # No need for BATCH_SIZE here, fetch all potentially relevant data at once.
    # Let calculate_advanced_features handle the full dataframe.
    # Limit might be useful if historical data is enormous, but let's assume manageable for now.
    raw_df = get_ohlcv_data(engine, symbol, start_date=fetch_start_ts)

    if raw_df.empty:
        if latest_processed_ts:
             print(f"No new OHLCV data found for {symbol} since {fetch_start_ts}.")
        else:
             print(f"No OHLCV data found at all for {symbol}.")
        return 0 # No data to process

    print(f"Fetched {len(raw_df)} raw OHLCV rows (including overlap if applicable).")

    # Ensure data is sorted by timestamp before calculating features
    raw_df = raw_df.sort_values('timestamp').reset_index(drop=True)

    # Check if the fetched data actually contains timestamps newer than the last processed one
    if latest_processed_ts and not raw_df.empty:
        if raw_df['timestamp'].max() <= latest_processed_ts:
            print(f"No OHLCV data strictly newer than {latest_processed_ts} found. Nothing to process.")
            return 0


    # 4. Calculate features for the entire fetched dataframe
    # This ensures rolling calculations are correct across the overlap boundary.
    print(f"Calculating features for {len(raw_df)} rows...")
    features_df = calculate_advanced_features(raw_df)

    if features_df.empty:
        print("Feature calculation resulted in an empty DataFrame. Skipping insertion.")
        return 0

    # 5. Filter features to insert only *new* rows
    # Select rows with timestamps strictly greater than the latest one already in the DB.
    if latest_processed_ts:
        features_to_insert = features_df[features_df['timestamp'] > latest_processed_ts].copy()
    else:
        features_to_insert = features_df.copy() # Insert all if it's the first run

    num_to_insert = len(features_to_insert)
    if num_to_insert > 0:
        print(f"Calculated {len(features_df)} feature rows. Found {num_to_insert} new rows to insert.")

        # Add the symbol column back if it was dropped during calculation (it shouldn't be now)
        if 'symbol' not in features_to_insert.columns:
             features_to_insert['symbol'] = symbol

        # 6. Insert the new feature rows into the database
        inserted_count = insert_features_batch(engine, features_to_insert)
        print(f"Attempted to insert {num_to_insert} rows. Insert function reported processing {inserted_count} rows.")
        # The actual number inserted might differ if ON CONFLICT skipped some rows unexpectedly,
        # but reporting attempted count is usually sufficient here.
        return num_to_insert # Return the number of new rows identified
    else:
        print(f"Calculated {len(features_df)} feature rows, but no timestamps were newer than {latest_processed_ts}. No new rows to insert.")
        return 0


def main():
    """Main function to run the feature calculation loop"""
    print(f"[{datetime.now()}] Starting advanced feature creation service...")
    print(f"Checking for new data every {LOOP_INTERVAL_SECONDS} seconds.")
    print(f"Symbols: {SYMBOLS}")
    print(f"Database: {DB_CONNECTION.split('@')[1] if '@' in DB_CONNECTION else DB_CONNECTION}") # Avoid printing password

    try:
        # Connect to database (do this once outside the loop)
        engine = create_engine(DB_CONNECTION, pool_pre_ping=True) # pool_pre_ping helps with long-running connections

        # Create advanced features table if it doesn't exist (do this once)
        print("Ensuring 'advanced_features' table exists...")
        create_advanced_features_table(engine)
        print("Table check complete.")

    except Exception as e:
        print(f"[{datetime.now()}] CRITICAL ERROR: Failed to connect to database or ensure table exists. Exiting.")
        print(f"Error: {e}")
        traceback.print_exc()
        return # Exit if initial setup fails

    # --- Main Loop ---
    while True:
        start_time = datetime.now()
        print(f"\n[{start_time}] Starting processing cycle...")
        total_rows_processed_cycle = 0

        for symbol in SYMBOLS:
            try:
                # Process data for the symbol incrementally
                rows_processed = process_symbol_data_incremental(engine, symbol, REQUIRED_OVERLAP_HOURS)
                total_rows_processed_cycle += rows_processed if rows_processed else 0
                print(f"[{datetime.now()}] Completed processing for {symbol}. New rows processed: {rows_processed if rows_processed else 0}")

            except Exception as e:
                # Log error but continue the loop for other symbols/next cycle
                print(f"[{datetime.now()}] ERROR processing symbol {symbol}!")
                print(f"Error: {e}")
                traceback.print_exc() # Print detailed traceback for debugging
                # Optional: add a small delay after an error before processing next symbol
                # time.sleep(5)

        end_time = datetime.now()
        cycle_duration = (end_time - start_time).total_seconds()
        print(f"\n[{end_time}] Processing cycle finished.")
        print(f"Total new rows processed across all symbols in this cycle: {total_rows_processed_cycle}")
        print(f"Cycle duration: {cycle_duration:.2f} seconds.")

        # Wait before starting the next cycle
        wait_time = max(0, LOOP_INTERVAL_SECONDS - cycle_duration)
        if wait_time > 0:
            print(f"Waiting for {wait_time:.2f} seconds before next cycle...")
            time.sleep(wait_time)
        else:
            print("Processing took longer than interval, starting next cycle immediately.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[{datetime.now()}] Shutdown signal received (Ctrl+C). Exiting gracefully.")
    except Exception as e:
        # Catch any unexpected error in the main loop itself or setup
        print(f"\n[{datetime.now()}] An unexpected critical error occurred in the main loop:")
        print(f"Error: {e}")
        traceback.print_exc()