import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, DateTime, MetaData, text
import time
from datetime import datetime, timedelta
import os
import warnings
import traceback
import pandas_ta as ta  # Technical indicators

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Suppress pandas warnings
warnings.filterwarnings('ignore')

# --- Configuration ---
DB_CONNECTION = os.getenv('DB_CONNECTION', 'postgresql+psycopg2://postgres:bubZ$tep433@localhost:5432/ohlcv_db')
SYMBOLS = ['BTC', 'SOL']
BATCH_SIZE = 10000
REQUIRED_OVERLAP_HOURS = 175
LOOP_INTERVAL_SECONDS = 120

# --- Function Definitions ---

def create_advanced_features_table(engine):
    """Create the advanced features table if it doesn't exist"""
    metadata = MetaData()

    features_table = Table(
        'advanced_features', metadata,
        Column('timestamp', DateTime, primary_key=True),
        Column('symbol', String, primary_key=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume_btc', Float),
        Column('volume_usd', Float),
        # Advanced Features
        Column('garman_klass_12h', Float),
        Column('price_range_pct', Float),
        Column('oc_change_pct', Float),
        Column('parkinson_3h', Float),
        # Moving Averages and Rolling Std
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
        # Interaction Features and Non-linear Transformations
        Column('volume_btc_x_range', Float),
        Column('rolling_std_3h_sq', Float),
        Column('price_return_1h_sq', Float),
        Column('rolling_std_12h_sqrt', Float),
        # Time-Based Features - Dummy Encoded
        Column('hour_0', Integer), Column('hour_1', Integer), Column('hour_2', Integer),
        Column('hour_3', Integer), Column('hour_4', Integer), Column('hour_5', Integer),
        Column('hour_6', Integer), Column('hour_7', Integer), Column('hour_8', Integer),
        Column('hour_9', Integer), Column('hour_10', Integer), Column('hour_11', Integer),
        Column('hour_12', Integer), Column('hour_13', Integer), Column('hour_14', Integer),
        Column('hour_15', Integer), Column('hour_16', Integer), Column('hour_17', Integer),
        Column('hour_18', Integer), Column('hour_19', Integer), Column('hour_20', Integer),
        Column('hour_21', Integer), Column('hour_22', Integer), Column('hour_23', Integer),
        Column('day_0', Integer), Column('day_1', Integer), Column('day_2', Integer),
        Column('day_3', Integer), Column('day_4', Integer), Column('day_5', Integer),
        Column('day_6', Integer),
        # Momentum Indicators
        Column('rsi_14h', Float),
        Column('macd', Float),
        Column('macd_signal', Float),
        Column('macd_hist', Float),
        # Volume-Based Indicators
        Column('volume_ma_12h', Float),
        Column('volume_ma_24h', Float),
        Column('volume_ma_72h', Float),
        Column('volume_ma_168h', Float),
        Column('volume_div_ma_24h', Float),
        Column('obv', Float),
        Column('obv_ma_24h', Float),
        # Volatility Ratios
        Column('atr_14_div_atr_48', Float),
        # Normalized Price Position
        Column('stoch_k', Float),
        Column('stoch_d', Float),
        Column('z_score_24h', Float),
        # Higher-Order Statistics
        Column('rolling_skew_24h', Float),
        Column('rolling_kurt_24h', Float),
        # Advanced Features
        Column('adx_14h', Float),
        Column('plus_di_14h', Float),
        Column('minus_di_14h', Float),
        Column('ad_line', Float),
        Column('cmf_20h', Float),
        Column('cci_20h', Float),
        Column('bband_width_20h', Float),
        Column('bband_pctb_20h', Float),
        Column('close_pos_in_range', Float),
        # Additional Features
        Column('vwap_24h', Float),
        Column('log_return_1h', Float),
        Column('stoch_rsi_k', Float),
        Column('stoch_rsi_d', Float)
    )
    metadata.create_all(engine)
    return features_table

def get_ohlcv_data(engine, symbol, start_date=None, end_date=None, limit=None):
    """Retrieve OHLCV data for a symbol from the database"""
    query = f"""
    SELECT * FROM public.ohlcv
    WHERE symbol = '{symbol}'
    """
    if start_date:
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
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result]
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if df['timestamp'].dt.tz:
                df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        numeric_columns = ['open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df
    except Exception as e:
        logging.error(f"Error getting OHLCV data for {symbol}: {e}")
        return pd.DataFrame()

def calculate_advanced_features(df):
    """Calculate all the advanced features from OHLCV data"""
    if df is None or len(df) < 3:
        return pd.DataFrame()

    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Sort by timestamp and set index (while keeping the timestamp column)
    df = df.sort_values('timestamp').set_index('timestamp', drop=False)

    # Rename volume columns
    df['volume_btc'] = df['volumefrom']
    df['volume_usd'] = df['volumeto']

    # Time-Based Features
    hour_of_day = df.index.hour
    day_of_week = df.index.dayofweek
    for hour in range(24):
        df[f'hour_{hour}'] = (hour_of_day == hour).astype(int)
    for day in range(7):
        df[f'day_{day}'] = (day_of_week == day).astype(int)

    # Basic Price & Volume Changes
    df['price_return_1h'] = df['close'].pct_change()
    df['oc_change_pct'] = (df['close'] - df['open']) / df['open']
    df['price_range_pct'] = (df['high'] - df['low']) / df['low']
    df['volume_return_1h'] = df['volume_btc'].pct_change()

    # Moving Averages and Rolling Std (min_periods set to 2 or full window as needed)
    min_periods_base = 2
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        df[f'ma_{hours}h'] = df['close'].rolling(window=hours, min_periods=min_periods_base).mean()
        df[f'rolling_std_{hours}h'] = df['close'].rolling(window=hours, min_periods=min_periods_base).std()

    # Lagged Price Returns
    for hours in [3, 6, 12, 24, 48, 72, 168]:
        df[f'lag_{hours}h_price_return'] = df['close'].pct_change(periods=hours)
    # Lagged Volume Returns
    for hours in [3, 6, 12, 24]:
        df[f'lag_{hours}h_volume_return'] = df['volume_btc'].pct_change(periods=hours)

    # Volatility Measures
    log_hl = np.log(df['high'] / df['low'])**2
    log_co = np.log(df['close'] / df['open'])**2
    df['garman_klass_12h'] = np.sqrt(0.5 * log_hl.rolling(12, min_periods=12).sum() - (2*np.log(2)-1) * log_co.rolling(12, min_periods=12).sum())
    df['parkinson_3h'] = np.sqrt((1 / (4 * np.log(2))) * (np.log(df['high'] / df['low'])**2).rolling(3, min_periods=3).sum())
    df['tr'] = np.maximum(df['high'] - df['low'],
                          np.maximum(abs(df['high'] - df['close'].shift(1)), abs(df['low'] - df['close'].shift(1))))
    for hours in [14, 24, 48]:
        df[f'atr_{hours}h'] = df['tr'].rolling(hours, min_periods=hours).mean()

    # Momentum Indicators
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=14).mean()
    rs = gain / loss
    df['rsi_14h'] = (100 - (100 / (1 + rs))).replace([np.inf, -np.inf], 100).fillna(50)
    ema_12 = df['close'].ewm(span=12, adjust=False, min_periods=12).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False, min_periods=26).mean()
    df['macd'] = ema_12 - ema_26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False, min_periods=9).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']

    # Volume-Based Indicators
    for hours in [12, 24, 72, 168]:
        df[f'volume_ma_{hours}h'] = df['volume_btc'].rolling(window=hours, min_periods=min_periods_base).mean()
    df['volume_div_ma_24h'] = df['volume_btc'] / df['volume_ma_24h']
    obv = (np.sign(df['close'].diff()) * df['volume_btc']).fillna(0).cumsum()
    df['obv'] = obv
    df['obv_ma_24h'] = df['obv'].rolling(window=24, min_periods=min_periods_base).mean()

    # Ratio Features
    df['close_div_ma_24h'] = df['close'] / df['ma_24h']
    df['close_div_ma_48h'] = df['close'] / df['ma_48h']
    df['close_div_ma_168h'] = df['close'] / df['ma_168h']
    df['ma12_div_ma48'] = df['ma_12h'] / df['ma_48h']
    df['ma24_div_ma168'] = df['ma_24h'] / df['ma_168h']
    df['std12_div_std72'] = df['rolling_std_12h'] / df['rolling_std_72h']
    df['atr_14_div_atr_48'] = df['atr_14h'] / df['atr_48h']

    # Normalized Price Position
    low_14 = df['low'].rolling(window=14, min_periods=14).min()
    high_14 = df['high'].rolling(window=14, min_periods=14).max()
    df['stoch_k'] = (100 * (df['close'] - low_14) / (high_14 - low_14)).fillna(50)
    df['stoch_d'] = df['stoch_k'].rolling(window=3, min_periods=3).mean()
    df['z_score_24h'] = (df['close'] - df['ma_24h']) / df['rolling_std_24h']

    # Higher-Order Statistics
    df['rolling_skew_24h'] = df['price_return_1h'].rolling(window=24, min_periods=24).skew()
    df['rolling_kurt_24h'] = df['price_return_1h'].rolling(window=24, min_periods=24).kurt()

    # Interaction Features and Non-linear Transformations
    df['volume_btc_x_range'] = df['volume_btc'] * df['price_range_pct']
    df['rolling_std_3h_sq'] = df['rolling_std_3h'] ** 2
    df['price_return_1h_sq'] = df['price_return_1h'] ** 2
    df['rolling_std_12h_sqrt'] = np.sqrt(df['rolling_std_12h'].abs())

    # --- New Advanced Features using pandas_ta ---
    ta_df = df.copy()
    ta_df.rename(columns={'volume_btc': 'volume'}, inplace=True)

    # ADX/DMI
    adx_df = ta_df.ta.adx(length=14)
    df['adx_14h'] = adx_df['ADX_14']
    df['plus_di_14h'] = adx_df['DMP_14']
    df['minus_di_14h'] = adx_df['DMN_14']

    # Accumulation/Distribution Line
    df['ad_line'] = ta_df.ta.ad()
    # Chaikin Money Flow
    df['cmf_20h'] = ta_df.ta.cmf(length=20)
    # Commodity Channel Index
    df['cci_20h'] = ta_df.ta.cci(length=20)
    # Bollinger Bands
    bbands_df = ta_df.ta.bbands(length=20, std=2)
    df['bband_width_20h'] = bbands_df['BBB_20_2.0']
    df['bband_pctb_20h'] = bbands_df['BBP_20_2.0']
    # Position in Range
    range_hl = df['high'] - df['low']
    df['close_pos_in_range'] = ((df['close'] - df['low']) / range_hl).fillna(0.5)
    df['close_pos_in_range'] = df['close_pos_in_range'].replace([np.inf, -np.inf], 0.5)

    # --- Additional New Features ---
    # Log Returns
    df['log_return_1h'] = np.log(df['close'] / df['close'].shift(1))
    # VWAP (24h) - ensure proper volume sum and handle potential division by zero
    df['vwap_24h'] = (df['close'] * df['volume_btc']).rolling(window=24, min_periods=24).sum() / \
                     df['volume_btc'].rolling(window=24, min_periods=24).sum()
    # Stochastic RSI using pandas_ta
    stochrsi_df = ta_df.ta.stochrsi(length=14)
    # Fix: Check the actual column names in stochrsi_df and use them
    if not stochrsi_df.empty:
        # Get the actual column names that contain 'k' and 'd'
        k_col = [col for col in stochrsi_df.columns if 'k' in col.lower()]
        d_col = [col for col in stochrsi_df.columns if 'd' in col.lower() and 'k' not in col.lower()]
        
        if k_col:
            df['stoch_rsi_k'] = stochrsi_df[k_col[0]]
        else:
            df['stoch_rsi_k'] = np.nan
            
        if d_col:
            df['stoch_rsi_d'] = stochrsi_df[d_col[0]]
        else:
            df['stoch_rsi_d'] = np.nan
    else:
        df['stoch_rsi_k'] = np.nan
        df['stoch_rsi_d'] = np.nan

    # Cleanup: reset index and replace infinities
    df = df.reset_index(drop=True)
    df = df.replace([np.inf, -np.inf], np.nan)

    # Define the list of columns to keep (including the additional features but excluding target)
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
        'z_score_24h', 'rolling_skew_24h', 'rolling_kurt_24h',
        'adx_14h', 'plus_di_14h', 'minus_di_14h', 'ad_line', 'cmf_20h', 'cci_20h',
        'bband_width_20h', 'bband_pctb_20h', 'close_pos_in_range',
        'vwap_24h', 'log_return_1h', 'stoch_rsi_k', 'stoch_rsi_d'
    ]

    # Ensure all columns exist; add missing ones as NaN if needed
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan

    return df[columns]

def get_latest_timestamp(engine, symbol, table_name):
    """Get the latest timestamp for a symbol from a table"""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT MAX(timestamp) FROM public.{table_name} WHERE symbol = :symbol")
            result = conn.execute(query, {'symbol': symbol})
            latest_timestamp = result.scalar()
        if latest_timestamp and latest_timestamp.tzinfo:
            latest_timestamp = latest_timestamp.replace(tzinfo=None)
        return latest_timestamp
    except Exception as e:
        logging.error(f"Error getting latest timestamp for {symbol} from {table_name}: {e}")
        return None

def insert_features_batch(engine, features_df):
    """Insert features into the database using ON CONFLICT DO NOTHING"""
    if features_df is None or len(features_df) == 0:
        return 0

    table_name = 'advanced_features'
    features_df = features_df.replace({np.nan: None, pd.NaT: None})

    # Ensure integer columns are handled as object (to allow None)
    int_cols = [f'hour_{h}' for h in range(24)] + [f'day_{d}' for d in range(7)]
    for col in int_cols:
        if col in features_df.columns:
            features_df[col] = features_df[col].astype('object')

    columns = ', '.join([f'"{col}"' for col in features_df.columns])
    placeholders = ', '.join(['%s'] * len(features_df.columns))
    data_tuples = [tuple(row) for row in features_df.itertuples(index=False, name=None)]

    conn = None
    cursor = None
    total_to_insert = len(data_tuples)

    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        query = f'INSERT INTO public."{table_name}" ({columns}) VALUES ({placeholders}) ON CONFLICT (timestamp, symbol) DO NOTHING'
        db_batch_size = 5000
        for i in range(0, total_to_insert, db_batch_size):
            batch_data = data_tuples[i:min(i + db_batch_size, total_to_insert)]
            if batch_data:
                cursor.executemany(query, batch_data)
        conn.commit()
        return total_to_insert
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error inserting data batch into {table_name}: {e}")
        logging.error(f"Failed query structure (first 500 chars): {query[:500]}...")
        if data_tuples:
            logging.error(f"Sample data row (first 10 elements): {str(data_tuples[0][:10])}")
        traceback.print_exc()
        return 0
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_symbol_data_incremental(engine, symbol, required_overlap_hours):
    """Processes new data for a symbol incrementally"""
    logging.info(f"Processing symbol: {symbol}")
    latest_processed_ts = get_latest_timestamp(engine, symbol, 'advanced_features')
    if latest_processed_ts:
        fetch_start_ts = latest_processed_ts - timedelta(hours=required_overlap_hours)
        logging.info(f"Latest feature timestamp found: {latest_processed_ts}. Fetching OHLCV data from {fetch_start_ts}")
    else:
        fetch_start_ts = None
        logging.info(f"No existing features found for {symbol}. Fetching all OHLCV data.")

    raw_df = get_ohlcv_data(engine, symbol, start_date=fetch_start_ts)

    if raw_df.empty:
        logging.info(f"No OHLCV data found for {symbol} in the required range.")
        return 0
    logging.info(f"Fetched {len(raw_df)} raw OHLCV rows.")
    raw_df = raw_df.sort_values('timestamp').reset_index(drop=True)

    if latest_processed_ts and not raw_df.empty and raw_df['timestamp'].max() <= latest_processed_ts:
        logging.info(f"No OHLCV data strictly newer than {latest_processed_ts} found.")
        return 0

    logging.info(f"Calculating features for {len(raw_df)} rows...")
    features_df = calculate_advanced_features(raw_df)

    if features_df.empty:
        logging.info("Feature calculation resulted in empty DataFrame.")
        return 0

    if latest_processed_ts:
        features_to_insert = features_df[features_df['timestamp'] > latest_processed_ts].copy()
    else:
        features_to_insert = features_df.copy()

    num_to_insert = len(features_to_insert)
    if num_to_insert > 0:
        logging.info(f"Calculated {len(features_df)} feature rows. Found {num_to_insert} new rows to insert.")
        if 'symbol' not in features_to_insert.columns:
            features_to_insert['symbol'] = symbol
        inserted_count = insert_features_batch(engine, features_to_insert)
        logging.info(f"Attempted to insert {num_to_insert} rows. Insert function reported processing {inserted_count} rows.")
        return num_to_insert
    else:
        logging.info(f"Calculated {len(features_df)} feature rows, but no timestamps were newer than {latest_processed_ts}.")
        return 0

def main():
    """Main function to run the feature calculation loop"""
    logging.info("Starting advanced feature creation service...")
    logging.info(f"Checking for new data every {LOOP_INTERVAL_SECONDS} seconds.")
    logging.info(f"Symbols: {SYMBOLS}")
    db_info = DB_CONNECTION.split('@')[1] if '@' in DB_CONNECTION else DB_CONNECTION
    logging.info(f"Database: {db_info}")

    try:
        engine = create_engine(DB_CONNECTION, pool_pre_ping=True)
        logging.info("Ensuring 'advanced_features' table exists...")
        create_advanced_features_table(engine)
        logging.info("Table check complete.")
    except Exception as e:
        logging.critical("CRITICAL ERROR: Failed to connect/ensure table exists. Exiting.")
        logging.critical(f"Error: {e}")
        traceback.print_exc()
        return

    while True:
        cycle_start = datetime.now()
        logging.info("Starting processing cycle...")
        total_rows_processed_cycle = 0
        for symbol in SYMBOLS:
            try:
                rows_processed = process_symbol_data_incremental(engine, symbol, REQUIRED_OVERLAP_HOURS)
                total_rows_processed_cycle += rows_processed if rows_processed else 0
                logging.info(f"Completed processing for {symbol}. New rows processed: {rows_processed if rows_processed else 0}")
            except Exception as e:
                logging.error(f"ERROR processing symbol {symbol}: {e}")
                traceback.print_exc()
        cycle_end = datetime.now()
        cycle_duration = (cycle_end - cycle_start).total_seconds()
        logging.info("Processing cycle finished.")
        logging.info(f"Total new rows processed in this cycle: {total_rows_processed_cycle}")
        logging.info(f"Cycle duration: {cycle_duration:.2f} seconds.")
        wait_time = max(0, LOOP_INTERVAL_SECONDS - cycle_duration)
        if wait_time > 0:
            logging.info(f"Waiting for {wait_time:.2f} seconds before next cycle...")
            time.sleep(wait_time)
        else:
            logging.info("Processing took longer than the interval, starting next cycle immediately.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Shutdown signal received (Ctrl+C). Exiting gracefully.")
    except Exception as e:
        logging.error("An unexpected critical error occurred:")
        logging.error(f"Error: {e}")
        traceback.print_exc()
