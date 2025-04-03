import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import psycopg2
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bitfinex_collector.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("bitfinex_collector")

# Configuration
DB_PARAMS = {
    'dbname': 'ohlcv_db',
    'user': 'postgres',
    'password': 'bubZ$tep433',
    'host': 'localhost',
    'port': '5432'
}
SYMBOLS = {
    'BTC': 'tBTCUSD',
    'SOL': 'tSOLUSD'
}
COLLECTION_INTERVAL = 60  # Collect data every 60 seconds
RETRY_INTERVAL = 10  # Retry after 10 seconds if API call fails
MAX_RETRIES = 3  # Maximum number of retries for API calls

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    return psycopg2.connect(**DB_PARAMS)

def create_ohlcv_table():
    """Create OHLCV table if it doesn't exist"""
    query = """
    CREATE TABLE IF NOT EXISTS public.ohlcv (
        timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volumefrom NUMERIC,
        volumeto NUMERIC,
        PRIMARY KEY (timestamp, symbol)
    );
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table: {e}")
    finally:
        cursor.close()
        conn.close()

def get_latest_timestamp(symbol):
    """Get the latest timestamp for a symbol from the database"""
    query = """
    SELECT MAX(timestamp) FROM public.ohlcv WHERE symbol = %s
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()
    latest_timestamp = None
    
    try:
        cursor.execute(query, (symbol,))
        latest_timestamp = cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting latest timestamp: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return latest_timestamp

def fetch_bitfinex_candles(symbol_pair, timeframe='1m', limit=1000, start=None, end=None):
    """Fetch candle data from Bitfinex API
    
    timeframe: 1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 7D, 14D, 1M
    start/end: millisecond timestamps
    """
    base_url = f"https://api-pub.bitfinex.com/v2/candles/trade:{timeframe}:{symbol_pair}/hist"
    
    params = {
        'limit': limit,
        'sort': 1  # Sort by timestamp (oldest first)
    }
    
    if start:
        # Convert datetime to milliseconds timestamp
        params['start'] = int(start.timestamp() * 1000)
    
    if end:
        params['end'] = int(end.timestamp() * 1000)
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Bitfinex returns: [timestamp, open, close, high, low, volume]
            columns = ['timestamp', 'open', 'close', 'high', 'low', 'volume']
            df = pd.DataFrame(data, columns=columns)
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Reorder columns to match our schema
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            
            # Calculate volumefrom and volumeto
            # For simplicity, we'll use the average price to estimate
            df['average_price'] = (df['high'] + df['low']) / 2
            df['volumefrom'] = df['volume']  # Base asset volume
            df['volumeto'] = df['volume'] * df['average_price']  # Quote asset volume
            
            # Drop unnecessary columns
            df = df.drop(['average_price', 'volume'], axis=1)
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data from Bitfinex: {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Retrying in {RETRY_INTERVAL} seconds...")
                time.sleep(RETRY_INTERVAL)
            else:
                logger.error("Max retries reached. Giving up.")
                return None

def insert_data_to_db(df, symbol):
    """Insert data into the database using psycopg2"""
    if df is None or len(df) == 0:
        logger.warning(f"No data to insert for {symbol}")
        return 0
    
    # Add symbol column
    df['symbol'] = symbol
    
    # Prepare data for insertion
    data = []
    for _, row in df.iterrows():
        data.append((
            row['timestamp'],
            row['symbol'],
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row['volumefrom'],
            row['volumeto']
        ))
    
    # Insert query
    query = """
    INSERT INTO public.ohlcv (timestamp, symbol, open, high, low, close, volumefrom, volumeto)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (timestamp, symbol) DO NOTHING
    """
    
    conn = get_db_connection()
    cursor = conn.cursor()
    inserted = 0
    
    try:
        # Execute in batches
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            cursor.executemany(query, batch)
            inserted += cursor.rowcount
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return inserted

def collect_latest_data():
    """Collect the latest data for all symbols"""
    total_inserted = 0
    
    for symbol, bitfinex_symbol in SYMBOLS.items():
        try:
            logger.info(f"Collecting data for {symbol}...")
            
            # Get latest timestamp from database
            latest_timestamp = get_latest_timestamp(symbol)
            
            # Set up time range
            end_time = datetime.now()
            
            if latest_timestamp:
                # Add a small buffer to avoid duplicates
                start_time = latest_timestamp + timedelta(seconds=1)
            else:
                # No existing data, get the last day
                start_time = end_time - timedelta(days=1)
            
            # Fetch data from Bitfinex
            df = fetch_bitfinex_candles(
                bitfinex_symbol,
                timeframe='1m',
                limit=1000,  # Maximum allowed by Bitfinex
                start=start_time,
                end=end_time
            )
            
            if df is not None and len(df) > 0:
                # Insert into database
                inserted = insert_data_to_db(df, symbol)
                logger.info(f"Inserted {inserted} rows for {symbol}")
                total_inserted += inserted
            else:
                logger.info(f"No new data for {symbol}")
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
    
    return total_inserted

def main():
    """Main function to periodically collect data"""
    logger.info("Starting Bitfinex data collector...")
    
    # Create table if it doesn't exist
    create_ohlcv_table()
    
    while True:
        start_time = time.time()
        
        try:
            # Collect latest data
            inserted = collect_latest_data()
            logger.info(f"Collection cycle complete. Inserted {inserted} rows in total.")
        except Exception as e:
            logger.error(f"Error in collection cycle: {e}")
        
        # Calculate sleep time to maintain the collection interval
        elapsed = time.time() - start_time
        sleep_time = max(1, COLLECTION_INTERVAL - elapsed)
        
        logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()