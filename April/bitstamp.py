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
        logging.FileHandler("bitstamp_collector.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("bitstamp_collector")

# Configuration
DB_PARAMS = {
    'dbname': 'ohlcv_db',
    'user': 'postgres',
    'password': 'bubZ$tep433',
    'host': 'localhost',
    'port': '5432'
}
SYMBOLS = {
    'BTC': 'btcusd',
    'SOL': 'solusd'
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

def fetch_bitstamp_ohlc(pair, step=60, limit=1000):
    """Fetch OHLC data from Bitstamp API
    
    pair: Trading pair (e.g., btcusd)
    step: Time step in seconds (60 = 1 minute)
    limit: Number of data points (max 1000)
    """
    url = f"https://www.bitstamp.net/api/v2/ohlc/{pair}/"
    
    params = {
        'step': step,
        'limit': limit
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'data' not in data or 'ohlc' not in data['data']:
                logger.error(f"Unexpected Bitstamp response format: {data}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Retrying in {RETRY_INTERVAL} seconds...")
                    time.sleep(RETRY_INTERVAL)
                    continue
                else:
                    return None
            
            # Extract the OHLC data
            ohlc_data = data['data']['ohlc']
            
            # Create DataFrame
            df = pd.DataFrame(ohlc_data)
            
            # Convert timestamp to datetime - FIXED LINE
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='s')
            
            # Convert string values to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            
            # Rename volume to volumefrom
            df = df.rename(columns={'volume': 'volumefrom'})
            
            # Calculate volumeto (USD volume) as volume * close price
            df['volumeto'] = df['volumefrom'] * df['close']
            
            # Keep only the columns we need
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']]
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data from Bitstamp: {e}")
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
    ON CONFLICT (timestamp, symbol) DO UPDATE 
    SET open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volumefrom = EXCLUDED.volumefrom,
        volumeto = EXCLUDED.volumeto
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
    
    for symbol, bitstamp_pair in SYMBOLS.items():
        try:
            logger.info(f"Collecting data for {symbol}...")
            
            # Get latest timestamp from database
            latest_timestamp = get_latest_timestamp(symbol)
            
            # Fetch data from Bitstamp
            df = fetch_bitstamp_ohlc(
                pair=bitstamp_pair,
                step=60,  # 1-minute candles
                limit=1000  # Maximum allowed
            )
            
            if df is not None and len(df) > 0:
                # If we have existing data, only keep new rows
                if latest_timestamp:
                    df = df[df['timestamp'] > latest_timestamp]
                
                if len(df) > 0:
                    # Insert into database
                    inserted = insert_data_to_db(df, symbol)
                    logger.info(f"Inserted {inserted} rows for {symbol}")
                    total_inserted += inserted
                else:
                    logger.info(f"No new data for {symbol}")
            else:
                logger.warning(f"Failed to fetch data for {symbol}")
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
    
    return total_inserted

def main():
    """Main function to periodically collect data"""
    logger.info("Starting Bitstamp data collector...")
    
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