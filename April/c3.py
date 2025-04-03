import requests
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine, text, Table, Column, Integer, Float, String, DateTime, MetaData

def fetch_crypto_compare_ohlcv(symbol, currency='USD', limit=1, aggregate=1):
    """
    Fetch the most recent completed candle from CryptoCompare.
    Setting limit=1 returns one candle.
    """
    url = "https://min-api.cryptocompare.com/data/v2/histominute"
    params = {
        'fsym': symbol,
        'tsym': currency,
        'limit': limit,  # 1 candle
        'aggregate': aggregate
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error fetching data for {symbol}: {response.status_code} {response.text}")
        return None
    data = response.json()
    if data.get("Response") != "Success":
        print(f"Error in response for {symbol}: {data.get('Message')}")
        return None
    return data.get("Data", {}).get("Data", [])

def candles_to_dataframe(candles, symbol):
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles)
    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    df['symbol'] = symbol
    # Reorder columns: timestamp, symbol, open, high, low, close, volumefrom, volumeto
    df = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']]
    return df

def update_database(engine, df, table_name="ohlcv"):
    # Use direct SQL execution instead of pandas to_sql
    metadata = MetaData()
    
    # Define table structure
    ohlcv_table = Table(
        table_name, metadata,
        Column('timestamp', DateTime, primary_key=True),
        Column('symbol', String, primary_key=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volumefrom', Float),
        Column('volumeto', Float)
    )
    
    # Create table if it doesn't exist
    try:
        metadata.create_all(engine)
    except Exception as e:
        print(f"Error creating table: {e}")
    
    # Insert data row by row
    connection = engine.connect()
    try:
        for _, row in df.iterrows():
            insert_stmt = ohlcv_table.insert().values(
                timestamp=row['timestamp'],
                symbol=row['symbol'],
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                volumefrom=row['volumefrom'],
                volumeto=row['volumeto']
            )
            connection.execute(insert_stmt)
        connection.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        connection.close()

def main():
    # Setup SQLAlchemy engine; here we use SQLite for demonstration.
    # For PostgreSQL, use: 'postgresql+psycopg2://user:password@host/dbname'
    engine = create_engine('postgresql+psycopg2://postgres:bubZ$tep433@localhost:5432/ohlcv_db', echo=False)
    
    symbols = ['BTC', 'SOL']
    
    while True:
        for symbol in symbols:
            print(f"[{datetime.now()}] Fetching latest data for {symbol}...")
            candles = fetch_crypto_compare_ohlcv(symbol, limit=1)
            df = candles_to_dataframe(candles, symbol)
            if not df.empty:
                update_database(engine, df)
                print(f"Inserted new data for {symbol}:")
                print(df)
            else:
                print(f"No data retrieved for {symbol}.")
        print("Sleeping for 60 seconds...\n")
        time.sleep(60)

if __name__ == "__main__":
    main()
