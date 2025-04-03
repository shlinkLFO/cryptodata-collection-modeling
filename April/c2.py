import requests
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine

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
    # Append new data to the table; creates the table if it doesn't exist
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

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
