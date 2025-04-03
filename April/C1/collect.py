import requests
import pandas as pd
from datetime import datetime

def fetch_crypto_compare_ohlcv(symbol, currency='USD', limit=120, aggregate=1):
    """
    Fetch historical 1-minute OHLCV data from CryptoCompare.
    - symbol: e.g. 'BTC' or 'SOL'
    - currency: e.g. 'USD'
    - limit: number of candles (limit+1 data points are returned)
    - aggregate: interval multiplier
    """
    url = "https://min-api.cryptocompare.com/data/v2/histominute"
    params = {
        'fsym': symbol,
        'tsym': currency,
        'limit': limit,
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

def candles_to_dataframe(candles):
    if not candles:
        return pd.DataFrame()
    # Create a DataFrame from the list of candle dictionaries
    df = pd.DataFrame(candles)
    # Convert the 'time' field (in seconds) to a datetime object
    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    # Keep only the columns we care about:
    # timestamp, open, high, low, close, volumefrom, volumeto
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']]
    return df

def main():
    symbols = ['BTC', 'SOL']
    for symbol in symbols:
        print(f"Fetching {symbol} data...")
        candles = fetch_crypto_compare_ohlcv(symbol)
        df = candles_to_dataframe(candles)
        if df.empty:
            print(f"No data retrieved for {symbol}")
        else:
            csv_filename = f"{symbol.lower()}_ohlcv_cryptocompare.csv"
            df.to_csv(csv_filename, index=False)
            print(f"{symbol} data saved to {csv_filename}")
            print(df.tail())

if __name__ == "__main__":
    main()
