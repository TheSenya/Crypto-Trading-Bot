import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from dotenv import load_dotenv
import time
from constants import RAW_CANDLE_DATA_DIR

load_dotenv()
api_key = os.getenv('BINANCE_API_KEY')

initialstart_date = '2017-08-17 00:00:00'

# https://api.binance.com
# https://api-gcp.binance.com
# https://api1.binance.com
# https://api2.binance.com
# https://api3.binance.com
# https://api4.binance.com

def get_historical_klines(symbol='BTCUSDT', interval='1h', start_date='2020-01-01 00:00:00'):
    """
    Fetch historical candlestick data from Binance API with pagination
    Args:
        symbol: Trading pair symbol
        interval: Candlestick interval
        start_date: Start date string in format 'YYYY-MM-DD HH:MM:SS'
    """
    # Convert interval to timedelta
    interval_map = {
        '1m': timedelta(minutes=1),
        '3m': timedelta(minutes=3),
        '5m': timedelta(minutes=5),
        '15m': timedelta(minutes=15),
        '30m': timedelta(minutes=30),
        '1h': timedelta(hours=1),
        '2h': timedelta(hours=2),
        '4h': timedelta(hours=4),
        '6h': timedelta(hours=6),
        '8h': timedelta(hours=8),
        '12h': timedelta(hours=12),
        '1d': timedelta(days=1),
        '3d': timedelta(days=3),
        '1w': timedelta(days=7),
    }
    
    interval_td = interval_map[interval]
    
    # Convert start_date to datetime
    current_start = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)
    
    all_klines = []
    
    while current_start < end_date:
        # Calculate end time for this batch (500 intervals from start)
        batch_end = current_start + (interval_td * 500)
        if batch_end > end_date:
            batch_end = end_date
            
        # Convert to millisecond timestamps
        start_ts = int(current_start.timestamp() * 1000)
        end_ts = int(batch_end.timestamp() * 1000)
        
        try:
            response = requests.get(
                f"https://api.binance.com/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "startTime": start_ts,
                    "endTime": end_ts,
                    "limit": 500
                }
            )
            
            if response.status_code == 200:
                batch_data = response.json()
                all_klines.extend(batch_data)
                print(f"Retrieved {len(batch_data)} candlesticks from {current_start} to {batch_end}")
                
                # Update start time for next batch
                current_start = batch_end
                
                # Add a small delay to avoid hitting rate limits
                time.sleep(0.1)
            else:
                print(f"Error fetching data: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error: {str(e)}")
            return None
    
    # Convert all collected data to DataFrame
    if all_klines:
        df = pd.DataFrame(all_klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                             'close_time', 'quote_asset_volume', 'number_of_trades',
                                             'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                             'ignore'])
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # Get actual date range from the data
        start_date_actual = df['timestamp'].min().strftime('%Y%m%d')
        end_date_actual = df['timestamp'].max().strftime('%Y%m%d')
        
        # Create filename with date range
        filename = f'{RAW_CANDLE_DATA_DIR}/{symbol}_{interval}_start({start_date_actual})_to_end({end_date_actual}).csv'
        
        # Create directory if it doesn't exist
        os.makedirs(RAW_CANDLE_DATA_DIR, exist_ok=True)
        
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        
        return df
    
    return None

if __name__ == "__main__":
    # ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1m']
    intervals = ['1M']

    for interval in intervals:
        get_historical_klines(symbol='BTCUSDT', interval=interval, start_date=initialstart_date)