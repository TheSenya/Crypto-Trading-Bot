import pandas as pd
import ta
import os
import argparse
from constants import INDICATOR_CANDLE_DATA_DIR, RAW_CANDLE_DATA_DIR

def add_trend_indicators(df):
    """
    Add trend indicators: SMA, EMA, MACD
    """
    df['sma_20'] = ta.trend.sma_indicator(df['close'], window=20)
    df['sma_50'] = ta.trend.sma_indicator(df['close'], window=50)
    df['sma_200'] = ta.trend.sma_indicator(df['close'], window=200)
    df['ema_20'] = ta.trend.ema_indicator(df['close'], window=20)
    
    macd = ta.trend.MACD(df['close'])
    df['macd_line'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    df['macd_histogram'] = macd.macd_diff()
    
    return df

def add_momentum_indicators(df):
    """
    Add momentum indicators: RSI, Stochastic
    """
    df['rsi'] = ta.momentum.rsi(df['close'], window=14)
    
    stoch = ta.momentum.StochasticOscillator(df['high'], df['low'], df['close'])
    df['stoch_k'] = stoch.stoch()
    df['stoch_d'] = stoch.stoch_signal()
    
    return df

def add_volume_indicators(df):
    """
    Add volume indicators: OBV, ADI
    """
    df['obv'] = ta.volume.on_balance_volume(df['close'], df['volume'])
    df['adi'] = ta.volume.acc_dist_index(df['high'], df['low'], df['close'], df['volume'])
    
    return df

def add_volatility_indicators(df):
    """
    Add volatility indicators: Bollinger Bands, ATR
    """
    bollinger = ta.volatility.BollingerBands(df['close'])
    df['bb_high'] = bollinger.bollinger_hband()
    df['bb_mid'] = bollinger.bollinger_mavg()
    df['bb_low'] = bollinger.bollinger_lband()
    
    df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'])
    
    return df

def prepare_dataframe(df):
    """
    Prepare dataframe by converting price columns to float
    """
    # Convert price columns to float
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
    
    # Ensure timestamp is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df

def process_file(filename):
    """
    Process a single CSV file from the training_data directory
    Args:
        filename: Name of the CSV file to process (e.g., 'BTCUSDT_1h_20200101_to_20240315.csv')
    """
    
    input_path = os.path.join(RAW_CANDLE_DATA_DIR, filename)
    print(f"Input path: {input_path}")
    
    if not os.path.exists(input_path):
        print(f"Error: File {input_path} not found")
        return
    
    print(f"\nProcessing {filename}...")
    
    # Read and prepare the data
    df = pd.read_csv(input_path)
    df = prepare_dataframe(df)
    
    # Add indicators
    print("Adding trend indicators...")
    df = add_trend_indicators(df)
    
    print("Adding momentum indicators...")
    df = add_momentum_indicators(df)
    
    print("Adding volume indicators...")
    df = add_volume_indicators(df)
    
    print("Adding volatility indicators...")
    df = add_volatility_indicators(df)
    
    # Save to new directory
    new_filename = filename.replace('.csv', '_indicators.csv')
    output_path = os.path.join(INDICATOR_CANDLE_DATA_DIR, new_filename)
    df.to_csv(output_path, index=False)
    
    print(f"Saved to {output_path}")
    print(f"Data shape: {df.shape}")
    print("Added indicators:", [col for col in df.columns if col not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a single CSV file from the training_data directory")
    parser.add_argument("filename", type=str, help="Name of the CSV file to process (e.g., 'BTCUSDT_1h_20200101_to_20240315.csv')")
    args = parser.parse_args()
    process_file(args.filename) 