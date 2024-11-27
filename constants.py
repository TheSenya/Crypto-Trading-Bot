from pathlib import Path

# Base directory (project root)
BASE_DIR = Path(__file__).parent.parent

# Data directories
RAW_CANDLE_DATA_DIR = BASE_DIR / "Crypto" / "data" / "raw_candle"
INDICATOR_CANDLE_DATA_DIR = BASE_DIR / "Crypto" / "data" / "indicator_candle"
