"""Centralized configuration constants."""

from pathlib import Path

# Paths
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_FILE = CACHE_DIR / "scan_results.json"

# Scan settings
CACHE_TTL_HOURS = 6
DATA_LOOKBACK_DAYS = 365
BENCHMARK_LOOKBACK_DAYS = 60
MAX_WORKERS = 6
MAX_RAW_SCORE = 230
TOP_N_RESULTS = 750

# Batch scan settings
BATCH_SIZE = 80
BATCH_DELAY = 10  # seconds between batches

# ATR-based risk management
ATR_SL_MULTIPLIER = 2.0
TARGET_USES_RESISTANCE = True

# High Conviction thresholds (tighter in bear market)
HC_MIN_SCORE = 45
HC_MIN_SIGNALS_BULLISH = 5
HC_RSI_RANGE = (28, 70)
HC_DELIVERY_MIN = 45
HC_ATR_RANGE = (1.5, 5.5)
HC_RISK_MAX = 60
HC_REQUIRE_MACD_BULLISH = True
HC_REQUIRE_VOLUME = 1.0       # min volume ratio
HC_MIN_RISK_REWARD = 2.0

# Bear Play thresholds (oversold bounce in bear market)
BP_RSI_MAX = 40
BP_VOLUME_MIN = 1.2
BP_DELIVERY_MIN = 45
BP_WEEK1_MAX_LOSS = -2.0      # 1W return not worse than -2%
BP_MACD_BULLISH = True         # MACD must be bullish
BP_TARGET_PCT = 10.0           # realistic target in bear market
