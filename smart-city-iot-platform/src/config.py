"""Central configuration: every path and constant used by the project lives here."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# ---------- input data ----------
DATA_RAW = ROOT / "data" / "raw"
TRAFFIC_TRAIN_CSV = DATA_RAW / "traffic" / "train_aWnotuB.csv"
TRAFFIC_TEST_CSV = DATA_RAW / "traffic" / "test_BdBKkAj.csv"  # unlabeled (no Vehicles column)
AIR_CSV = DATA_RAW / "air_quality" / "AirQualityUCI.csv"
WEATHER_DIR = DATA_RAW / "weather"
WEATHER_SAMPLE_JSON = ROOT / "tests" / "fixtures" / "open_meteo_sample.json"

# ---------- streaming input (written by the replayer, read by Spark) ----------
STREAM_INPUT = ROOT / "data" / "stream_input"
STREAM_TRAFFIC_DIR = STREAM_INPUT / "traffic"
STREAM_AIR_DIR = STREAM_INPUT / "air"
STREAM_STAGING_DIR = STREAM_INPUT / "_staging"

# ---------- outputs (medallion layout) ----------
OUTPUT = ROOT / "output"
BRONZE = OUTPUT / "bronze"   # raw data stored as Parquet, untouched
SILVER = OUTPUT / "silver"   # cleaned, typed, de-duplicated
GOLD = OUTPUT / "gold"       # features, aggregates, predictions
MODELS = OUTPUT / "models"
ML_OUT = OUTPUT / "ml"
PERF = OUTPUT / "perf"
PERF_DATA = OUTPUT / "perf_data"
CHECKPOINTS = OUTPUT / "checkpoints"
STREAM_METRICS = OUTPUT / "stream_metrics"

TRAFFIC_MODEL_DIR = MODELS / "traffic_model"
AIR_MODEL_DIR = MODELS / "air_co_model"
THRESHOLDS_JSON = MODELS / "thresholds.json"

# ---------- time-based splits (never random for time series) ----------
TRAFFIC_VAL_START = "2017-02-01"   # train  < 2017-02-01 (junction 4 only starts in Jan 2017)
TRAFFIC_TEST_START = "2017-03-01"  # val    Feb 2017, test >= 2017-03-01
AIR_VAL_START = "2004-11-01"
AIR_TEST_START = "2005-01-01"

# ---------- alerting ----------
ALERT_PERCENTILE = 0.90  # alert when prediction exceeds the 90th percentile seen in training

# ---------- live weather API (Open-Meteo, free, no key) ----------
# The traffic dataset does not name its city, so the weather location is configurable.
CITY = {"name": "Bengaluru", "lat": 12.97, "lon": 77.59}

# ---------- streaming ----------
TRIGGER_INTERVAL = "5 seconds"
WATERMARK = "2 hours"   # in sensor time: hourly sensors, tolerate up to 2 late readings
WINDOW = "6 hours"


def p(path: Path) -> str:
    """Path string Spark accepts on Windows and Linux (forward slashes)."""
    return path.resolve().as_posix()
