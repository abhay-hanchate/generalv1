"""Step 2: load the three sources (CSV, CSV, JSON/API) with explicit schemas and clean them."""
import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

from src import config

# ---------------------------------------------------------------- traffic (CSV)
TRAFFIC_SCHEMA = StructType([
    StructField("DateTime", TimestampType(), True),
    StructField("Junction", IntegerType(), True),
    StructField("Vehicles", IntegerType(), True),
    StructField("ID", LongType(), True),
])


def load_traffic_raw(spark: SparkSession, path=config.TRAFFIC_TRAIN_CSV) -> DataFrame:
    # Explicit schema: no inferSchema pass over the file, and wrong types become nulls we can count.
    return (spark.read
            .option("header", True)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("mode", "PERMISSIVE")
            .schema(TRAFFIC_SCHEMA)
            .csv(config.p(path)))


def clean_traffic(raw: DataFrame) -> DataFrame:
    return (raw
            .select(F.col("DateTime").alias("event_time"),
                    F.col("Junction").alias("junction"),
                    F.col("Vehicles").alias("vehicles"),
                    F.col("ID").alias("record_id"))
            .dropna(subset=["event_time", "junction", "vehicles"])
            .filter(F.col("vehicles") >= 0)
            .dropDuplicates(["junction", "event_time"]))


def traffic_quality_report(raw: DataFrame, clean: DataFrame) -> dict:
    """Row counts, nulls, duplicates and hourly gaps per junction."""
    nulls = raw.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in raw.columns]).first().asDict()
    dupes = raw.count() - raw.dropDuplicates(["Junction", "DateTime"]).count()
    per_junction = (clean.groupBy("junction")
                    .agg(F.min("event_time").alias("first"), F.max("event_time").alias("last"),
                         F.count("*").alias("rows"))
                    .withColumn("expected_hours",
                                ((F.unix_timestamp("last") - F.unix_timestamp("first")) / 3600 + 1).cast("long"))
                    .withColumn("missing_hours", F.col("expected_hours") - F.col("rows"))
                    .orderBy("junction"))
    return {
        "raw_rows": raw.count(),
        "clean_rows": clean.count(),
        "nulls_per_column": nulls,
        "duplicate_junction_hours": dupes,
        "per_junction": [r.asDict() for r in per_junction.collect()],
    }


# ---------------------------------------------------------------- air quality (CSV)
# Original column -> clean name. NMHC(GT) is ~90% missing, so it is dropped.
AIR_COLUMNS = {
    "CO(GT)": "co_gt", "PT08.S1(CO)": "pt08_s1_co", "NMHC(GT)": "nmhc_gt", "C6H6(GT)": "c6h6_gt",
    "PT08.S2(NMHC)": "pt08_s2_nmhc", "NOx(GT)": "nox_gt", "PT08.S3(NOx)": "pt08_s3_nox",
    "NO2(GT)": "no2_gt", "PT08.S4(NO2)": "pt08_s4_no2", "PT08.S5(O3)": "pt08_s5_o3",
    "T": "temperature", "RH": "rel_humidity", "AH": "abs_humidity",
}
AIR_RAW_SCHEMA = StructType(
    [StructField("Date", StringType()), StructField("Time", StringType())]
    + [StructField(c, StringType()) for c in AIR_COLUMNS]
    + [StructField("_empty1", StringType()), StructField("_empty2", StringType())]  # trailing ';;'
)
AIR_MISSING_SENTINEL = -200.0


def load_air_raw(spark: SparkSession, path=config.AIR_CSV) -> DataFrame:
    # Read everything as text first: the file uses ';' separators AND ',' decimals,
    # which Spark's CSV reader cannot parse as numbers directly.
    # header=False + our own column names: the file's header has two empty names (trailing ';;')
    # that Spark would otherwise warn about; the header line itself is filtered out below.
    df = spark.read.option("header", False).option("sep", ";").schema(AIR_RAW_SCHEMA).csv(config.p(path))
    return df.filter(F.col("Date").isNull() | (F.col("Date") != "Date"))


def clean_air(raw: DataFrame) -> DataFrame:
    numeric = []
    for src, dst in AIR_COLUMNS.items():
        value = F.regexp_replace(F.col(f"`{src}`"), ",", ".").cast("double")
        numeric.append(F.when(value == AIR_MISSING_SENTINEL, None).otherwise(value).alias(dst))
    df = (raw
          .filter(F.col("Date").isNotNull() & (F.trim("Date") != ""))  # drop the blank trailing lines
          .select(F.to_timestamp(F.concat_ws(" ", "Date", "Time"), "dd/MM/yyyy HH.mm.ss").alias("event_time"),
                  *numeric)
          .dropna(subset=["event_time"])
          .dropDuplicates(["event_time"])
          .drop("nmhc_gt"))
    return df


def air_quality_report(raw: DataFrame, clean: DataFrame) -> dict:
    total = clean.count()
    missing = clean.select([F.round(F.avg(F.col(c).isNull().cast("int")) * 100, 1).alias(c)
                            for c in clean.columns]).first().asDict()
    return {
        "raw_lines": raw.count(),
        "clean_rows": total,
        "blank_lines_removed": raw.count() - total,
        "missing_percent_after_-200_to_null": missing,
        "first": str(clean.agg(F.min("event_time")).first()[0]),
        "last": str(clean.agg(F.max("event_time")).first()[0]),
    }


# ---------------------------------------------------------------- weather (API -> JSON)
WEATHER_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]


def fetch_weather_json(lat=config.CITY["lat"], lon=config.CITY["lon"], past_days=7, timeout=20):
    """Call Open-Meteo and save the raw JSON response to data/raw/weather/. Returns the file path."""
    import requests

    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={"latitude": lat, "longitude": lon, "hourly": ",".join(WEATHER_VARS),
                "past_days": past_days, "forecast_days": 1, "timezone": "UTC"},
        timeout=timeout,
    )
    resp.raise_for_status()
    config.WEATHER_DIR.mkdir(parents=True, exist_ok=True)
    path = config.WEATHER_DIR / f"open_meteo_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(resp.json()), encoding="utf-8")
    return path


def latest_weather_json():
    """Fetch fresh data; if offline, fall back to the newest saved response, then to the test sample."""
    try:
        return fetch_weather_json(), "live API"
    except Exception as exc:  # network errors, HTTP errors
        saved = sorted(config.WEATHER_DIR.glob("open_meteo_*.json"))
        if saved:
            return saved[-1], f"cached file (API failed: {type(exc).__name__})"
        return config.WEATHER_SAMPLE_JSON, f"offline sample (API failed: {type(exc).__name__})"


def load_weather(spark: SparkSession, path) -> DataFrame:
    """Open-Meteo returns column-oriented arrays: hourly.time[], hourly.temperature_2m[], ...
    arrays_zip + explode turns them into one row per hour."""
    raw = spark.read.option("multiLine", True).json(config.p(path))
    hourly = raw.select("latitude", "longitude", "hourly.*")
    rows = hourly.select("latitude", "longitude",
                         F.explode(F.arrays_zip("time", *WEATHER_VARS)).alias("r"))
    return rows.select(
        F.to_timestamp("r.time", "yyyy-MM-dd'T'HH:mm").alias("event_time"),
        "latitude", "longitude",
        *[F.col(f"r.{v}").cast("double").alias(v) for v in WEATHER_VARS],
    )
