"""Data-handling tests. Run:  python -m pytest -q"""
import json

import pandas as pd
import pytest
from pyspark.sql import functions as F

from src import config, features, ingest
from src.spark_utils import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests", shuffle_partitions=2, driver_memory="2g")
    yield s
    s.stop()


def test_traffic_load_has_no_nulls_or_duplicates(spark):
    clean = ingest.clean_traffic(ingest.load_traffic_raw(spark))
    assert clean.count() == 48120
    assert clean.filter(F.col("vehicles").isNull() | F.col("event_time").isNull()).count() == 0
    assert clean.count() == clean.dropDuplicates(["junction", "event_time"]).count()


def test_air_cleaning_handles_decimal_commas_and_sentinel(spark):
    air = ingest.clean_air(ingest.load_air_raw(spark))
    assert air.count() == 9357                                   # 114 blank lines removed
    assert "nmhc_gt" not in air.columns                          # 90% missing -> dropped
    assert air.filter(F.col("co_gt") == -200).count() == 0       # sentinel -> null
    first = air.orderBy("event_time").first()
    assert str(first["event_time"]) == "2004-03-10 18:00:00"
    assert first["co_gt"] == pytest.approx(2.6)                  # "2,6" parsed as 2.6
    assert first["abs_humidity"] == pytest.approx(0.7578)


def test_weather_json_is_flattened_to_rows(spark):
    df = ingest.load_weather(spark, config.WEATHER_SAMPLE_JSON)
    raw = json.loads(config.WEATHER_SAMPLE_JSON.read_text(encoding="utf-8"))
    assert df.count() == len(raw["hourly"]["time"])
    row = df.orderBy("event_time").first()
    assert row["temperature_2m"] == pytest.approx(raw["hourly"]["temperature_2m"][0])


def test_lags_are_matched_by_timestamp_not_row(spark):
    rows = [("2020-01-01 00:00:00", 1, 10), ("2020-01-01 01:00:00", 1, 20),
            ("2020-01-01 03:00:00", 1, 40)]  # 02:00 is missing
    df = spark.createDataFrame(rows, "t STRING, junction INT, vehicles INT") \
        .select(F.to_timestamp("t").alias("event_time"), "junction", "vehicles")
    out = {str(r["event_time"]): r for r in features.add_traffic_lags(df).collect()}
    assert out["2020-01-01 01:00:00"]["prev_hour_vehicles"] == 10
    assert out["2020-01-01 03:00:00"]["prev_hour_vehicles"] is None   # not 20 from the wrong hour
    assert out["2020-01-01 03:00:00"]["rolling_3h_mean"] == pytest.approx(15.0)  # 00:00 and 01:00


def test_replayer_features_match_training_features(spark):
    """Train/serve parity: features the replayer sends == features the model was trained on."""
    raw = pd.read_csv(config.TRAFFIC_TRAIN_CSV, parse_dates=["DateTime"]).rename(
        columns={"DateTime": "event_time", "Junction": "junction", "Vehicles": "vehicles"})
    live = features.traffic_context_pandas(raw[["event_time", "junction", "vehicles"]])
    live = live.rename(columns={"forecast_for": "target_time"}).drop(columns=["event_time", "vehicles"])

    batch = features.add_traffic_lags(ingest.clean_traffic(ingest.load_traffic_raw(spark))) \
        .select(F.col("event_time").alias("target_time"), "junction", *features.LAGS, "rolling_3h_mean") \
        .toPandas()
    merged = batch.merge(live, on=["target_time", "junction"], suffixes=("_batch", "_live"))
    assert len(merged) == 48120 - 4  # every hour except the last per junction (its target is in the future)
    for col in [*features.LAGS, "rolling_3h_mean"]:
        pd.testing.assert_series_equal(merged[f"{col}_batch"].astype(float), merged[f"{col}_live"].astype(float),
                                       check_names=False, rtol=1e-9)


def test_adaptive_threshold_uses_only_past_same_hour():
    idx = pd.date_range("2020-01-01", periods=24 * 30, freq="h")
    s = pd.Series(10.0, index=idx)
    s[s.index.hour == 8] = 50.0                 # 08:00 is always busy
    s[pd.Timestamp("2020-01-29 08:00")] = 1000  # a spike must not raise its own threshold
    thr = features.adaptive_alert_threshold(s)
    assert pd.isna(thr[pd.Timestamp("2020-01-05 08:00")])         # < 7 days of history
    assert thr[pd.Timestamp("2020-01-20 08:00")] == pytest.approx(50.0)
    assert thr[pd.Timestamp("2020-01-20 09:00")] == pytest.approx(10.0)
    assert thr[pd.Timestamp("2020-01-29 08:00")] == pytest.approx(50.0)  # excludes the hour itself
    assert pd.Timestamp("2020-01-31 00:00") in thr.index                # one hour past the data
