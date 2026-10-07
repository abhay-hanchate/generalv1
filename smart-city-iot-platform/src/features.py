"""Feature engineering shared by batch training (Spark) and the sensor replayer (pandas).

The model forecasts vehicles at hour T using only what is known by hour T-1:
    prev_hour_vehicles   = vehicles(T-1h)
    same_hour_yesterday  = vehicles(T-24h)
    same_hour_last_week  = vehicles(T-168h)
    rolling_3h_mean      = mean(vehicles(T-3h), vehicles(T-2h), vehicles(T-1h))
Lags are matched on the *timestamp*, not the row position, so a missing hour gives a null
instead of silently using the wrong reading. tests/test_features.py checks both versions agree.
"""
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LAGS = {"prev_hour_vehicles": 1, "same_hour_yesterday": 24, "same_hour_last_week": 168}
TIME_FEATURES = ["hour", "day_of_week", "month", "is_weekend"]
TRAFFIC_NUMERIC = ["prev_hour_vehicles", "same_hour_yesterday", "same_hour_last_week", "rolling_3h_mean", "month"]
TRAFFIC_CATEGORICAL = ["junction", "hour", "day_of_week", "is_weekend"]
TRAFFIC_LABEL = "vehicles"
# Traffic grows over time and tree models cannot predict above the range they were trained on,
# so the model learns the *change* from the last hour and we add the last hour back afterwards.
TRAFFIC_FIT_LABEL = "vehicles_delta"

AIR_NUMERIC = ["pt08_s1_co", "pt08_s2_nmhc", "pt08_s3_nox", "pt08_s4_no2", "pt08_s5_o3",
               "temperature", "rel_humidity", "abs_humidity"]
AIR_CATEGORICAL = ["hour"]
AIR_LABEL = "co_gt"


def add_time_features(df: DataFrame, ts_col: str = "event_time") -> DataFrame:
    dow = F.dayofweek(ts_col)  # 1 = Sunday ... 7 = Saturday
    return (df.withColumn("hour", F.hour(ts_col))
              .withColumn("day_of_week", dow)
              .withColumn("month", F.month(ts_col))
              .withColumn("is_weekend", dow.isin(1, 7).cast("int")))


def add_traffic_lags(df: DataFrame) -> DataFrame:
    """Spark version (batch). Needs columns junction, event_time, vehicles."""
    w = Window.partitionBy("junction").orderBy("event_time")
    for name, hours in LAGS.items():
        lag_value = F.lag("vehicles", hours).over(w)
        lag_time = F.lag("event_time", hours).over(w)
        expected = F.col("event_time") - F.expr(f"INTERVAL {hours} HOURS")
        df = df.withColumn(name, F.when(lag_time == expected, lag_value).cast("double"))
    # time-based window: the 3 hours strictly before this row
    by_seconds = (Window.partitionBy("junction").orderBy(F.col("event_time").cast("long"))
                  .rangeBetween(-3 * 3600, -1))
    return df.withColumn("rolling_3h_mean", F.avg("vehicles").over(by_seconds))


def build_traffic_features(clean_traffic: DataFrame) -> DataFrame:
    df = add_time_features(add_traffic_lags(clean_traffic))
    return df.withColumn(TRAFFIC_FIT_LABEL, F.col("vehicles") - F.col("prev_hour_vehicles"))


def to_vehicle_forecast(scored: DataFrame) -> DataFrame:
    """Turn the model's predicted change into a vehicle count (used in training AND streaming)."""
    return scored.withColumn("prediction",
                             F.greatest(F.lit(0.0), F.col("prev_hour_vehicles") + F.col("prediction")))


def adaptive_alert_threshold(s: pd.Series, days=28, q=0.90, min_days=7) -> pd.Series:
    """'Unusually busy' level for each hour: the q-quantile of the same hour-of-day over the previous
    `days` days (the hour itself excluded). Adapts as traffic grows, unlike one fixed number.
    Returned on an hourly index that extends one hour past the data, so the next hour has a value."""
    full = s.reindex(pd.date_range(s.index.min(), s.index.max() + pd.Timedelta(hours=1), freq="h"))
    by_hour = full.groupby(full.index.hour, group_keys=False)
    return by_hour.apply(lambda g: g.shift(1).rolling(f"{days}D", min_periods=min_days).quantile(q)).sort_index()


def traffic_context_pandas(traffic: pd.DataFrame) -> pd.DataFrame:
    """pandas version used by the replayer (the 'edge gateway' that knows recent history).

    For each reading at time t it returns the features of the *next* hour T = t+1h, so the
    streaming job can forecast T as soon as reading t arrives. Input columns: event_time, junction, vehicles.
    """
    out = []
    for junction, g in traffic.sort_values("event_time").groupby("junction"):
        s = g.set_index("event_time")["vehicles"].astype(float)
        target = s.index + pd.Timedelta(hours=1)
        frame = pd.DataFrame({
            "event_time": s.index,
            "junction": junction,
            "vehicles": s.values.astype(int),
            "forecast_for": target,
        })
        for name, hours in LAGS.items():
            # look the value up by timestamp T - lag; a missing hour gives NaN, not a wrong row
            frame[name] = s.reindex(target - pd.Timedelta(hours=hours)).values
        # mean of the 3 hours before T: (T-3h, T-2h, T-1h) = (t-2h, t-1h, t)
        frame["rolling_3h_mean"] = s.rolling("3h").mean().reindex(s.index).values
        frame["alert_threshold"] = adaptive_alert_threshold(s).reindex(target).values
        out.append(frame)
    return pd.concat(out, ignore_index=True)
