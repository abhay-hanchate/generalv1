"""Steps 2-4 as one runnable job: raw files -> bronze -> silver -> gold.

Run:  python -m src.batch_pipeline
"""
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config, features, ingest
from src.spark_utils import get_spark

ZONES = ["Central", "North", "South", "East", "West"]
ROAD_TYPES = ["Arterial", "Collector", "Highway", "Local"]


def junction_dim(spark: SparkSession, n_junctions: int) -> DataFrame:
    """Small reference table (city master data) used to show a broadcast join.
    The traffic dataset only has junction ids, so these attributes are generated deterministically."""
    ids = spark.range(1, n_junctions + 1).withColumnRenamed("id", "junction")
    return (ids
            .withColumn("junction", F.col("junction").cast("int"))
            .withColumn("zone", F.element_at(F.array(*[F.lit(z) for z in ZONES]), (F.col("junction") % 5) + 1))
            .withColumn("road_type", F.element_at(F.array(*[F.lit(r) for r in ROAD_TYPES]),
                                                  (F.col("junction") % 4) + 1))
            .withColumn("capacity_per_hour", (F.col("junction") % 7 + 3) * 20))


def write_parquet(df: DataFrame, path, partition_by=None, mode="overwrite"):
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(config.p(path))


def run(spark: SparkSession, verbose=True) -> dict:
    # ---- load (Step 2)
    traffic_raw = ingest.load_traffic_raw(spark)
    air_raw = ingest.load_air_raw(spark)
    weather_path, weather_source = ingest.latest_weather_json()
    weather = ingest.load_weather(spark, weather_path)

    # ---- bronze: raw data as-is, in a columnar format
    write_parquet(traffic_raw, config.BRONZE / "traffic")
    write_parquet(air_raw, config.BRONZE / "air_quality")
    write_parquet(weather, config.BRONZE / "weather")

    # ---- silver: cleaned + typed (Step 3/4 transformations)
    traffic = ingest.clean_traffic(traffic_raw)
    air = ingest.clean_air(air_raw)
    write_parquet(traffic.withColumn("year", F.year("event_time")), config.SILVER / "traffic",
                  partition_by=["junction"])
    write_parquet(air, config.SILVER / "air_quality")

    # ---- gold: model-ready features + hourly city profile
    traffic_features = features.build_traffic_features(traffic)
    write_parquet(traffic_features, config.GOLD / "traffic_features")
    air_features = features.add_time_features(air)
    write_parquet(air_features, config.GOLD / "air_features")

    dim = junction_dim(spark, 4)
    profile = (traffic_features.join(F.broadcast(dim), "junction")
               .groupBy("zone", "junction", "hour", "is_weekend")
               .agg(F.round(F.avg("vehicles"), 2).alias("avg_vehicles"),
                    F.max("vehicles").alias("max_vehicles"),
                    F.count("*").alias("hours_observed")))
    write_parquet(profile, config.GOLD / "junction_hourly_profile")

    report = {
        "traffic": ingest.traffic_quality_report(traffic_raw, traffic),
        "air_quality": ingest.air_quality_report(air_raw, air),
        "weather": {"source": weather_source, "file": weather_path.name, "rows": weather.count()},
    }
    config.OUTPUT.mkdir(parents=True, exist_ok=True)
    (config.OUTPUT / "data_quality_report.json").write_text(json.dumps(report, indent=2, default=str))
    if verbose:
        print(json.dumps(report, indent=2, default=str))
    return report


if __name__ == "__main__":
    session = get_spark("SmartCity-Batch")
    run(session)
    session.stop()
