"""Make a larger copy of the traffic data for the performance experiments.

48k rows is too small: every optimization would finish in under a second and show nothing.
Each of the 4 junctions is copied `factor` times as new junctions (1-4, 11-14, 21-24, ...), with
+/-10% random noise on the counts, so ×100 gives ~4.8M rows from 400 junctions.
The copy is written as CSV and as Parquet (partitioned by year) for the format comparison.

Run:  python -m src.scale_data --factor 100
"""
import argparse

from pyspark.sql import functions as F

from src import config, ingest
from src.spark_utils import get_spark

SCALED_CSV = config.PERF_DATA / "traffic_scaled_csv"
SCALED_PARQUET = config.PERF_DATA / "traffic_scaled_parquet"


def build_scaled(spark, factor):
    base = ingest.clean_traffic(ingest.load_traffic_raw(spark))
    copies = spark.range(factor).withColumnRenamed("id", "copy")
    noise = 0.9 + 0.2 * F.rand(seed=7)
    return (base.crossJoin(copies)
            .withColumn("junction", (F.col("junction") + F.col("copy") * 10).cast("int"))
            .withColumn("vehicles", F.greatest(F.lit(1), F.round(F.col("vehicles") * noise)).cast("int"))
            .withColumn("year", F.year("event_time"))
            .drop("copy", "record_id"))


def run(spark, factor=100):
    scaled = build_scaled(spark, factor).repartition(16).cache()
    rows = scaled.count()
    scaled.write.mode("overwrite").option("header", True).csv(config.p(SCALED_CSV))
    scaled.write.mode("overwrite").partitionBy("year").parquet(config.p(SCALED_PARQUET))
    scaled.unpersist()
    print(f"scaled copy: {rows:,} rows, {factor * 4} junctions -> {SCALED_CSV.name}, {SCALED_PARQUET.name}")
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--factor", type=int, default=100)
    session = get_spark("SmartCity-ScaleData")
    run(session, ap.parse_args().factor)
    session.stop()
