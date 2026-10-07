"""Real-time layer: Spark Structured Streaming over the sensor events.

Four streaming queries share one SparkSession:
  traffic_predictions  - score each traffic reading with the saved model -> next-hour forecast + alert
  air_predictions      - soft-sensor CO estimate per air reading + alert
  traffic_window_6h    - 6-hour tumbling-window aggregates per junction (event time + watermark)
  city_snapshot        - stream-stream join of traffic and air readings from the same hour

Every sink uses foreachBatch and writes each micro-batch to <table>/batch_id=N with mode
'overwrite'. If Spark re-runs a batch after a crash it overwrites the same folder, so together with
the checkpoint and the replayable file source the output is exactly-once.

Run:  python -m src.streaming_job --reset
"""
import argparse
import json
import shutil
import time

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType,
                               TimestampType)

from src import config, features
from src.spark_utils import get_spark

TRAFFIC_EVENT_SCHEMA = StructType([
    StructField("sensor_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("junction", IntegerType()),
    StructField("vehicles", IntegerType()),
    StructField("forecast_for", TimestampType()),
    StructField("prev_hour_vehicles", DoubleType()),
    StructField("same_hour_yesterday", DoubleType()),
    StructField("same_hour_last_week", DoubleType()),
    StructField("rolling_3h_mean", DoubleType()),
    StructField("ingest_time", TimestampType()),
])
AIR_EVENT_SCHEMA = StructType(
    [StructField("sensor_id", StringType()), StructField("event_time", TimestampType())]
    + [StructField(c, DoubleType()) for c in features.AIR_NUMERIC + ["co_gt", "nox_gt", "no2_gt"]]
    + [StructField("original_time", TimestampType()), StructField("ingest_time", TimestampType())]
)


def idempotent_sink(name):
    target = config.GOLD / "stream" / name

    def write(batch_df, batch_id):
        batch_df = batch_df.withColumn("processed_time", F.current_timestamp())
        if "ingest_time" in batch_df.columns:
            batch_df = batch_df.withColumn(
                "latency_sec",
                F.col("processed_time").cast("double") - F.col("ingest_time").cast("double"))
        if batch_df.isEmpty():
            return
        batch_df.coalesce(1).write.mode("overwrite").parquet(config.p(target / f"batch_id={batch_id}"))
    return write


def start(df, name, output_mode="append"):
    return (df.writeStream.queryName(name)
            .outputMode(output_mode)
            .foreachBatch(idempotent_sink(name))
            .option("checkpointLocation", config.p(config.CHECKPOINTS / name))
            .trigger(processingTime=config.TRIGGER_INTERVAL)
            .start())


def build_queries(spark, max_files):
    thresholds = json.loads(config.THRESHOLDS_JSON.read_text())
    traffic_model = PipelineModel.load(config.p(config.TRAFFIC_MODEL_DIR))
    air_model = PipelineModel.load(config.p(config.AIR_MODEL_DIR))

    for d in (config.STREAM_TRAFFIC_DIR, config.STREAM_AIR_DIR):
        d.mkdir(parents=True, exist_ok=True)
    traffic = (spark.readStream.schema(TRAFFIC_EVENT_SCHEMA)
               .option("maxFilesPerTrigger", max_files)
               .json(config.p(config.STREAM_TRAFFIC_DIR))
               .withWatermark("event_time", config.WATERMARK))
    air = (spark.readStream.schema(AIR_EVENT_SCHEMA)
           .option("maxFilesPerTrigger", max_files)
           .json(config.p(config.STREAM_AIR_DIR))
           .withWatermark("event_time", config.WATERMARK))

    # 1) traffic forecast: time features of the hour being forecast, same as in training
    limit_map = F.create_map(*[x for j, v in thresholds["traffic_vehicles_by_junction"].items()
                               for x in (F.lit(int(j)), F.lit(float(v)))])
    scored = features.to_vehicle_forecast(
        traffic_model.transform(features.add_time_features(traffic, ts_col="forecast_for")))
    traffic_pred = (scored
                    .withColumn("predicted_vehicles", F.round("prediction", 1))
                    .withColumn("alert_threshold", limit_map[F.col("junction")])
                    .withColumn("congestion_alert", F.col("predicted_vehicles") > F.col("alert_threshold"))
                    .select("sensor_id", "junction", "event_time", "vehicles", "forecast_for",
                            "predicted_vehicles", "alert_threshold", "congestion_alert", "ingest_time"))

    # 2) air soft sensor
    co_limit = float(thresholds["air_co_mg_m3"])
    air_pred = (air_model.transform(features.add_time_features(air))
                .withColumn("estimated_co", F.round(F.greatest(F.lit(0.0), "prediction"), 3))
                .withColumn("co_alert", F.col("estimated_co") > F.lit(co_limit))
                .select("sensor_id", "event_time", "original_time", "estimated_co", "co_gt", "nox_gt",
                        "no2_gt", "temperature", "rel_humidity", "co_alert", "ingest_time"))

    # 3) windowed aggregate, finalised once the watermark passes the window end
    windowed = (traffic.groupBy(F.window("event_time", config.WINDOW), "junction")
                .agg(F.avg("vehicles").alias("avg_vehicles"), F.max("vehicles").alias("max_vehicles"),
                     F.count("*").alias("readings"))
                .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                        "junction", F.round("avg_vehicles", 2).alias("avg_vehicles"), "max_vehicles", "readings"))

    # 4) stream-stream join: readings from the same hour (time-range condition bounds the state)
    t = traffic.select("junction", "vehicles", F.col("event_time").alias("t_time"))
    a = air.select(F.col("event_time").alias("a_time"), "pt08_s1_co", "pt08_s3_nox", "temperature",
                   "rel_humidity")
    snapshot = (t.join(a, F.expr("a_time >= t_time - INTERVAL 30 MINUTES AND "
                                 "a_time <= t_time + INTERVAL 30 MINUTES"))
                .select(F.col("t_time").alias("event_time"), "junction", "vehicles", "pt08_s1_co",
                        "pt08_s3_nox", "temperature", "rel_humidity"))

    return [start(traffic_pred, "traffic_predictions"), start(air_pred, "air_predictions"),
            start(windowed, "traffic_window_6h"), start(snapshot, "city_snapshot")]


def monitor(spark, queries, run_seconds, idle_stop):
    """Log each query's progress (rows/s, batch duration) for the performance analysis."""
    config.STREAM_METRICS.mkdir(parents=True, exist_ok=True)
    log = config.STREAM_METRICS / "progress.jsonl"
    seen, began, last_input = {}, time.time(), time.time()
    try:
        while any(q.isActive for q in queries):
            time.sleep(2)
            for q in queries:
                for prog in q.recentProgress:
                    key = (prog["name"], prog["batchId"])
                    if key in seen:
                        continue
                    seen[key] = True
                    row = {
                        "query": prog["name"], "batch_id": prog["batchId"], "timestamp": prog["timestamp"],
                        "input_rows": prog["numInputRows"],
                        "input_rows_per_sec": prog.get("inputRowsPerSecond", 0.0),
                        "processed_rows_per_sec": prog.get("processedRowsPerSecond", 0.0),
                        "trigger_ms": prog["durationMs"].get("triggerExecution", 0),
                        "watermark": prog.get("eventTime", {}).get("watermark"),
                    }
                    with open(log, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps(row) + "\n")
                    if row["input_rows"]:
                        last_input = time.time()
                        if row["query"] == "traffic_predictions":
                            print(f"[{row['query']}] batch {row['batch_id']:4d}  rows {row['input_rows']:5d}  "
                                  f"{row['processed_rows_per_sec']:8.1f} rows/s  {row['trigger_ms']} ms  "
                                  f"watermark {row['watermark']}")
            for q in queries:
                if q.exception():
                    raise RuntimeError(f"query {q.name} failed: {q.exception()}")
            if run_seconds and time.time() - began > run_seconds:
                print("run time reached, stopping")
                break
            if idle_stop and time.time() - last_input > idle_stop:
                print(f"no new input for {idle_stop}s, stopping")
                break
    except KeyboardInterrupt:
        print("stopping...")
    finally:
        for q in queries:
            q.stop()


def main():
    ap = argparse.ArgumentParser(description="Smart City streaming job")
    ap.add_argument("--reset", action="store_true", help="delete previous stream outputs and checkpoints")
    ap.add_argument("--max-files-per-trigger", type=int, default=50)
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds (0 = run until Ctrl+C)")
    ap.add_argument("--idle-stop", type=int, default=0, help="stop after N seconds without new input")
    args = ap.parse_args()

    if args.reset:
        for d in (config.GOLD / "stream", config.CHECKPOINTS, config.STREAM_METRICS):
            shutil.rmtree(d, ignore_errors=True)

    spark = get_spark("SmartCity-Streaming", shuffle_partitions=4)
    queries = build_queries(spark, args.max_files_per_trigger)
    print(f"{len(queries)} streaming queries running; Spark UI: {spark.sparkContext.uiWebUrl}")
    monitor(spark, queries, args.run_seconds, args.idle_stop)
    spark.stop()


if __name__ == "__main__":
    main()
