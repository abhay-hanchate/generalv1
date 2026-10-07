"""Step 5 + 7: performance optimization experiments on the scaled traffic data.

Each experiment runs a workload in a baseline configuration and an optimized one. Every variant
has one warm-up run and then 3 timed runs, and the median is reported. Workloads end in the 'noop'
sink, so Spark does all the work but disk writes do not distort the timing.

  E1 caching           reuse an expensive feature DataFrame for 3 queries: no cache vs persist()
  E2 shuffle partitions  groupBy with spark.sql.shuffle.partitions = 200 / 64 / 16 / 8 (AQE off)
  E3 join strategy     fact x dimension: sort-merge join vs broadcast hash join
  E4 file format       CSV vs Parquet: full scan + aggregate, and a filtered query (partition pruning)
  E5 repartition vs coalesce  writing 8 output files: full shuffle vs merge (time and file balance)
  E6 AQE               the E2 workload at 200 partitions with Adaptive Query Execution off vs on

Run:  python -m src.benchmark          (after python -m src.scale_data)
"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src import config, features  # noqa: E402
from src.batch_pipeline import junction_dim  # noqa: E402
from src.scale_data import SCALED_CSV, SCALED_PARQUET  # noqa: E402
from src.spark_utils import executed_plan, get_spark, run_noop, time_runs  # noqa: E402

REPEATS = 3


def dir_size_mb(path):
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files if not f.startswith((".", "_")))
    return round(total / 1e6, 1)


def record(results, experiment, variant, fn, note="", repeats=REPEATS):
    median, runs = time_runs(fn, repeats=repeats)
    results.append({"experiment": experiment, "variant": variant, "median_s": round(median, 3),
                    "runs_s": [round(r, 3) for r in runs], "note": note})
    print(f"  {experiment:22s} {variant:34s} {median:7.2f} s")


def set_conf(spark, **conf):
    for k, v in conf.items():
        spark.conf.set(k, str(v))


def default_conf(spark):
    set_conf(spark, **{"spark.sql.shuffle.partitions": 8, "spark.sql.adaptive.enabled": "true",
                       "spark.sql.autoBroadcastJoinThreshold": 10 * 1024 * 1024})


def e1_caching(spark, results):
    def feature_df():
        df = spark.read.parquet(config.p(SCALED_PARQUET))
        return features.add_time_features(features.add_traffic_lags(df))

    # Every query uses all window features (as model training does). If a query used only one
    # feature, Spark's optimizer would skip the other windows and the comparison would be unfair.
    lag_cols = [*features.LAGS, "rolling_3h_mean"]
    queries = [
        lambda d: d.groupBy("junction").agg(*[F.avg(c) for c in lag_cols]),
        lambda d: d.groupBy("hour", "is_weekend").agg(*[F.stddev(c) for c in lag_cols]),
        lambda d: d.filter(F.col("vehicles") > F.greatest(*lag_cols) * 1.2).groupBy("year").count(),
    ]

    def no_cache():
        d = feature_df()
        for q in queries:
            run_noop(q(d))

    def with_cache():
        d = feature_df().persist(StorageLevel.MEMORY_AND_DISK)
        d.count()  # materialise once (included in the timing)
        for q in queries:
            run_noop(q(d))
        d.unpersist(blocking=True)

    record(results, "E1 caching", "no cache (lineage recomputed 3x)", no_cache, repeats=2)
    record(results, "E1 caching", "persist(MEMORY_AND_DISK)", with_cache, repeats=2)


def shuffle_workload(spark):
    df = spark.read.parquet(config.p(SCALED_PARQUET))
    return (features.add_time_features(df)
            .groupBy("junction", "hour", "day_of_week")
            .agg(F.avg("vehicles").alias("avg_v"), F.stddev("vehicles").alias("sd_v"), F.count("*").alias("n")))


def e2_shuffle_partitions(spark, results):
    set_conf(spark, **{"spark.sql.adaptive.enabled": "false"})
    for n in (200, 64, 16, 8):
        set_conf(spark, **{"spark.sql.shuffle.partitions": n})
        record(results, "E2 shuffle partitions", f"{n} partitions (AQE off)",
               lambda: run_noop(shuffle_workload(spark)))
    default_conf(spark)


def e3_join(spark, results):
    facts = spark.read.parquet(config.p(SCALED_PARQUET))
    n = facts.agg(F.max("junction")).first()[0]
    dim = junction_dim(spark, n).cache()
    dim.count()
    set_conf(spark, **{"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": -1})

    def smj():
        return facts.join(dim, "junction").groupBy("zone", "road_type").agg(F.sum("vehicles"))

    def bhj():
        return facts.join(F.broadcast(dim), "junction").groupBy("zone", "road_type").agg(F.sum("vehicles"))

    plans = {"sort_merge": "SortMergeJoin" in executed_plan(smj()),
             "broadcast": "BroadcastHashJoin" in executed_plan(bhj())}
    record(results, "E3 join strategy", "sort-merge join (shuffles both sides)", lambda: run_noop(smj()),
           note=f"plan has SortMergeJoin={plans['sort_merge']}")
    record(results, "E3 join strategy", "broadcast hash join (dimension copied)", lambda: run_noop(bhj()),
           note=f"plan has BroadcastHashJoin={plans['broadcast']}")
    dim.unpersist()
    default_conf(spark)


def e4_formats(spark, results):
    schema = "event_time TIMESTAMP, junction INT, vehicles INT, year INT"
    csv = lambda: spark.read.option("header", True).schema(schema).csv(config.p(SCALED_CSV))  # noqa: E731
    pq = lambda: spark.read.parquet(config.p(SCALED_PARQUET))  # noqa: E731
    agg = lambda d: d.groupBy("junction").agg(F.avg("vehicles"))  # noqa: E731
    filt = lambda d: d.filter((F.col("year") == 2017) & (F.col("junction") < 50)).agg(F.sum("vehicles"))  # noqa: E731
    sizes = f"CSV {dir_size_mb(SCALED_CSV)} MB vs Parquet {dir_size_mb(SCALED_PARQUET)} MB"
    record(results, "E4 file format", "CSV full scan + groupBy", lambda: run_noop(agg(csv())), note=sizes)
    record(results, "E4 file format", "Parquet full scan + groupBy", lambda: run_noop(agg(pq())), note=sizes)
    record(results, "E4 file format", "CSV filter year=2017", lambda: run_noop(filt(csv())))
    record(results, "E4 file format", "Parquet filter (partition pruning)", lambda: run_noop(filt(pq())),
           note="only the year=2017 folder is read")


def e5_repartition_coalesce(spark, results):
    # small input splits -> ~30 input partitions, so both methods can reach 8 output files
    # (coalesce can only reduce the partition count, never increase it)
    set_conf(spark, **{"spark.sql.files.maxPartitionBytes": 1024 * 1024})
    df = spark.read.parquet(config.p(SCALED_PARQUET)).filter(F.col("vehicles") > 20)
    print(f"  E5 input partitions: {df.rdd.getNumPartitions()}")
    out = config.PERF_DATA / "write_test"
    balance = {}

    def write(method):
        d = df.repartition(8) if method == "repartition" else df.coalesce(8)
        d.write.mode("overwrite").parquet(config.p(out / method))
        sizes = [os.path.getsize(os.path.join(out / method, f)) for f in os.listdir(out / method)
                 if f.endswith(".parquet")]
        balance[method] = f"{len(sizes)} files, size spread (max/min) {max(sizes) / max(min(sizes), 1):.2f}x"

    record(results, "E5 repartition/coalesce", "repartition(8) - full shuffle", lambda: write("repartition"))
    record(results, "E5 repartition/coalesce", "coalesce(8) - no shuffle", lambda: write("coalesce"))
    results[-2]["note"], results[-1]["note"] = balance["repartition"], balance["coalesce"]
    spark.conf.unset("spark.sql.files.maxPartitionBytes")


def e6_aqe(spark, results):
    set_conf(spark, **{"spark.sql.shuffle.partitions": 200})
    for aqe in ("false", "true"):
        set_conf(spark, **{"spark.sql.adaptive.enabled": aqe})
        record(results, "E6 adaptive execution", f"200 partitions, AQE {'on' if aqe == 'true' else 'off'}",
               lambda: run_noop(shuffle_workload(spark)),
               note="AQE coalesces tiny shuffle partitions at runtime" if aqe == "true" else "")
    default_conf(spark)


def plot(results, path):
    df = pd.DataFrame(results)
    exps = list(dict.fromkeys(df["experiment"]))
    fig, axes = plt.subplots(len(exps), 1, figsize=(9, 1.25 * len(df) + 0.6 * len(exps)))
    for ax, exp in zip(axes, exps):
        d = df[df["experiment"] == exp]
        fastest = d["median_s"].min()
        colors = ["#2a78d6" if v == fastest else "#b7c4d9" for v in d["median_s"]]
        bars = ax.barh(d["variant"], d["median_s"], color=colors, height=0.6)
        ax.bar_label(bars, fmt="%.2f s", padding=3, fontsize=9)
        ax.set_title(exp, loc="left", fontsize=11, fontweight="bold")
        ax.invert_yaxis()
        ax.set_xlim(0, d["median_s"].max() * 1.25)
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(labelsize=9)
    axes[-1].set_xlabel("Median wall-clock time (s), lower is better")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def run(spark, experiments=("e1", "e2", "e3", "e4", "e5", "e6")):
    if not SCALED_PARQUET.exists():
        raise SystemExit("Scaled data not found. Run: python -m src.scale_data --factor 100")
    config.PERF.mkdir(parents=True, exist_ok=True)
    results = []
    rows = spark.read.parquet(config.p(SCALED_PARQUET)).count()
    print(f"benchmark data: {rows:,} rows, {os.cpu_count()} CPU cores, Spark {spark.version}")
    funcs = {"e1": e1_caching, "e2": e2_shuffle_partitions, "e3": e3_join, "e4": e4_formats,
             "e5": e5_repartition_coalesce, "e6": e6_aqe}
    for key in experiments:
        default_conf(spark)
        spark.catalog.clearCache()
        funcs[key](spark, results)

    df = pd.DataFrame(results)
    df["speedup_vs_slowest_in_experiment"] = (
        df.groupby("experiment")["median_s"].transform("max") / df["median_s"]).round(2)
    df.drop(columns="runs_s").to_csv(config.PERF / "benchmark_results.csv", index=False)
    (config.PERF / "benchmark_results.json").write_text(json.dumps(
        {"rows": rows, "cores": os.cpu_count(), "spark": spark.version, "results": results}, indent=2))
    plot(results, config.PERF / "benchmark_results.png")
    print(f"\nsaved {config.PERF / 'benchmark_results.csv'} and .png")
    return df


if __name__ == "__main__":
    session = get_spark("SmartCity-Benchmark")
    run(session)
    session.stop()
