"""SparkSession factory and small timing helpers."""
import os
import statistics
import sys
import time


def check_windows_setup():
    """Fail early with a clear message instead of an obscure Hadoop stack trace."""
    if os.name != "nt":
        return
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home or not os.path.exists(os.path.join(hadoop_home, "bin", "winutils.exe")):
        raise RuntimeError(
            "HADOOP_HOME is not set or winutils.exe is missing. "
            "Run setup_windows.ps1 (see README, section 'Windows setup') and open a new terminal."
        )
    if not os.environ.get("JAVA_HOME"):
        print("WARNING: JAVA_HOME is not set; Spark may not find Java 17.")


def get_spark(app_name="SmartCityIoT", shuffle_partitions=8, driver_memory="4g", extra_conf=None):
    # On Windows Spark otherwise launches 'python' from PATH, which may be a different
    # interpreter than this venv ("Python worker failed to connect back").
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    check_windows_setup()

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        # Timestamps in the datasets carry no zone; pin UTC so nothing shifts by IST/DST.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_noop(df):
    """Execute a DataFrame fully without writing anything (the right action for benchmarks)."""
    df.write.format("noop").mode("overwrite").save()


def time_runs(fn, repeats=3, warmup=1):
    """Run fn warmup+repeats times; return (median_seconds, all_timed_runs)."""
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return statistics.median(times), times


def executed_plan(df) -> str:
    return df._jdf.queryExecution().executedPlan().toString()
