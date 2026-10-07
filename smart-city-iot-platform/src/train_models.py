"""ML component: two Spark MLlib models that the streaming job uses.

1. Traffic forecaster  - next-hour vehicles per junction (time-series regression).
2. Air "soft sensor"   - estimates true CO (mg/m3) from the cheap metal-oxide sensors, a classic
                         IoT calibration task (the reference analyser is expensive and often offline).

Model selection uses a time-based validation window; the test window is only used once, at the end.
Run:  python -m src.train_models   (needs output/gold from src.batch_pipeline)
"""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import Imputer, OneHotEncoder, VectorAssembler  # noqa: E402
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src import config, features  # noqa: E402
from src.spark_utils import get_spark  # noqa: E402

SEED = 42


def candidate_models(label):
    return {
        "LinearRegression": [LinearRegression(labelCol=label, regParam=r, elasticNetParam=0.0)
                             for r in (0.0, 0.1)],
        "RandomForest": [RandomForestRegressor(labelCol=label, numTrees=60, maxDepth=d, seed=SEED)
                         for d in (8, 12)],
        "GBT": [GBTRegressor(labelCol=label, maxIter=100, maxDepth=d, stepSize=0.1, seed=SEED)
                for d in (4, 6)],
    }


def build_pipeline(estimator, numeric, categorical):
    imputed = [f"{c}_imp" for c in numeric]
    encoded = [f"{c}_ohe" for c in categorical]
    return Pipeline(stages=[
        # median imputation is learned on training data and replayed identically in streaming
        Imputer(inputCols=numeric, outputCols=imputed, strategy="median"),
        OneHotEncoder(inputCols=categorical, outputCols=encoded, handleInvalid="keep"),
        VectorAssembler(inputCols=imputed + encoded, outputCol="features", handleInvalid="keep"),
        estimator,
    ])


def evaluate(pred: DataFrame, label: str, prediction_col="prediction") -> dict:
    out = {}
    for metric in ("rmse", "mae", "r2"):
        ev = RegressionEvaluator(labelCol=label, predictionCol=prediction_col, metricName=metric)
        out[metric] = round(ev.evaluate(pred), 4)
    return out


def describe(est) -> str:
    keys = {"regParam", "numTrees", "maxDepth", "maxIter"}
    params = {p.name: est.getOrDefault(p) for p in est.extractParamMap() if p.name in keys}
    return ", ".join(f"{k}={v}" for k, v in sorted(params.items()))


def select_and_train(train, val, test, label, numeric, categorical, fit_label=None, post=lambda d: d):
    """Pick the best config per family on the validation window, refit on train+val, score test.
    fit_label/post let a model learn a transformed target (e.g. the hourly change) while it is
    still judged on the real target."""
    results, best = [], None
    for family, configs in candidate_models(fit_label or label).items():
        family_best = None
        for est in configs:
            model = build_pipeline(est, numeric, categorical).fit(train)
            val_metrics = evaluate(post(model.transform(val)), label)
            if family_best is None or val_metrics["rmse"] < family_best[1]["rmse"]:
                family_best = (est, val_metrics)
        est, val_metrics = family_best
        final = build_pipeline(est.copy(), numeric, categorical).fit(train.unionByName(val))
        test_pred = post(final.transform(test))
        row = {"model": family, "params": describe(est), "val": val_metrics, "test": evaluate(test_pred, label)}
        results.append(row)
        print(f"  {family:16s} {row['params']:30s} val RMSE={val_metrics['rmse']:.3f}  "
              f"test RMSE={row['test']['rmse']:.3f}  R2={row['test']['r2']:.3f}")
        if best is None or val_metrics["rmse"] < best[1]["val"]["rmse"]:
            best = (final, row, test_pred)
    return results, best


def vector_feature_names(pred_df):
    attrs = pred_df.schema["features"].metadata["ml_attr"]["attrs"]
    slots = sorted([a for group in attrs.values() for a in group], key=lambda a: a["idx"])
    return [a["name"] for a in slots]


def importances(model, pred_df, top=12):
    est = model.stages[-1]
    if not hasattr(est, "featureImportances"):
        return []
    names = vector_feature_names(pred_df)
    pairs = sorted(zip(names, est.featureImportances.toArray()), key=lambda x: -x[1])[:top]
    return [(n, round(float(v), 4)) for n, v in pairs]


# ----------------------------------------------------------------------------- traffic
def train_traffic(spark):
    print("\n=== Traffic forecaster (target: vehicles next hour, learned as change vs last hour) ===")
    df = (spark.read.parquet(config.p(config.GOLD / "traffic_features"))
          .dropna(subset=["prev_hour_vehicles", "same_hour_yesterday", "same_hour_last_week"]))
    train = df.filter(F.col("event_time") < config.TRAFFIC_VAL_START).cache()
    val = df.filter((F.col("event_time") >= config.TRAFFIC_VAL_START)
                    & (F.col("event_time") < config.TRAFFIC_TEST_START)).cache()
    test = df.filter(F.col("event_time") >= config.TRAFFIC_TEST_START).cache()
    sizes = {"train": train.count(), "val": val.count(), "test": test.count()}
    print("  rows:", sizes)

    label = features.TRAFFIC_LABEL
    # naive baselines a model must beat
    baselines = [
        {"model": "Baseline: last hour", "params": "y(t)=y(t-1)",
         "test": evaluate(test, label, "prev_hour_vehicles")},
        {"model": "Baseline: same hour last week", "params": "y(t)=y(t-168)",
         "test": evaluate(test, label, "same_hour_last_week")},
    ]
    for b in baselines:
        print(f"  {b['model']:30s} test RMSE={b['test']['rmse']:.3f}  R2={b['test']['r2']:.3f}")

    results, (model, best_row, test_pred) = select_and_train(
        train, val, test, label, features.TRAFFIC_NUMERIC, features.TRAFFIC_CATEGORICAL,
        fit_label=features.TRAFFIC_FIT_LABEL, post=features.to_vehicle_forecast)
    model.write().overwrite().save(config.p(config.TRAFFIC_MODEL_DIR))

    per_junction = {
        str(r["junction"]): evaluate(test_pred.filter(F.col("junction") == r["junction"]), label)
        for r in test_pred.select("junction").distinct().orderBy("junction").collect()
    }
    thresholds = {str(r["junction"]): float(r["p"]) for r in
                  train.unionByName(val).groupBy("junction")
                  .agg(F.percentile_approx("vehicles", config.ALERT_PERCENTILE).alias("p")).collect()}

    pdf = test_pred.select("event_time", "junction", "vehicles", "prediction").toPandas()
    plot_forecast(pdf, config.ML_OUT / "traffic_actual_vs_predicted.png")
    return {
        "rows": sizes, "baselines": baselines, "candidates": results, "best": best_row,
        "per_junction_test": per_junction, "feature_importance": importances(model, test_pred),
        "alert_thresholds_vehicles": thresholds,
    }


def plot_forecast(pdf, path, days=7):
    path.parent.mkdir(parents=True, exist_ok=True)
    start = pdf["event_time"].max() - pd.Timedelta(days=days)
    junctions = sorted(pdf["junction"].unique())
    fig, axes = plt.subplots(len(junctions), 1, figsize=(11, 2.4 * len(junctions)), sharex=True)
    for ax, j in zip(axes, junctions):
        d = pdf[(pdf.junction == j) & (pdf.event_time >= start)].sort_values("event_time")
        ax.plot(d.event_time, d.vehicles, label="actual", color="#1B1F3B", linewidth=1.4)
        ax.plot(d.event_time, d.prediction, label="predicted", color="#FF6B5B", linewidth=1.4, linestyle="--")
        ax.set_ylabel(f"Junction {j}")
        ax.grid(alpha=0.25)
    axes[0].legend(loc="upper right", frameon=False)
    axes[0].set_title(f"Next-hour traffic forecast vs actual (last {days} days of the test window)")
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


# ----------------------------------------------------------------------------- air
def train_air(spark):
    print("\n=== Air-quality soft sensor (target: CO(GT) mg/m3 from cheap sensors) ===")
    df = spark.read.parquet(config.p(config.GOLD / "air_features")).dropna(subset=[features.AIR_LABEL])
    train = df.filter(F.col("event_time") < config.AIR_VAL_START).cache()
    val = df.filter((F.col("event_time") >= config.AIR_VAL_START)
                    & (F.col("event_time") < config.AIR_TEST_START)).cache()
    test = df.filter(F.col("event_time") >= config.AIR_TEST_START).cache()
    sizes = {"train": train.count(), "val": val.count(), "test": test.count()}
    print("  rows:", sizes)

    results, (model, best_row, test_pred) = select_and_train(
        train, val, test, features.AIR_LABEL, features.AIR_NUMERIC, features.AIR_CATEGORICAL)
    model.write().overwrite().save(config.p(config.AIR_MODEL_DIR))
    co_threshold = float(train.unionByName(val)
                         .agg(F.percentile_approx(features.AIR_LABEL, config.ALERT_PERCENTILE)).first()[0])

    pdf = test_pred.select("event_time", "co_gt", "prediction").toPandas().sort_values("event_time")
    path = config.ML_OUT / "air_co_actual_vs_estimated.png"
    fig, ax = plt.subplots(figsize=(11, 3.2))
    d = pdf[pdf.event_time >= pdf.event_time.max() - pd.Timedelta(days=10)]
    ax.plot(d.event_time, d.co_gt, label="reference analyser CO", color="#1B1F3B", linewidth=1.4)
    ax.plot(d.event_time, d.prediction, label="soft-sensor estimate", color="#14B8A6", linewidth=1.4,
            linestyle="--")
    ax.set_ylabel("CO mg/m3")
    ax.set_title("Air-quality soft sensor: estimated vs reference CO (last 10 days of test window)")
    ax.legend(frameon=False)
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return {"rows": sizes, "candidates": results, "best": best_row,
            "feature_importance": importances(model, test_pred), "alert_threshold_co": co_threshold}


def plot_model_comparison(summary, path):
    rows = summary["traffic"]["baselines"] + summary["traffic"]["candidates"]
    names = [r["model"].replace("Baseline: ", "Base: ") for r in rows]
    rmse = [r["test"]["rmse"] for r in rows]
    fig, ax = plt.subplots(figsize=(8, 3.6))
    colors = ["#C9CCE0" if n.startswith("Base") else "#FF6B5B" for n in names]
    bars = ax.barh(names, rmse, color=colors)
    ax.bar_label(bars, fmt="%.2f", padding=3)
    ax.invert_yaxis()
    ax.set_xlabel("Test RMSE (vehicles/hour, lower is better)")
    ax.set_title("Traffic forecast: models vs naive baselines")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)


def run(spark):
    config.ML_OUT.mkdir(parents=True, exist_ok=True)
    config.MODELS.mkdir(parents=True, exist_ok=True)
    summary = {"traffic": train_traffic(spark), "air": train_air(spark)}
    config.THRESHOLDS_JSON.write_text(json.dumps({
        "traffic_vehicles_by_junction": summary["traffic"]["alert_thresholds_vehicles"],
        "air_co_mg_m3": summary["air"]["alert_threshold_co"],
        "percentile": config.ALERT_PERCENTILE,
    }, indent=2))
    (config.ML_OUT / "metrics.json").write_text(json.dumps(summary, indent=2, default=str))
    plot_model_comparison(summary, config.ML_OUT / "traffic_model_comparison.png")
    print("\nBest traffic model:", summary["traffic"]["best"]["model"], summary["traffic"]["best"]["test"])
    print("Best air model:    ", summary["air"]["best"]["model"], summary["air"]["best"]["test"])
    return summary


if __name__ == "__main__":
    session = get_spark("SmartCity-TrainModels")
    run(session)
    session.stop()
