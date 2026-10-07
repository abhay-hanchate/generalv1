"""Data-serving layer: live Streamlit dashboard over the gold streaming tables.

Run (after starting the streaming job and the replayer):
    streamlit run src/dashboard.py
"""
import json
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import pyarrow.dataset as ds
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import config  # noqa: E402

# categorical slots in fixed order (one per junction), status red reserved for alerts
JUNCTION_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
ACTUAL, PREDICTED, CRITICAL = "#2a78d6", "#eb6834", "#d03b3b"
STREAM = config.GOLD / "stream"

st.set_page_config(page_title="Smart City IoT Platform", page_icon="🏙️", layout="wide")


def read_table(name: str) -> pd.DataFrame:
    """Read every committed micro-batch folder. Spark's _temporary/_SUCCESS files are skipped."""
    path = STREAM / name
    if not path.exists():
        return pd.DataFrame()
    try:
        table = ds.dataset(str(path), format="parquet", partitioning="hive",
                           ignore_prefixes=[".", "_"]).to_table()
    except Exception:  # a batch folder appearing mid-read; next refresh picks it up
        return pd.DataFrame()
    df = table.to_pandas()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = pd.to_datetime(df[c]).dt.tz_localize(None)
    return df


@st.cache_data(ttl=600)
def live_weather(lat, lon):
    import requests
    r = requests.get("https://api.open-meteo.com/v1/forecast",
                     params={"latitude": lat, "longitude": lon, "timezone": "auto",
                             "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"},
                     timeout=10)
    r.raise_for_status()
    return r.json()["current"]


def kpi_row(traffic, air, progress):
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Sensor clock", traffic["event_time"].max().strftime("%d %b %Y %H:%M") if len(traffic) else "-")
    c2.metric("Events processed", f"{len(traffic) + len(air):,}")
    recent = traffic[traffic["event_time"] >= traffic["event_time"].max() - pd.Timedelta(hours=6)] \
        if len(traffic) else traffic
    c3.metric("Congestion alerts (last 6 h)", int(recent["congestion_alert"].sum()) if len(recent) else 0)
    lat = pd.concat([traffic.get("latency_sec", pd.Series(dtype=float)),
                     air.get("latency_sec", pd.Series(dtype=float))])
    c4.metric("Median latency (ingest to result)", f"{lat.median():.1f} s" if len(lat) else "-",
              help="Target from the plan: under 10 s end-to-end")
    if len(progress):
        p = progress[progress["query"] == "traffic_predictions"].tail(10)
        c5.metric("Throughput (rows/s, last 10 batches)", f"{p['processed_rows_per_sec'].mean():,.0f}")
    else:
        c5.metric("Throughput", "-")


def traffic_tab(traffic, junction, hours):
    if traffic.empty:
        st.info("Waiting for traffic events. Start the streaming job and the replayer.")
        return
    t = traffic[traffic["junction"] == junction]
    end = t["event_time"].max()
    actual = t[t["event_time"] >= end - pd.Timedelta(hours=hours)][["event_time", "vehicles"]]
    pred = t[t["forecast_for"] >= end - pd.Timedelta(hours=hours)][["forecast_for", "predicted_vehicles",
                                                                       "congestion_alert"]]
    long = pd.concat([
        actual.rename(columns={"event_time": "time", "vehicles": "value"}).assign(series="Actual"),
        pred.rename(columns={"forecast_for": "time", "predicted_vehicles": "value"})
            .drop(columns="congestion_alert").assign(series="Predicted (next hour)"),
    ])
    hover = alt.selection_point(fields=["time"], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(long).encode(
        x=alt.X("time:T", title=None),
        y=alt.Y("value:Q", title="Vehicles per hour"),
        color=alt.Color("series:N", scale=alt.Scale(domain=["Actual", "Predicted (next hour)"],
                                                    range=[ACTUAL, PREDICTED]),
                        legend=alt.Legend(orient="top", title=None)),
        strokeDash=alt.StrokeDash("series:N", scale=alt.Scale(domain=["Actual", "Predicted (next hour)"],
                                                              range=[[1, 0], [5, 3]]), legend=None),
    )
    lines = base.mark_line(strokeWidth=2)
    points = base.mark_point(size=60, filled=True).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("time:T", format="%d %b %H:%M"), "series:N", alt.Tooltip("value:Q", format=".1f")],
    ).add_params(hover)
    rule = alt.Chart(long).mark_rule(color="#9a9893").encode(x="time:T").transform_filter(hover)
    limit = t["alert_threshold"].iloc[-1]
    threshold = alt.Chart(pd.DataFrame({"y": [limit]})).mark_rule(color=CRITICAL, strokeDash=[2, 2]).encode(y="y:Q")
    st.altair_chart((lines + points + rule + threshold).properties(height=320), use_container_width=True)
    st.caption(f"Red dotted line = alert threshold for junction {junction} "
               f"({limit:.0f} vehicles/h, the 90th percentile seen in training).")

    m = t.dropna(subset=["predicted_vehicles"]).merge(
        t[["event_time", "vehicles"]].rename(columns={"event_time": "forecast_for", "vehicles": "actual_next"}),
        on="forecast_for")
    if len(m):
        err = (m["predicted_vehicles"] - m["actual_next"]).abs()
        st.write(f"**Live accuracy, junction {junction}:** MAE {err.mean():.2f} vehicles/h over {len(m):,} forecasts")

    alerts = traffic[traffic["congestion_alert"]].sort_values("forecast_for", ascending=False).head(15)
    st.subheader("⚠ Congestion alerts (latest 15)")
    if alerts.empty:
        st.write("No alerts yet.")
    else:
        st.dataframe(alerts[["forecast_for", "junction", "predicted_vehicles", "alert_threshold"]]
                     .rename(columns={"forecast_for": "forecast hour", "predicted_vehicles": "predicted",
                                      "alert_threshold": "threshold"}),
                     hide_index=True, use_container_width=True)


def air_tab(air, hours):
    if air.empty:
        st.info("Waiting for air-quality events.")
        return
    a = air[air["event_time"] >= air["event_time"].max() - pd.Timedelta(hours=hours)]
    long = pd.concat([
        a[["event_time", "co_gt"]].rename(columns={"co_gt": "value"}).assign(series="Reference analyser"),
        a[["event_time", "estimated_co"]].rename(columns={"estimated_co": "value"}).assign(series="Soft-sensor estimate"),
    ]).dropna()
    chart = alt.Chart(long).mark_line(strokeWidth=2).encode(
        x=alt.X("event_time:T", title=None),
        y=alt.Y("value:Q", title="CO (mg/m³)"),
        color=alt.Color("series:N", scale=alt.Scale(range=[ACTUAL, PREDICTED]),
                        legend=alt.Legend(orient="top", title=None)),
        tooltip=[alt.Tooltip("event_time:T", format="%d %b %H:%M"), "series:N", alt.Tooltip("value:Q", format=".2f")],
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)
    thr = json.loads(config.THRESHOLDS_JSON.read_text())["air_co_mg_m3"]
    st.caption(f"The soft sensor estimates CO from cheap sensors, including hours when the expensive "
               f"reference analyser is offline (gaps in the blue line). Alert above {thr:.1f} mg/m³.")
    alerts = air[air["co_alert"]].sort_values("event_time", ascending=False).head(10)
    st.subheader("⚠ High-CO alerts (latest 10)")
    if alerts.empty:
        st.write("No alerts yet.")
    else:
        st.dataframe(alerts[["event_time", "estimated_co", "co_gt", "temperature"]], hide_index=True,
                     use_container_width=True)


def windows_tab(windows):
    if windows.empty:
        st.info("6-hour windows appear once the watermark passes the end of the first window.")
        return
    w = windows.sort_values("window_start").tail(4 * 28)
    chart = alt.Chart(w).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=30)).encode(
        x=alt.X("window_start:T", title="Window start"),
        y=alt.Y("avg_vehicles:Q", title="Average vehicles per hour"),
        color=alt.Color("junction:N", scale=alt.Scale(domain=[1, 2, 3, 4], range=JUNCTION_COLORS),
                        legend=alt.Legend(orient="top", title="Junction")),
        tooltip=["junction:N", alt.Tooltip("window_start:T", format="%d %b %H:%M"),
                 "avg_vehicles:Q", "max_vehicles:Q", "readings:Q"],
    ).properties(height=320)
    st.altair_chart(chart, use_container_width=True)
    st.caption("Tumbling 6-hour windows on sensor time, emitted once the 2-hour watermark has passed.")


def snapshot_tab(snapshot):
    if snapshot.empty:
        st.info("Waiting for the stream-stream join (traffic + air readings in the same hour).")
        return
    s = snapshot.tail(2000)
    chart = alt.Chart(s).mark_circle(size=40, opacity=0.6).encode(
        x=alt.X("vehicles:Q", title="Vehicles per hour"),
        y=alt.Y("pt08_s1_co:Q", title="CO sensor signal (PT08.S1)", scale=alt.Scale(zero=False)),
        color=alt.Color("junction:N", scale=alt.Scale(domain=[1, 2, 3, 4], range=JUNCTION_COLORS),
                        legend=alt.Legend(orient="top", title="Junction")),
        tooltip=["event_time:T", "junction:N", "vehicles:Q", "pt08_s1_co:Q", "temperature:Q"],
    ).properties(height=320)
    st.altair_chart(chart, use_container_width=True)
    st.caption("Output of the stream-stream join. The two datasets come from different cities, so this "
               "shows the join mechanics; it is not evidence of a real traffic-pollution link.")


def health_tab(traffic, progress):
    if progress.empty:
        st.info("No streaming progress logged yet.")
        return
    p = progress[progress["input_rows"] > 0].copy()
    p["time"] = pd.to_datetime(p["timestamp"]).dt.tz_localize(None)
    chart = alt.Chart(p).mark_line(strokeWidth=2).encode(
        x=alt.X("time:T", title=None), y=alt.Y("trigger_ms:Q", title="Micro-batch duration (ms)"),
        color=alt.Color("query:N", scale=alt.Scale(range=JUNCTION_COLORS), legend=alt.Legend(orient="top", title=None)),
        tooltip=["query:N", "batch_id:Q", "input_rows:Q", "processed_rows_per_sec:Q", "trigger_ms:Q"],
    ).properties(height=260)
    st.altair_chart(chart, use_container_width=True)
    if "latency_sec" in traffic:
        st.write("**End-to-end latency (s), traffic predictions:**")
        st.dataframe(traffic["latency_sec"].describe(percentiles=[0.5, 0.95]).to_frame().T.round(2),
                     use_container_width=True)


def weather_panel():
    st.subheader(f"Live weather: {config.CITY['name']}")
    try:
        w = live_weather(config.CITY["lat"], config.CITY["lon"])
        c = st.columns(4)
        c[0].metric("Temperature", f"{w['temperature_2m']} °C")
        c[1].metric("Humidity", f"{w['relative_humidity_2m']} %")
        c[2].metric("Rain", f"{w['precipitation']} mm")
        c[3].metric("Wind", f"{w['wind_speed_10m']} km/h")
        st.caption(f"Open-Meteo API, observed {w['time']} local time. Refreshed every 10 min.")
    except Exception as exc:
        st.warning(f"Weather API unreachable ({type(exc).__name__}). The rest of the dashboard is unaffected.")


st.title("🏙️ Smart City IoT Data Platform")
st.caption("Live traffic forecasts, air-quality estimates and alerts, served from Spark Structured Streaming.")
with st.sidebar:
    st.header("Controls")
    refresh = st.slider("Refresh every (s)", 2, 30, 5)
    hours = st.slider("History shown (sensor hours)", 24, 24 * 14, 72, step=24)
    junction = st.selectbox("Junction", [1, 2, 3, 4])


@st.fragment(run_every=refresh)
def live():
    traffic, air = read_table("traffic_predictions"), read_table("air_predictions")
    windows, snapshot = read_table("traffic_window_6h"), read_table("city_snapshot")
    log = config.STREAM_METRICS / "progress.jsonl"
    progress = pd.read_json(log, lines=True) if log.exists() and log.stat().st_size else pd.DataFrame()
    kpi_row(traffic, air, progress)
    tabs = st.tabs(["🚦 Traffic", "🌫️ Air quality", "🪟 6-h windows", "🔗 City snapshot (join)", "⚙️ Pipeline health"])
    with tabs[0]:
        traffic_tab(traffic, junction, hours)
    with tabs[1]:
        air_tab(air, hours)
    with tabs[2]:
        windows_tab(windows)
    with tabs[3]:
        snapshot_tab(snapshot)
    with tabs[4]:
        health_tab(traffic, progress)


live()
weather_panel()
