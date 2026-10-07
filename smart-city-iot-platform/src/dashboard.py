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
VIEWS = ["🚦 Traffic", "🌫️ Air quality", "🪟 6-h windows", "🔗 City snapshot (join)", "⚙️ Pipeline health"]

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
    if len(traffic):
        st.caption(f"Sensor clock: **{traffic['event_time'].max():%d %b %Y, %H:%M}** (replayed sensor time)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events processed", f"{len(traffic) + len(air):,}")
    recent = traffic[traffic["event_time"] >= traffic["event_time"].max() - pd.Timedelta(hours=6)] \
        if len(traffic) else traffic
    c2.metric("Alerts, last 6 h", int(recent["congestion_alert"].sum()) if len(recent) else 0)
    lat = pd.concat([traffic.get("latency_sec", pd.Series(dtype=float)),
                     air.get("latency_sec", pd.Series(dtype=float))])
    c3.metric("Median latency", f"{lat.median():.1f} s" if len(lat) else "-",
              help="Sensor file written to result stored. Target from the plan: under 10 s.")
    rate = "-"
    if len(progress):
        p = progress[(progress["query"] == "traffic_predictions") & (progress["input_rows"] > 0)].tail(10)
        rate = f"{p['processed_rows_per_sec'].mean():,.0f}" if len(p) else "-"
    c4.metric("Rows / s", rate, help="Spark processing rate, traffic query, last 10 micro-batches")


def traffic_tab(traffic, junction, hours):
    if traffic.empty:
        st.info("Waiting for traffic events. Start the streaming job and the replayer.")
        return
    t = traffic[traffic["junction"] == junction]
    end = t["event_time"].max()
    actual = t[t["event_time"] >= end - pd.Timedelta(hours=hours)][["event_time", "vehicles"]]
    pred = t[t["forecast_for"] >= end - pd.Timedelta(hours=hours)]
    series = ["Actual", "Predicted (next hour)", "Alert threshold"]
    long = pd.concat([
        actual.rename(columns={"event_time": "time", "vehicles": "value"}).assign(series=series[0]),
        pred[["forecast_for", "predicted_vehicles"]]
            .rename(columns={"forecast_for": "time", "predicted_vehicles": "value"}).assign(series=series[1]),
        pred[["forecast_for", "alert_threshold"]]
            .rename(columns={"forecast_for": "time", "alert_threshold": "value"}).assign(series=series[2]),
    ])
    hover = alt.selection_point(fields=["time"], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(long).encode(
        x=alt.X("time:T", title=None),
        y=alt.Y("value:Q", title="Vehicles per hour"),
        color=alt.Color("series:N", scale=alt.Scale(domain=series, range=[ACTUAL, PREDICTED, CRITICAL]),
                        legend=alt.Legend(orient="top", title=None, symbolType="stroke")),
        strokeDash=alt.StrokeDash("series:N", scale=alt.Scale(domain=series, range=[[1, 0], [5, 3], [2, 2]]),
                                  legend=None),
    )
    lines = base.mark_line(strokeWidth=2)
    points = base.mark_point(size=60, filled=True).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)),
        tooltip=[alt.Tooltip("time:T", format="%d %b %H:%M"), "series:N", alt.Tooltip("value:Q", format=".1f")],
    ).add_params(hover)
    rule = alt.Chart(long).mark_rule(color="#9a9893").encode(x="time:T").transform_filter(hover)
    flagged = pred[pred["congestion_alert"]]
    marks = alt.Chart(flagged).mark_point(shape="triangle-up", size=90, filled=True, color=CRITICAL).encode(
        x="forecast_for:T", y="predicted_vehicles:Q",
        tooltip=[alt.Tooltip("forecast_for:T", title="alert for", format="%d %b %H:%M"),
                 alt.Tooltip("predicted_vehicles:Q", title="predicted"),
                 alt.Tooltip("alert_threshold:Q", title="threshold")])
    st.altair_chart((lines + points + rule + marks).properties(height=320), use_container_width=True)
    st.caption("Alert (▲) when the forecast exceeds the dotted red line: the 90th percentile of the same hour "
               "of day at this junction over the previous 28 days, so 'unusually busy' adapts as traffic grows.")

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
    a = air[air["event_time"] >= air["event_time"].max() - pd.Timedelta(hours=hours)].sort_values("event_time")
    ref = a[["event_time", "co_gt"]].dropna().rename(columns={"co_gt": "value"})
    # break the reference line where the analyser was offline instead of bridging the gap
    ref["segment"] = (ref["event_time"].diff() > pd.Timedelta(hours=1)).cumsum()
    names = ["Reference analyser", "Soft sensor, drift-corrected", "Soft sensor, raw"]
    long = pd.concat([
        ref.assign(series=names[0]),
        a[["event_time", "calibrated_co"]].rename(columns={"calibrated_co": "value"}).assign(segment=-1, series=names[1]),
        a[["event_time", "estimated_co"]].rename(columns={"estimated_co": "value"}).assign(segment=-2, series=names[2]),
    ])
    chart = alt.Chart(long).mark_line(strokeWidth=2).encode(
        x=alt.X("event_time:T", title=None),
        y=alt.Y("value:Q", title="CO (mg/m³)"),
        color=alt.Color("series:N", scale=alt.Scale(domain=names, range=[ACTUAL, PREDICTED, "#a3a29b"]),
                        legend=alt.Legend(orient="top", title=None, symbolType="stroke")),
        strokeDash=alt.StrokeDash("series:N", scale=alt.Scale(domain=names, range=[[1, 0], [1, 0], [4, 3]]),
                                  legend=None),
        detail="segment:N",
        tooltip=[alt.Tooltip("event_time:T", format="%d %b %H:%M"), "series:N", alt.Tooltip("value:Q", format=".2f")],
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)
    both = air.dropna(subset=["co_gt"])
    if len(both):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Analyser offline", f"{air['co_gt'].isna().mean() * 100:.0f} % of hours",
                  help="The soft sensor still gives a value for these hours")
        c2.metric("MAE raw", f"{(both['estimated_co'] - both['co_gt']).abs().mean():.2f} mg/m³")
        c3.metric("MAE drift-corrected", f"{(both['calibrated_co'] - both['co_gt']).abs().mean():.2f} mg/m³")
        c4.metric("Calibration factor", f"× {air.sort_values('event_time')['calibration_factor'].iloc[-1]:.2f}",
                  help="Sum of true CO / sum of estimated CO over the last 24 readings with a reference value. The cheap "
                       "sensors drift as they age; this recalibrates the model online (× 1.00 = no drift).")
    thr = json.loads(config.THRESHOLDS_JSON.read_text())["air_co_mg_m3"]
    st.caption(f"The soft sensor estimates CO from cheap sensors, including hours when the expensive "
               f"reference analyser is offline (gaps in the blue line). Alerts use the drift-corrected value "
               f"(above {thr:.1f} mg/m³).")
    alerts = air[air["co_alert"]].sort_values("event_time", ascending=False).head(10)
    st.subheader("⚠ High-CO alerts (latest 10)")
    if alerts.empty:
        st.write("No alerts yet.")
    else:
        st.dataframe(alerts[["event_time", "calibrated_co", "estimated_co", "co_gt", "temperature"]], hide_index=True,
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
    # only the selected view is drawn (charts inside hidden tabs get sized wrongly)
    view = st.radio("View", list(VIEWS), horizontal=True, label_visibility="collapsed", key="view")
    if view == "🚦 Traffic":
        traffic_tab(traffic, junction, hours)
    elif view == "🌫️ Air quality":
        air_tab(air, hours)
    elif view == "🪟 6-h windows":
        windows_tab(windows)
    elif view == "🔗 City snapshot (join)":
        snapshot_tab(snapshot)
    else:
        health_tab(traffic, progress)


live()
weather_panel()
