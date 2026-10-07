"""IoT sensor simulator: replays the real datasets as a live stream of JSON events.

Each tick writes one JSON-lines file per sensor type into data/stream_input/{traffic,air}/.
Files are written to a staging folder first and then moved in, so Spark never reads a half-written
file. The replay starts at the hold-out (test) period, so the models only see data they were not
trained on.

Simulation assumptions (state these in the report):
* The two datasets come from different cities and years. The air-quality timeline is shifted by a
  whole number of weeks so both streams share one clock (same weekday and hour are preserved).
* Traffic events carry recent-history context (last hour, same hour yesterday / last week), as an
  edge gateway or feature store would. Structured Streaming cannot compute lag() over a stream.

Run:  python -m src.replayer --hours-per-tick 1 --interval 1
"""
import argparse
import json
import os
import shutil
import time
from datetime import datetime, timezone

import pandas as pd

from src import config
from src.features import traffic_context_pandas

AIR_RENAME = {
    "PT08.S1(CO)": "pt08_s1_co", "PT08.S2(NMHC)": "pt08_s2_nmhc", "PT08.S3(NOx)": "pt08_s3_nox",
    "PT08.S4(NO2)": "pt08_s4_no2", "PT08.S5(O3)": "pt08_s5_o3", "T": "temperature",
    "RH": "rel_humidity", "AH": "abs_humidity", "CO(GT)": "co_gt", "NOx(GT)": "nox_gt", "NO2(GT)": "no2_gt",
}


def load_traffic_events(start):
    raw = pd.read_csv(config.TRAFFIC_TRAIN_CSV, parse_dates=["DateTime"])
    raw = raw.rename(columns={"DateTime": "event_time", "Junction": "junction", "Vehicles": "vehicles"})
    raw = raw.drop_duplicates(["junction", "event_time"])
    ctx = traffic_context_pandas(raw[["event_time", "junction", "vehicles"]])
    ctx = ctx[ctx["event_time"] >= start]
    ctx.insert(0, "sensor_id", "TRF-J" + ctx["junction"].astype(str))
    return ctx


def load_air_events(sim_start, air_start):
    raw = pd.read_csv(config.AIR_CSV, sep=";", decimal=",").dropna(how="all", axis=1).dropna(how="all")
    raw["event_time"] = pd.to_datetime(raw["Date"] + " " + raw["Time"], format="%d/%m/%Y %H.%M.%S")
    raw = raw.rename(columns=AIR_RENAME)[["event_time", *AIR_RENAME.values()]]
    raw = raw.replace(-200, float("nan")).drop_duplicates("event_time")
    raw = raw[raw["event_time"] >= air_start].copy()
    # shift by whole weeks so weekday/hour patterns survive the move onto the traffic clock
    weeks = round((pd.Timestamp(sim_start) - raw["event_time"].min()) / pd.Timedelta(weeks=1))
    raw["original_time"] = raw["event_time"]
    raw["event_time"] = raw["event_time"] + pd.Timedelta(weeks=weeks)
    raw.insert(0, "sensor_id", "AQ-01")
    return raw, weeks


def to_records(df):
    out = []
    for rec in df.to_dict(orient="records"):
        clean = {}
        for k, v in rec.items():
            if isinstance(v, pd.Timestamp):
                clean[k] = v.strftime("%Y-%m-%dT%H:%M:%S")
            elif isinstance(v, float) and pd.isna(v):
                clean[k] = None
            elif hasattr(v, "item"):  # numpy scalar
                clean[k] = v.item()
            else:
                clean[k] = v
        out.append(clean)
    return out


def write_atomic(records, target_dir, name, ingest_time):
    if not records:
        return 0
    config.STREAM_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    tmp = config.STREAM_STAGING_DIR / f"{name}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        for r in records:
            r["ingest_time"] = ingest_time
            fh.write(json.dumps(r) + "\n")
    os.replace(tmp, target_dir / name)  # atomic rename on the same drive
    return len(records)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=config.TRAFFIC_TEST_START, help="sensor-time start (default: test period)")
    ap.add_argument("--hours-per-tick", type=int, default=1, help="sensor hours emitted per tick")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between ticks")
    ap.add_argument("--max-ticks", type=int, default=0, help="stop after N ticks (0 = replay everything)")
    ap.add_argument("--reset", action="store_true", help="delete previous stream input files first")
    args = ap.parse_args()

    if args.reset and config.STREAM_INPUT.exists():
        shutil.rmtree(config.STREAM_INPUT)

    start = pd.Timestamp(args.start)
    traffic = load_traffic_events(start)
    air, weeks = load_air_events(start, pd.Timestamp(config.AIR_TEST_START))
    print(f"traffic events: {len(traffic):,}  air events: {len(air):,}  "
          f"(air timeline shifted by {weeks} weeks)")

    hours = pd.date_range(start, traffic["event_time"].max(), freq="h")
    tick, sent = 0, 0
    t_groups = dict(tuple(traffic.groupby("event_time")))
    a_groups = dict(tuple(air.groupby("event_time")))
    try:
        for i in range(0, len(hours), args.hours_per_tick):
            block = hours[i:i + args.hours_per_tick]
            t_rec = to_records(pd.concat([t_groups[h] for h in block if h in t_groups] or [traffic.iloc[:0]]))
            a_rec = to_records(pd.concat([a_groups[h] for h in block if h in a_groups] or [air.iloc[:0]]))
            now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            name = f"{tick:06d}_{block[0]:%Y%m%dT%H}.json"
            sent += write_atomic(t_rec, config.STREAM_TRAFFIC_DIR, name, now)
            sent += write_atomic(a_rec, config.STREAM_AIR_DIR, name, now)
            tick += 1
            if tick % 24 == 0:
                print(f"tick {tick:5d}  sensor time {block[-1]}  events sent {sent:,}")
            if args.max_ticks and tick >= args.max_ticks:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    print(f"done: {tick} ticks, {sent:,} events")


if __name__ == "__main__":
    main()
