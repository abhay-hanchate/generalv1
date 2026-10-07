# Case Study 6 — Smart City IoT Data Platform

**Syllabus mapping:** 6.1, 6.3 — Real-time pipelines, ML systems

## 1. What the project actually asks for

A city has thousands of IoT sensors (traffic counters, air-quality monitors, weather stations, parking sensors) sending data every few seconds. We must design and build a **platform** that:

| Requirement (from brief) | What it means for us |
|---|---|
| Identify IoT sources | List the sensors, what they send, how often, and in what format |
| Design streaming architecture | Ingest → process → store → serve, in real time |
| Identify real-time requirements | Define latency, throughput and freshness targets |
| Integrate ML prediction | Train a model, use it on live data (e.g. predict traffic or pollution) |
| Design dashboard / data-serving layer | Show live readings, predictions and alerts to city officials |

The practical part must also cover the 7 standard steps (setup, load, distributed processing, transformations/actions, optimization, results, performance analysis).

## 2. Chosen use case (one clear story)

> **"Predict traffic congestion and air pollution in the next hour and alert the city."**

Traffic and air quality are linked (more cars → worse air), there are good open datasets for both, and it gives a clean ML problem (regression on time series).

## 3. IoT sources (simulated with real datasets)

| Sensor type | Fields | Frequency | Dataset we use |
|---|---|---|---|
| Traffic counter at junction | junction_id, timestamp, vehicles | hourly | **Smart City Traffic Patterns** (Kaggle, 4 junctions, ~48k rows) |
| Air-quality monitor | CO, NOx, NO2, benzene, temp, humidity | hourly | **UCI Air Quality** (Italian city, 9,358 rows, multi-gas sensor) |
| Weather station (live) | temperature, wind, rain | hourly / live | **Open-Meteo API** (free, no key, JSON) — dashboard enrichment only, not a model feature |
| Multi-sensor city feed (optional, advanced) | traffic, pollution, parking, weather | 5 min | **CityPulse / Open Data Aarhus** (Aug–Sep 2014) |

This covers all three load formats required: **CSV** (Kaggle and UCI), **JSON** (Aarhus / saved API responses) and **API** (Open-Meteo).

Dataset links:
- Smart City Traffic Patterns: https://www.kaggle.com/datasets/utathya/smart-city-traffic-patterns
- UCI Air Quality: https://archive.ics.uci.edu/dataset/360/air+quality
- Open-Meteo API: https://open-meteo.com/en/docs (e.g. `https://api.open-meteo.com/v1/forecast?latitude=..&longitude=..&hourly=temperature_2m,windspeed_10m,precipitation`)
- CityPulse Aarhus: http://iot.ee.surrey.ac.uk:8080/datasets.html, and the Mendeley mirror https://data.mendeley.com/datasets/mf35mkghmj/1

## 4. Architecture

```
 IoT sources (simulated)          Ingestion            Processing                      Storage            Serving
 ┌───────────────────────┐     ┌────────────┐    ┌─────────────────────────────┐   ┌──────────────┐   ┌──────────────────┐
 │ Traffic CSV replayer  │──►  │            │    │ Spark Structured Streaming  │   │ Parquet      │   │ Streamlit        │
 │ Air-quality replayer  │──►  │ Kafka      │──► │  • parse + clean            │──►│ (bronze /    │──►│ dashboard        │
 │ Open-Meteo API poller │──►  │ (or file   │    │  • windowed aggregates      │   │  silver /    │   │ • live map/charts│
 └───────────────────────┘     │  stream)   │    │  • stream-stream join (time)│   │  gold)       │   │ • predictions    │
                               └────────────┘    │  • ML model scoring (MLlib) │   └──────────────┘   │ • alerts         │
                                                 │  • threshold alerts         │                      └──────────────────┘
                                                 └─────────────────────────────┘
          Batch side: Spark batch job on historical CSVs → feature engineering → train MLlib model → save model
```

- **Lambda-style design:** a batch layer trains the model on history, and a speed layer scores live data.
- **Kafka is optional.** If installing it is a problem, Spark's **file-stream source** (a replayer script drops small JSON files into a folder) gives the same streaming behaviour with zero setup. Plan: build with the file stream first, add Kafka if time allows.
- **Medallion storage:** bronze = raw, silver = cleaned and joined, gold = aggregates and predictions.

## 5. Real-time requirements (to state in the report)

| Metric | Target |
|---|---|
| End-to-end latency (sensor → dashboard) | < 10 s |
| Micro-batch trigger | 5 s |
| Throughput (simulated) | ≥ 1,000 events/s (replayer can speed up time) |
| Late data tolerance | 10-min watermark |
| Alert latency (pollution / congestion spike) | < 1 micro-batch |
| Fault tolerance | Spark checkpointing → exactly-once to Parquet |

## 6. ML component

- **Problem:** predict next-hour vehicles per junction (regression), plus optionally next-hour CO / NO2.
- **Features:** hour, day-of-week, month, weekend flag, junction, `prev_hour_vehicles`, `same_hour_yesterday`, rolling 3h mean. (No weather features, because the traffic dataset has no known city or coordinates.)
- **Streaming note:** Spark cannot compute `lag()` on a stream, so the **replayer attaches `prev_hour_vehicles` and `same_hour_yesterday` to each event**. Batch training computes the same columns with `Window.lag`, so training and serving use identical features.
- **Models (Spark MLlib):** Linear Regression as the baseline, then **Random Forest / GBT Regressor**.
- **Metrics:** RMSE, MAE, R² on a time-based split (last 20 % as the test set, never a random split).
- **Serving:** load the saved `PipelineModel` inside the streaming job and score every micro-batch.
- **Alert rule:** predicted vehicles > the junction's 90th percentile, OR CO > threshold → "ALERT".

## 7. Mapping to the 7 practical steps

| # | Step | What we do | Evidence (screenshot) |
|---|---|---|---|
| 1 | Setup environment | Python 3.10+, Java 17, PySpark 3.5, (Hadoop winutils on Windows), Jupyter, Streamlit; optional Kafka via Docker | `spark.version`, Spark UI at :4040 |
| 2 | Load dataset | CSV (traffic, air quality) with an explicit schema; JSON (Aarhus / saved API responses); API (Open-Meteo via `requests` → Spark DF) | `printSchema()`, `show()`, `count()` |
| 3 | Distributed processing | Scale data up ×100 (~5M rows, see §12); repartition by `junction_id`; groupBy / window aggregations; join with a junction lookup table | Spark UI stages and tasks |
| 4 | Transformations & actions | Transformations: `filter`, `withColumn`, `window`, `join`, `groupBy`, lag via `Window`. Actions: `count`, `show`, `collect`, `write`. Explain lazy evaluation and the DAG | `explain()` output, DAG screenshot |
| 5 | Optimize performance | `cache()`/`persist()` on the reused feature DF; `repartition` vs `coalesce`; tune `spark.sql.shuffle.partitions` (200 → 8); **broadcast join** for the small weather table; Parquet + partitionBy(date); AQE on | Before/after timings table |
| 6 | Capture results | Aggregates, model metrics, streaming query progress, dashboard screenshots | Screenshots folder |
| 7 | Analyze performance | Compare runtimes (no-cache vs cache, 200 vs 8 shuffle partitions, normal vs broadcast join, CSV vs Parquet); streaming `inputRowsPerSecond` / `processedRowsPerSecond`; model RMSE comparison | Charts and tables in the report |

## 8. Folder structure (to be built)

```
smart-city-iot-platform/
├── PLAN.md                       ← this file
├── README.md                     how to run
├── requirements.txt
├── data/
│   ├── raw/                      downloaded CSV / JSON
│   └── stream_input/             replayer drops files here
├── notebooks/
│   ├── 01_setup_and_load.ipynb           steps 1–2
│   ├── 02_batch_processing.ipynb         steps 3–4
│   ├── 03_optimization.ipynb             step 5 + timings
│   └── 04_ml_training.ipynb              model training
├── src/
│   ├── replayer.py               simulates IoT sensors (CSV → JSON events)
│   ├── weather_api.py            Open-Meteo poller
│   ├── streaming_job.py          Spark Structured Streaming + ML scoring
│   └── dashboard.py              Streamlit app
├── output/                       parquet (bronze/silver/gold), models, checkpoints
├── screenshots/                  evidence for step 6
└── report/
    ├── RollNo_Name_CaseStudy.pdf ← final documentation
    └── RollNo_Name_CaseStudy.pptx← presentation
```

## 9. Work plan

| Phase | Tasks | Output |
|---|---|---|
| 1. Setup & data | Install the stack, download datasets, load and explore | Notebook 01 |
| 2. Batch pipeline | Clean, join, aggregate, explain transformations vs actions | Notebook 02 |
| 3. Optimization | Run each optimization experiment and time it | Notebook 03 + timing table |
| 4. ML | Feature engineering, train LR / RF / GBT, evaluate, save model | Notebook 04 + metrics |
| 5. Streaming | Replayer + streaming job + live scoring + alerts | `src/` scripts |
| 6. Dashboard | Streamlit: live charts, predictions, alert panel | `dashboard.py` + screenshots |
| 7. Docs & slides | Report and PPT, plagiarism-safe (own wording, cite datasets) | `report/` |

## 10. How we hit each evaluation criterion

| Criterion | How we score well |
|---|---|
| **Concept clarity** | Clear architecture diagram; explain streaming vs batch, lambda architecture, watermarking, lazy evaluation and the DAG |
| **Implementation quality** | Modular code in `src/`, explicit schemas, checkpointing, README with run steps, reproducible notebooks |
| **Performance optimization** | Before/after table for cache, partitions, broadcast join, Parquet and AQE, with Spark UI screenshots |
| **Documentation** | Report: problem → sources → architecture → real-time requirements → implementation → results → performance → conclusion and future scope, plus dataset references |
| **Presentation** | 12–15 slide deck: problem, architecture, demo screenshots, ML results, performance gains, conclusion |

## 11. Submission checklist

- [ ] File naming: `RollNo_Name_CaseStudy` (report, PPT and zip of code)
- [ ] Submit before the deadline
- [ ] Original code and wording, with datasets and references cited (no plagiarism)
- [ ] Screenshots of every practical step included

## 14. Decisions needed from the team

1. Roll number and name for file naming.
2. Machine OS (Windows needs `winutils.exe` for Hadoop) — or use Google Colab / Databricks Community.
3. Kafka (more impressive) vs file-stream (simpler) for ingestion.
4. Deadline, so the phases can be dated.

## 12. Plan verification — issues found and fixes

| # | Issue found in v1 of this plan | Fix |
|---|---|---|
| 1 | Traffic data (2015–17, unnamed city) and UCI air data (2004–05, Italy) are from **different places and years**, so joining them on real timestamps produces nothing | Treat them as two independent sensor streams. The replayer **re-stamps both to the current time**, so they arrive together live. ML is trained per stream on its own history. We say this openly in the report as a simulation assumption |
| 2 | 48k rows is too small: cache, partitioning and broadcast tests would show ~0 s differences | Make an **×100 scaled copy** (~5M rows) by duplicating with new junction IDs plus small noise. Optimization timings run on this copy |
| 3 | `lag()` features do not work in Structured Streaming | The replayer sends lag values inside each event (see §6) |
| 4 | UCI file quirks: `;` separator, `,` decimals, `-200` = missing, empty trailing columns | Cleaning step: parse with `sep=';'`, replace `,`→`.`, set `-200` to null, drop empty columns, then forward-fill or drop |
| 5 | Junction 4 only has data from 2017 (fewer rows) | Train per-junction or include junction as a feature; report its metrics separately |
| 6 | Weather API has no matching location for the traffic data | Use it only as a live dashboard panel and to demo **API loading**, not as an ML input |
| 7 | Kafka on Windows is painful | Default to the **file-stream source**; Kafka via Docker is an optional extra |
| 8 | Model accuracy only means something on a time-ordered split | Train on data before a cut-off date and test on data after it; never use a random split |
| 9 | Dashboard must read live results | The streaming job writes the gold layer to Parquet (append); Streamlit re-reads it every 5 s |

## 13. Final implementation flow

1. `notebooks/01` — load traffic CSV and air CSV with explicit schemas, plus the Open-Meteo JSON via `requests` → `spark.createDataFrame`; clean them; write bronze and silver Parquet.
2. `notebooks/02` — transformations (filter, withColumn, Window lag, groupBy, join with a junction lookup) and actions; `explain()`; DAG screenshot.
3. `src/scale_data.py` — ×100 dataset. `notebooks/03` — timing experiments: cache, shuffle partitions, repartition vs coalesce, broadcast join, CSV vs Parquet, AQE on/off. Each result goes in a table and a chart.
4. `notebooks/04` — features → LR / RF / GBT → RMSE, MAE, R² → save the best `PipelineModel`.
5. `src/replayer.py` writes JSON events (with lag fields) into `data/stream_input/` every second. `src/streaming_job.py` reads them with `readStream` and a schema, adds a watermark, computes 5-min window aggregates, scores with the saved model, flags alerts, and writes gold Parquet with checkpointing.
6. `src/dashboard.py` (Streamlit) shows live vehicles vs predicted per junction, air-quality gauges, live weather from the API, and an alert table.
7. Collect screenshots, `lastProgress` metrics (rows/sec, batch duration), the report and the PPT.

