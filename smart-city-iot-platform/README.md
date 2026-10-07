# Smart City IoT Data Platform — Case Study 6

**Roll No:** `<ROLL_NO>`  **Name:** `<NAME>`  **Syllabus:** 6.1, 6.3 — Real-time pipelines, ML systems

A Spark-based platform. It ingests city sensor data (traffic counters, air-quality monitors, weather) and processes it in batch and in real time. Two ML models produce next-hour traffic forecasts and CO estimates, and alerts and live metrics are served on a dashboard.

```
 IoT sources                 Ingestion                 Processing (PySpark 3.5)                      Serving
 ┌──────────────────────┐   ┌──────────────────┐   ┌──────────────────────────────────────────┐   ┌───────────────────┐
 │ traffic CSV  (48k)   │   │ replayer.py      │   │ BATCH  bronze → silver → gold (Parquet)  │   │ Streamlit         │
 │ air CSV      (9.4k)  │──►│ JSON events into │──►│        features, MLlib training          │──►│ dashboard.py      │
 │ Open-Meteo API (JSON)│   │ data/stream_input│   │ STREAM Structured Streaming, 5 s trigger │   │ forecasts, alerts │
 └──────────────────────┘   └──────────────────┘   │        model scoring, windows, join      │   │ latency, weather  │
                                                   └──────────────────────────────────────────┘   └───────────────────┘
```

## What is where

| Path | Purpose | Practical step |
|---|---|---|
| `setup_windows.ps1` | one-time Windows setup (Java check, winutils, venv, smoke test) | 1 |
| `src/config.py` | every path and constant | – |
| `src/ingest.py` | load CSV / CSV / API→JSON with explicit schemas, cleaning, quality reports | 2 |
| `src/batch_pipeline.py` | raw → bronze → silver → gold | 2–4 |
| `src/features.py` | lag/time features (Spark for training, pandas for the live sensors) | 4 |
| `src/train_models.py` | traffic forecaster + air soft sensor, time-based validation | ML |
| `src/scale_data.py`, `src/benchmark.py` | ×100 data copy and 6 optimization experiments | 5, 7 |
| `src/replayer.py` | simulates the IoT sensors as a live JSON stream | streaming |
| `src/streaming_job.py` | 4 Structured Streaming queries | streaming |
| `src/dashboard.py` | live dashboard (data-serving layer) | 6 |
| `notebooks/01…06` | step-by-step walkthrough with outputs for screenshots | 1–7 |
| `tests/test_pipeline.py` | data-handling tests, incl. train/serve feature parity | quality |
| `PLAN.md` | design, requirements, decisions | docs |

## Windows setup (once)

Prerequisites (install these first):
1. **Python 3.11**: https://www.python.org/downloads/release/python-3119/ (tick *Add python.exe to PATH*). PySpark 3.5 does not work reliably with Python 3.12+ on Windows.
2. **Java 17 (Temurin JDK)**: https://adoptium.net/temurin/releases/?version=17 (tick *Set JAVA_HOME*).
3. Put the project in a folder **without spaces**, e.g. `C:\projects\smart-city-iot-platform`.

Then, in PowerShell from the project folder:
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\setup_windows.ps1
```
The script downloads `winutils.exe` and `hadoop.dll` (Spark needs them to write files on Windows) and sets `HADOOP_HOME`. It then creates `.venv`, installs `requirements.txt`, and runs a Spark smoke test. **Open a new PowerShell window afterwards** so the new environment variables apply, then activate the venv:
```powershell
.\.venv\Scripts\Activate.ps1
```

## Run order

```powershell
python -m pytest -q                      # 5 data-handling tests, ~30 s
jupyter notebook                         # then run notebooks 01 → 04 in order
```
| Notebook | Step | Time |
|---|---|---|
| `01_setup_and_load` | 1 Setup, 2 Load (CSV / JSON / API), writes bronze/silver/gold | ~1 min |
| `02_distributed_processing` | 3 Distributed processing, 4 Transformations & actions | ~1 min |
| `03_performance_optimization` | 5 Optimize (cache, partitions, shuffle, broadcast, Parquet, AQE) | 5–15 min |
| `04_ml_models` | ML training + evaluation, saves the models | 5–10 min |

**Live pipeline** (3 PowerShell windows, each with the venv activated):
```powershell
# window 1: start the streaming job and wait until it prints "4 streaming queries running"
python -m src.streaming_job --reset
# window 2: start the sensors (1 hour of sensor data per second)
python -m src.replayer --reset --hours-per-tick 1 --interval 1
# window 3: dashboard → http://localhost:8501
streamlit run src/dashboard.py
```
After 3–5 minutes, stop windows 1 and 2 with Ctrl+C, then run notebooks `05_streaming_results` (Step 6) and `06_performance_analysis` (Step 7).

Each notebook has 📸 markers that say exactly what to screenshot. Save the screenshots in `screenshots/`.

## How the data is handled

| Problem | Handling |
|---|---|
| Type safety | explicit schemas for every source; no `inferSchema` |
| Traffic: duplicates, nulls, gaps | checked and reported in `output/data_quality_report.json` (none found; 4 junctions × hourly, no gaps) |
| Air CSV: `;` separator, `2,6` decimals, `-200` = missing, 114 blank lines, `;;` empty columns | parsed as text, `,`→`.`, `-200`→null, blank rows and empty columns dropped |
| Air `NMHC(GT)` 90% missing | column dropped |
| Remaining missing sensor values | median `Imputer` inside the ML pipeline, fitted on training data only |
| Lag features with possible gaps | lags matched by timestamp (a missing hour gives null, never the wrong row) |
| Train/serve skew | live features (pandas) are tested to equal the training features (Spark) on all 48k rows |
| Time-series leakage | time-based train / validation / test split; models chosen on validation, test used once |
| Datasets from different cities/years | streams aligned on one clock (air shifted by whole weeks); stated as a simulation assumption |
| Timezones | Spark session pinned to UTC, so nothing shifts by IST/DST |
| Partial files in the stream | replayer writes to a staging folder and renames atomically |
| Duplicate output after a crash | `foreachBatch` writes to `batch_id=N` with overwrite + checkpoints → exactly-once |

## Troubleshooting (Windows)

| Error | Fix |
|---|---|
| `HADOOP_HOME is not set` / `UnsatisfiedLinkError ... NativeIO$Windows` | re-run `setup_windows.ps1`, then open a **new** terminal |
| `Java gateway process exited before sending its port number` | Java not installed or `JAVA_HOME` wrong; install Temurin 17 |
| `Python worker failed to connect back` | use the venv's Python (`.venv\Scripts\Activate.ps1`); `get_spark()` already sets `PYSPARK_PYTHON` |
| Out of memory in notebook 3 | set `FACTOR = 50` |
| Dashboard empty | the streaming job must be running *before* the replayer; check window 1 for errors |
| Weather panel shows a warning | no internet; everything else still works (the batch side uses the last saved API response) |

## Submission

* File naming: `RollNo_Name_CaseStudy` (e.g. report `RollNo_Name_CaseStudy.pdf`, slides `RollNo_Name_CaseStudy.pptx`, code `RollNo_Name_CaseStudy.zip`). Replace the placeholders `<ROLL_NO>` and `<NAME>` in this README and in each notebook's header.
* Datasets: Smart City Traffic Patterns (Kaggle), UCI Air Quality (De Vito et al., 2008), Open-Meteo API. Cite them in the report.
