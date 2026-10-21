# Benchmark Results and Operational Metrics

> **Rule:** Every number recorded in this document is produced directly from actual runs of the generator, Glue ETL pipeline, and Athena query execution benchmarks. No numbers are simulated or estimated.

---

## 1. Dataset Dimensions (Pending Milestone 2 & 7)

| Metric | Target / Expected | Measured Actual | Run Date | Evidence Source |
|---|---|---|---|---|
| Total Buildings | 10 (`B01`–`B10`) | TBD | TBD | |
| Total Sensors | 500 (50/bldg) | TBD | TBD | |
| Reading Interval | 10 seconds | TBD | TBD | |
| 1-Day Rows Read | 4,320,000 ± defects | TBD | TBD | |
| 7-Day Rows Read | 30,240,000 ± defects | TBD | TBD | |
| Raw CSV Size (7 days) | ~3 GB (uncompressed) | TBD | TBD | |
| Curated Parquet Size | TBD | TBD | TBD | |
| Injected Defects Rate | ~2% | TBD | TBD | |

---

## 2. Athena Query Performance Benchmark (Pending Milestone 8)

Queries run against `raw_csv` (unpartitioned CSV baseline) vs `readings_curated` / `gold_hourly` (partitioned Snappy Parquet).

| Query ID | Description | Raw CSV Data Scanned | Curated Parquet Data Scanned | Measured Data Reduction % | Raw Execution Time (ms) | Curated Execution Time (ms) | Speedup Ratio |
|---|---|---|---|---|---|---|---|
| **Q1** | Avg temperature for 1 building on 1 day | TBD | TBD | TBD (Acceptance: >= 98.4%) | TBD | TBD | TBD |
| **Q2** | Hourly energy for 1 building over 7 days | TBD | TBD | TBD | TBD | TBD | TBD |
| **Q3** | Count of readings across all buildings on 1 day | TBD | TBD | TBD | TBD | TBD | TBD |

---

## 3. Glue PySpark ETL Performance & Costs (Pending Milestone 5 & 7)

| Run Scope | Glue Version | Worker Type & Count | Execution Duration | DPU-Hours Consumed | Estimated Run Cost (USD) |
|---|---|---|---|---|---|
| 1 Day Manual Run | TBD | 2 x G.1X | TBD | TBD | TBD |
| 7 Days Automated Run | TBD | 2 x G.1X | TBD | TBD | TBD |
