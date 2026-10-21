# Benchmark Results and Operational Metrics

> **Rule:** Every number recorded in this document is produced directly from actual runs of the generator, Glue ETL pipeline, and Athena query execution benchmarks. No numbers are simulated or estimated.

---

## 1. Dataset Dimensions (Pending Milestone 2 & 7)

| Metric | Target / Expected | Measured Actual | Run Date | Evidence Source |
|---|---|---|---|---|
| Total Buildings | 10 (`B01`–`B10`) | 10 (`B01`–`B10`) | 2026-10-21 | `generator/config.py` |
| Total Sensors | 500 (50/bldg) | 500 (50/bldg) | 2026-10-21 | `build_sensor_topology()` |
| Reading Interval | 10 seconds | 10 seconds | 2026-10-21 | 8,640 intervals/day |
| 1-Day Rows (Base) | 4,320,000 | 4,320,000 | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| 1-Day Rows Written (Total) | 4,320,000 ± defects | 4,385,219 | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| 1-Day Raw CSV Size | ~430 MB (uncompressed) | 227.65 MB (238,709,475 bytes) | 2026-10-21 | Local disk measurement |
| 7-Day Rows Read | 30,240,000 ± defects | TBD (Milestone 7) | TBD | |
| Raw CSV Size (7 days) | ~3 GB (uncompressed) | TBD (Milestone 7) | TBD | |
| Curated Parquet Size | TBD | 39.33 MB (41,244,178 bytes) | 2026-10-21 | Local disk measurement |
| Exact Duplicates Injected | 1.5% (~64,800) | 65,219 (1.51%) | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| Late Records Injected | 0.5% (~21,600) | 20,521 (0.47%) | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| Combined Duplicate + Late | ~2.0% (~86,400) | 85,740 (1.98%) | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| Null Values Injected | 0.5% (~21,600) | 22,336 (0.52%) | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| Out-of-Range Injected | 0.3% (~12,960) | 12,948 (0.30%) | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| Bad Timestamps Injected | 0.2% (~8,640) | 8,794 (0.20%) | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |
| Total Defect Rows | ~3.0% (~129,600) | 130,018 (3.01%) | 2026-10-21 | `data/raw/_manifests/date=2026-10-01.json` |

---

## 2. Local PySpark 1-Day ETL Run & Reconciliation (Milestone 3)

Run on 1 full day (`2026-10-01`) using `glue/job.py`:

| Pipeline Stage | Ground Truth Expected | Measured Actual | Match Status | Notes |
|---|---|---|---|---|
| Raw Rows Read | 4,385,219 | 4,385,219 | **EXACT MATCH** | 4,320,000 base + 65,219 duplicate copies |
| Clean Curated Rows | 4,275,922 | 4,275,922 | **EXACT MATCH** | Written to `curated/readings/` as Snappy Parquet |
| Quarantined Rows | 44,078 | 44,078 | **EXACT MATCH** | `null_values` (22,336) + `out_of_range` (12,948) + `bad_timestamps` (8,794) |
| Duplicates Removed | 65,219 | 65,219 | **EXACT MATCH** | Deduplicated on `(sensor_id, event_time)` |
| Gold 1-Minute Rows | 14,400 | 14,400 | **EXACT MATCH** | 10 buildings × 1,440 minutes/day |
| Gold 1-Hour Rows | 240 | 240 | **EXACT MATCH** | 10 buildings × 24 hours/day |
| **Reconciliation Identity** | `4,275,922 + 44,078 + 65,219` | **4,385,219** | **BALANCED** | $rows_{read} = rows_{clean} + rows_{quarantine} + rows_{deduped}$ |
| Execution Duration | N/A | 143.87 seconds | Completed | 12 local worker threads on Windows |

---

## 3. Athena Query Performance Benchmark (Pending Milestone 8)

Queries run against `raw_csv` (unpartitioned CSV baseline) vs `readings_curated` / `gold_hourly` (partitioned Snappy Parquet).

| Query ID | Description | Raw CSV Data Scanned | Curated Parquet Data Scanned | Measured Data Reduction % | Raw Execution Time (ms) | Curated Execution Time (ms) | Speedup Ratio |
|---|---|---|---|---|---|---|---|
| **Q1** | Avg temperature for 1 building on 1 day | TBD | TBD | TBD (Acceptance: >= 98.4%) | TBD | TBD | TBD |
| **Q2** | Hourly energy for 1 building over 7 days | TBD | TBD | TBD | TBD | TBD | TBD |
| **Q3** | Count of readings across all buildings on 1 day | TBD | TBD | TBD | TBD | TBD | TBD |

---

## 4. Glue PySpark ETL Performance & Costs (Pending Milestone 5 & 7)

| Run Scope | Glue Version | Worker Type & Count | Execution Duration | DPU-Hours Consumed | Estimated Run Cost (USD) |
|---|---|---|---|---|---|
| 1 Day Manual Run | TBD | 2 x G.1X | TBD | TBD | TBD |
| 7 Days Automated Run | TBD | 2 x G.1X | TBD | TBD | TBD |
