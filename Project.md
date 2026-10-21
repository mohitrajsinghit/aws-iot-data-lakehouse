# Project.md – IoT Building-Sensor Data Lakehouse on AWS

> **Instructions for the AI builder (Claude):** Build this project end to end by following the milestones in Section 12. Work in small steps, run what you build, and show me real output. **Never invent benchmark numbers, row counts, or timings.** Every number that goes into the README or my resume must come from a real run (Section 11). If something in this spec does not work (a service is unavailable, a library conflicts), stop, tell me, and propose the closest honest alternative, and list which resume bullet changes because of it (Section 14). Ask before adding paid services.
>
> **Change log rule:** Every time you implement a change or apply a fix (code, Terraform, config, tests, docs), add an entry to `CHANGELOG.md` in the same step. Each entry needs the date, what changed, why, the files touched, and how it was verified (see Section 18). Do not skip this for small fixes.

---

## 1. What we are building

A small but realistic **data lakehouse pipeline for smart-building IoT telemetry** on AWS.

- A Python generator simulates sensor readings from **10 buildings and 500 sensors**, one reading every **10 seconds** per sensor (about **4.32 million rows per day**, **30.24 million rows for 7 days**).
- Raw CSV files land in **Amazon S3**. A manifest file triggers **AWS Lambda**, which starts an **AWS Glue PySpark** job.
- The job **validates** the data with 5 quality checks, **quarantines** bad rows, **deduplicates**, and writes **partitioned Parquet** (curated layer) plus **1-minute and hourly aggregates** (gold layer).
- **Amazon Athena** queries the data through the **AWS Glue Data Catalog**.
- All AWS resources are defined in **Terraform**. **GitHub Actions** runs lint, tests, and Terraform checks on every pull request and deploys on merge.
- A benchmark script measures **how much less data Athena scans** on the partitioned Parquet tables versus the raw CSV table.

## 2. Why we are building it (goals)

This is a portfolio project for a **Data Engineer** application (Siemens, smart-buildings domain). It should show, with real evidence:

| Job requirement | How this project shows it |
|---|---|
| AWS-native pipelines (S3, Glue, Athena, Lambda) | The whole pipeline runs on these services |
| PySpark | Glue job does cleaning, dedupe, aggregation |
| Data quality | 5 checks, quarantine zone, run metrics |
| Infrastructure as Code | Terraform for every resource |
| CI/CD | GitHub Actions: lint, test, plan, deploy |
| Query performance and cost | Partitioning + Parquet, measured with Athena stats |
| IoT / time-series / smart buildings | Simulated building telemetry with rollups |
| Metadata and catalog | Glue Data Catalog tables with partition projection |

**Non-goals:** real-time streaming, dashboards, ML models, multi-account setups, a web UI. Keep it focused.

## 3. Constraints

- **Cost must stay near zero.** Target under US$5 total. Use the AWS Free plan where possible, set a budget alarm, and tear everything down after the demo (`terraform destroy`).
- Region: `ap-south-1` (Mumbai).
- The repo must be **public-safe**: no secrets, no account IDs hard-coded, `.gitignore` for state files and data.
- Everything must be **reproducible from the README** by someone else.
- If **AWS Glue is not available** on the account plan, use the fallback in Section 13.

## 4. Architecture

```
 ┌──────────────────┐   CSV files    ┌─────────────────────────────┐
 │ Python generator │ ─────────────► │ S3: raw/                    │
 │ (seeded, 7 days) │                │  building_id=B01/date=.../  │
 └──────────────────┘                └──────────────┬──────────────┘
          │ writes manifest when a day is complete  │
          ▼                                          │
 ┌─────────────────────────┐   S3 event   ┌─────────▼─────────┐
 │ S3: raw/_manifests/     │ ───────────► │ AWS Lambda        │
 │  date=YYYY-MM-DD.json   │              │ (start Glue run)  │
 └─────────────────────────┘              └─────────┬─────────┘
                                                    │ StartJobRun(date=...)
                                          ┌─────────▼─────────┐
                                          │ AWS Glue (PySpark)│
                                          │ 1 read + cast     │
                                          │ 2 5 quality checks│
                                          │ 3 dedupe          │
                                          │ 4 aggregate       │
                                          └───┬─────┬─────┬───┘
                           ┌──────────────────┘     │     └──────────────┐
                           ▼                        ▼                    ▼
              S3: quarantine/ (bad rows)   S3: curated/readings/   S3: gold/minute, gold/hourly
                                           (Parquet, partitioned)   (Parquet, partitioned)
                                                    │
                                          ┌─────────▼─────────┐
                                          │ Glue Data Catalog │
                                          │ db: building_iot  │
                                          └─────────┬─────────┘
                                                    ▼
                                          ┌───────────────────┐
                                          │ Amazon Athena     │
                                          │ (workgroup + cap) │
                                          └───────────────────┘
```

Layers (medallion naming): **raw = Bronze**, **curated = Silver**, **gold = Gold**.

## 5. Tech stack

- Python 3.11, PySpark (Glue version 4.0 unless there is a reason to change it), boto3, pytest, ruff
- AWS: S3, Lambda, Glue (job + Data Catalog), Athena, IAM, CloudWatch Logs, AWS Budgets
- Great Expectations (pinned version compatible with the Glue Python and Spark versions)
- Terraform (AWS provider, pinned versions)
- GitHub Actions

## 6. Repository structure

```
building-iot-lakehouse/
├─ README.md                  # requirements, architecture, results, problems faced, runbook
├─ Project.md                 # this spec (optional to keep)
├─ CHANGELOG.md               # running log of every change and fix (see Section 18)
├─ .gitignore
├─ Makefile                   # make generate | test | lint | tf-plan | benchmark
├─ generator/
│  ├─ generate.py             # seeded sensor data generator (CLI)
│  └─ config.py               # sensors, buildings, ranges, defect rates
├─ glue/
│  ├─ job.py                  # PySpark job (entry point)
│  ├─ quality.py              # the 5 checks
│  └─ transforms.py           # cast, dedupe, aggregate (pure functions, unit-testable)
├─ lambda/
│  └─ start_glue.py           # reads manifest event, starts Glue run
├─ athena/
│  ├─ queries/q1_..sql, q2_..sql, q3_..sql
│  └─ benchmark.py            # runs queries, records bytes scanned and timings
├─ terraform/
│  ├─ main.tf, variables.tf, outputs.tf, versions.tf
│  ├─ s3.tf, iam.tf, lambda.tf, glue.tf, athena.tf, budget.tf
├─ tests/
│  ├─ test_generator.py, test_quality.py, test_transforms.py
├─ docs/
│  ├─ results.md              # benchmark tables (real numbers only)
│  ├─ problems.md             # problems faced log (fill while building)
│  └─ architecture.png        # optional
└─ .github/workflows/
   ├─ ci.yml                  # on pull_request
   └─ deploy.yml              # on push to main (manual approval)
```

## 7. Data design

### 7.1 Simulation parameters (defaults, all configurable)

| Parameter | Value |
|---|---|
| Buildings | 10 (`B01`…`B10`) |
| Sensors | 500 total, 50 per building (`S0001`…`S0500`), spread over 5 floors per building |
| Reading interval | 10 seconds |
| Rows per day | 500 × 8,640 = **4,320,000** |
| Days | 7 (default start date `2026-10-01`, configurable) |
| Rows for 7 days | **30,240,000** |
| Random seed | fixed (e.g. 42) so runs are reproducible |

### 7.2 Raw schema (CSV with header)

| Column | Type | Notes |
|---|---|---|
| `event_time` | timestamp (UTC, ISO 8601) | sensor timestamp |
| `building_id` | string | `B01`…`B10` |
| `floor` | int | 1–5 |
| `sensor_id` | string | `S0001`…`S0500` |
| `temperature_c` | double | realistic daily curve, normal range 18–28 |
| `energy_kwh` | double | rises in working hours, normal range 0–5 per interval |
| `co2_ppm` | double | follows occupancy, normal range 400–1500 |
| `occupancy_count` | int | 0 at night, higher in working hours |

Generate smooth, plausible time series (daily pattern plus small noise), not pure random numbers.

### 7.3 Injected defects (so the quality checks have something to catch)

| Defect | Rate | Caught by check |
|---|---|---|
| Exact duplicate rows | 1.5% | Check 4 (duplicates) |
| Late / out-of-order rows (event written in a later file of the same day) | 0.5% | Handled by dedupe and sorting, not rejected |
| Null reading values | 0.5% | Check 2 (nulls) |
| Out-of-range values (for example temperature 95 °C, negative energy) | 0.3% | Check 3 (value ranges) |
| Bad timestamps (future time, or date outside the partition date) | 0.2% | Check 5 (timestamps) |

The duplicate plus late rate is about **2%**, which the resume mentions. The generator must write a **ground-truth file** (`manifest` with counts of each injected defect) so we can verify the job caught what was injected.

### 7.4 File layout in S3 (single bucket, prefixes)

```
raw/building_id=B01/date=2026-10-01/readings_00.csv ... readings_23.csv   # hourly files
raw/_manifests/date=2026-10-01.json        # written LAST, triggers the pipeline
curated/readings/building_id=B01/date=2026-10-01/part-*.parquet
gold/minute/building_id=.../date=.../part-*.parquet
gold/hourly/building_id=.../date=.../part-*.parquet
quarantine/date=2026-10-01/part-*.parquet  # rows failing checks, with a reason column
metrics/run_date=2026-10-01/summary.json   # run statistics
athena-results/                            # query results (lifecycle: expire in 7 days)
glue-scripts/                              # job.py and dependencies
```

Raw CSV is **uncompressed** on purpose, so the baseline for the benchmark is honest. If you decide to compress it, state that wherever the benchmark is quoted.

Expected size: about 100 bytes per row, roughly 430 MB per day and **about 3 GB for 7 days**. Measure the real size and report it.

## 8. Pipeline details

### 8.1 Generator (`generator/generate.py`)
- CLI: `python generate.py --start-date 2026-10-01 --days 7 --out ./data --seed 42`
- Writes hourly CSV files per building and date into the raw layout above, injects defects per 7.3, writes the ground-truth counts, and finally writes the day's **manifest JSON** (`date`, `files`, `row_count`, `expected_defects`).
- Separate `upload.py` (or `aws s3 sync`) uploads files, with the **manifest uploaded last** so the trigger fires only when the day is complete.

### 8.2 Lambda (`lambda/start_glue.py`)
- Trigger: S3 `ObjectCreated` on prefix `raw/_manifests/`, suffix `.json`.
- Reads the date from the key, calls `glue.start_job_run` with `--run_date`, logs the run ID. Idempotent: if a run for the same date is already running, do nothing.
- Least-privilege IAM: `glue:StartJobRun`, `glue:GetJobRuns`, logs.

### 8.3 Glue job (`glue/job.py`)
Arguments: `--run_date`, `--bucket`. Steps:
1. **Read** the day's raw CSV with an **explicit string schema** (no schema inference), then cast to proper types. Cast failures are treated as bad rows, not job failures.
2. **Quality checks** (Section 8.4). Tag each row `CLEAN` or `FAILED` with a `failure_reason`. Write `FAILED` rows to `quarantine/`.
3. **Deduplicate** on `(sensor_id, event_time)`, keeping one row. Count removed duplicates.
4. **Write curated** Parquet (snappy), partitioned by `building_id` and `date`, using dynamic partition overwrite so reruns for one date are **idempotent**.
5. **Aggregate to gold**: per building, 1-minute and hourly averages (temperature, CO2, occupancy), sums (energy), reading counts. Write partitioned Parquet.
6. **Write run metrics** JSON: rows read, rows clean, rows quarantined by reason, duplicates removed, output row counts, duration.
- Worker config: smallest that works (G.1X, 2 workers), job timeout 30 minutes, no Glue crawlers.

### 8.4 The five quality checks (`glue/quality.py`)
1. **Schema**: required columns present, types castable.
2. **Nulls**: `event_time`, `building_id`, `sensor_id` and all four readings are non-null.
3. **Value ranges**: temperature −10 to 50 °C, energy ≥ 0, CO2 300–5000 ppm, occupancy ≥ 0.
4. **Duplicates**: no repeated `(sensor_id, event_time)` (duplicates are counted and removed).
5. **Timestamps**: not in the future, and the date matches the partition date.

Implement these with **Great Expectations** against the Spark DataFrame. If Great Expectations does not run reliably on Glue after a fair attempt, implement the same five checks as plain PySpark assertions and **tell me**, because the resume bullet must then drop the words "Great Expectations".

### 8.5 Glue Data Catalog and Athena
- Database `building_iot`. Define tables in **Terraform** (no crawlers):
  - `raw_csv` – external CSV table over `raw/` (**unpartitioned**, header skipped). This is the baseline.
  - `readings_curated` – Parquet, partitioned by `building_id` and `date`, using **partition projection** (`building_id` enum B01–B10, `date` date range) so no `MSCK REPAIR` is needed.
  - `gold_minute`, `gold_hourly` – same pattern.
  - Optional ablation: `readings_curated_flat` – the same Parquet **without** partitions, to isolate what partitioning adds beyond the file format.
- Athena **workgroup** with an enforced results location and a **per-query data-scanned limit** (for example 10 GB) as a cost guard.

### 8.6 Terraform (`terraform/`)
Creates: S3 bucket (block public access, encryption, versioning off, lifecycle rules), S3 notification, Lambda, Glue job and script upload, Glue database and tables, Athena workgroup, IAM roles (least privilege, separate roles for Lambda and Glue), CloudWatch log groups (7-day retention), an **AWS Budget** with an email alert at US$5. Remote state is optional; local state is fine for a personal project (git-ignore it). Variables for region, bucket name suffix, and alert email.

### 8.7 CI/CD (`.github/workflows/`)
- **ci.yml** (pull requests): `ruff` lint, `pytest` (generator, quality checks, transforms using local PySpark on a tiny sample), `terraform fmt -check`, `terraform validate`, and `terraform plan` if AWS credentials are available.
- **deploy.yml** (push to `main`): `terraform apply` and upload of the Glue script, behind a manual-approval GitHub Environment.
- AWS access from GitHub via **OIDC** and an IAM role (no long-lived keys in secrets). If OIDC setup is too heavy, say so and use `validate` only in CI, and the resume bullet will say CI runs lint, tests, and Terraform validation.

## 9. Testing

- **Unit tests**: generator is deterministic for a seed and injects the configured defect rates (within tolerance); each of the 5 checks catches its defect and passes clean rows; dedupe keeps exactly one row; aggregates are correct on a hand-computed example.
- **Reconciliation test** (end to end, 1 day): `rows_read = rows_clean + rows_quarantined + duplicates_removed` (adjust to your exact accounting) and the quarantine counts match the generator's ground-truth defect counts within a tight tolerance.
- **Idempotency test**: running the job twice for the same date produces identical curated row counts.

## 10. Observability and cost control

- Structured JSON logs from Lambda and Glue.
- `metrics/run_date=.../summary.json` per run (used for the README numbers).
- Budget alert at US$5, Athena workgroup scan limit, Glue timeout, no crawlers, log retention 7 days.
- After the demo: `terraform destroy`, then verify in the console that the bucket and Glue resources are gone.

## 11. Benchmark – how the "data scanned" number is produced

Run the **same queries** against `raw_csv` and `readings_curated` (and optionally `readings_curated_flat`), using `athena/benchmark.py` (boto3 `get_query_execution` → `Statistics.DataScannedInBytes` and `EngineExecutionTimeInMillis`). Run each query **3 times** and record all runs.

| Query | SQL idea |
|---|---|
| **Q1** | Average `temperature_c` for **one building on one day** |
| **Q2** | Hourly `energy_kwh` for **one building over 7 days** (use `gold_hourly` for the curated side) |
| **Q3** | Count of readings for **all buildings on one day** |

Compute for each query: `reduction % = (csv_bytes − curated_bytes) / csv_bytes × 100`.

- Partition pruning alone should scan about 1 of 70 partitions for Q1 (≈1.4% of the data, i.e. ≈98.6% reduction); Parquet column pruning and compression should push the result higher. These are **expectations, not results**.
- The **resume currently claims 98.4%** for Q1. The acceptance rule is: the measured Q1 reduction must be **≥ 98.4%**. If it is lower, **do not adjust the test to fit** – report the real number and tell me so I can update the resume.
- Put all numbers, dataset size, and the date of the run in `docs/results.md`, with a screenshot or exported query statistics for evidence.

## 12. Milestones (build order)

1. **Repo and tooling**: repo skeleton, `.gitignore`, `Makefile`, ruff and pytest config. *Done when:* `make lint test` runs (even with few tests).
2. **Generator**: build, unit-test, generate 1 day locally. *Done when:* row count is 4,320,000 ± defects and the ground-truth file matches the config.
3. **Transforms and checks locally**: `quality.py`, `transforms.py`, run on 1 local day with local PySpark; unit and reconciliation tests pass. *Done when:* quarantine counts match ground truth.
4. **Terraform base**: S3, IAM, Glue database, Athena workgroup, budget. `terraform plan` clean, then apply. *Done when:* bucket exists and budget alert is active.
5. **Upload and Glue job**: upload 1 day, run the Glue job manually, check curated and gold output in S3. *Done when:* Athena can query `readings_curated`.
6. **Lambda trigger**: manifest upload starts the Glue run automatically. *Done when:* uploading a manifest produces a successful Glue run with no manual step.
7. **Scale to 7 days**: generate and upload all days, let the pipeline run. Record Glue run time, DPU usage, and cost from the console.
8. **Benchmark**: create `raw_csv` (and optional flat table), run `benchmark.py`, fill `docs/results.md`.
9. **CI/CD**: `ci.yml`, then `deploy.yml`. *Done when:* a pull request shows green checks and a merge deploys.
10. **README and problems log**: finish the README (Section 15) and `docs/problems.md`.
11. **Teardown**: `terraform destroy` after screenshots and results are saved.

## 13. Fallback if Glue (or another service) is unavailable

- **Glue unavailable on the account plan:** run the same `job.py` logic locally with PySpark (Docker, Spark 3.5), write Parquet locally, upload it to S3, and keep Athena, the Glue Data Catalog tables, Terraform, and CI/CD on AWS. If Athena or S3 are also restricted, run fully local (MinIO or LocalStack for S3, Terraform against LocalStack) and say "local" in all wording.
- **Great Expectations fails on Glue:** use PySpark assertions with the same five checks.
- **OIDC too heavy:** CI uses `terraform validate` only.

Whichever fallback is used, update the resume wording using Section 14.

## 14. Resume truth table (what we may claim, and the evidence)

| Resume claim | Evidence required | If not true, say instead |
|---|---|---|
| 500 sensors, 10 buildings, 10-second readings, ~4.3M readings/day | Generator config and a real 1-day file | Use the real numbers |
| About 2% duplicate and late records injected | Ground-truth manifest | Use the real injected rate |
| S3 → Lambda trigger → Glue PySpark job | Lambda log showing the Glue run it started | "Glue job started on a schedule/manually" |
| 5 Great Expectations checks | Check suite running in the job | "5 PySpark data-quality checks" |
| 1-minute and hourly rollups as partitioned Parquet | Objects in `gold/` | Use what exists |
| Athena through the Glue Data Catalog, X% less data scanned | `docs/results.md` with query statistics | Use the measured value |
| Terraform for all AWS resources | `terraform apply` from this repo | "Terraform for core resources" |
| GitHub Actions lint, tests, plan on PR, deploy on merge | Green workflow runs | Describe the workflows that actually exist |

Also useful for the resume once measured: total rows processed, Glue run time for 7 days, quarantine rate, and total cost.

## 15. README requirements (the project write-up)

1. **Problem and goals** (short, from Sections 1–2)
2. **Requirements** (functional and non-functional, from Sections 7–10)
3. **Architecture** (diagram + one paragraph per component)
4. **Design decisions and trade-offs** (why Parquet, why partition by building and date, why partition projection instead of crawlers, why a manifest trigger, why idempotent overwrite)
5. **Data quality** (the 5 checks, injected vs caught counts)
6. **Results** (benchmark table, processing time, cost, row counts – real numbers only)
7. **Problems faced and how they were solved** – written from `docs/problems.md`, which must be filled **as problems actually happen** during the build (symptom, cause, fix)
8. **How to run** (prerequisites, deploy, generate, upload, benchmark)
9. **Teardown** and cost notes
10. **Future improvements** (streaming ingestion with Kinesis/Firehose, Iceberg tables, a Power BI or QuickSight dashboard, alerting)

## 16. Definition of done

- [ ] 7 days of data processed end to end with no manual step after the manifest upload
- [ ] Reconciliation test passes and quarantine counts match ground truth
- [ ] Re-running a date gives identical output (idempotent)
- [ ] Benchmark table with real numbers and evidence saved
- [ ] CI green on a pull request; deploy works on merge (or the documented fallback)
- [ ] README complete, problems log filled with real problems
- [ ] `CHANGELOG.md` has an entry for every change and fix made during the build
- [ ] Resume bullets match Section 14
- [ ] Everything destroyed with `terraform destroy` after the demo, cost recorded

## 17. Interview preparation (add to `docs/interview.md` after building)

Be ready to explain: why partition by `building_id` and `date`; partition projection vs crawlers; why raw data is read as strings first; how the job stays idempotent; how duplicates and late data are handled; what each quality check does and what happens to failed rows; how Athena charges by data scanned and why Parquet reduces it; why the manifest-last pattern is used; IAM least privilege; what you would change for real-time data (streaming, Iceberg/Delta, schema evolution).

## 18. Change log (`CHANGELOG.md`)

Keep a running log at the repo root so we can see later what was done and why. `docs/problems.md` is for problems and their root causes; `CHANGELOG.md` is for **every change and fix**, including routine ones. Newest entry on top. Use this format:

```
## 2026-10-21 – Short title of the change
- Type: feature | fix | refactor | config | infra | test | docs
- What changed: one or two sentences
- Why: the reason or the problem it solves (link to docs/problems.md if relevant)
- Files: path/one.py, terraform/glue.tf
- Verified by: the command run or test passed, with the result
- Impact: anything that affects the README, the benchmark, or a resume bullet (Section 14)
```

Rules:
- Add the entry in the **same step** as the change, not at the end.
- If a change alters a number in `docs/results.md` or a resume bullet, say so under **Impact**.
- Never edit old entries. If a change is reversed, add a new entry that says so.
- Commit messages should match the entry title.
