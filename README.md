# IoT Building-Sensor Data Lakehouse on AWS

A realistic, cost-controlled **smart-building IoT telemetry data lakehouse pipeline** deployed on AWS.

## Architecture Overview

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

- **Bronze (Raw)**: Partitioned hourly uncompressed CSV files landing in Amazon S3.
- **Silver (Curated)**: Cleaned, validated, deduplicated Parquet files partitioned by `building_id` and `date`.
- **Gold (Aggregated)**: 1-minute and hourly sensor rollups (temperature, energy, CO2, occupancy).
- **Quarantine Zone**: Rows failing quality checks preserved with error diagnostics.
- **Infrastructure as Code**: Terraform with AWS Budget cap ($5.00 limit).

## Repository Structure

```
building-iot-lakehouse/
├── .github/workflows/        # CI/CD workflows (lint, tests, terraform validate/deploy)
├── athena/                   # Benchmark scripts and query SQL definitions
├── docs/                     # Problems log and empirical benchmark results
├── generator/                # Seeded synthetic sensor telemetry generator
├── glue/                     # PySpark job, transforms, and quality checks
├── lambda/                   # S3 event manifest trigger handler
├── terraform/                # Infrastructure as Code definitions
├── tests/                    # Unit, reconciliation, and idempotency tests
├── CHANGELOG.md              # Historical log of all changes and verifications
├── Makefile                  # Developer workflow automation
└── pyproject.toml            # Tooling and dependency configuration
```

## Quickstart

### Prerequisites
- Python 3.11+
- GNU Make
- AWS CLI & Terraform (for AWS deployment)

### Developer Commands
```bash
# Run code linting and format checks
make lint

# Run unit tests
make test
```
