# Changelog

All notable changes to this project are documented in this file.
Format follows the specification in Project.md Section 18. Newest entries on top.

---

## 2026-10-21 – Milestone 2: Build seeded IoT sensor telemetry generator and run 1-day simulation
- Type: feature
- What changed: Built `generator/config.py` defining simulation topology (10 buildings, 500 sensors, 5 floors, 10s intervals) and defect rates (1.5% duplicates, 0.5% late, 0.5% nulls, 0.3% out-of-range, 0.2% bad timestamps). Implemented `generator/generate.py` with diurnal physics calculations, hourly partitioned CSV emission, and manifest JSON generation. Created unit test suite in `tests/test_generator.py` and updated `docs/results.md` with measured 1-day metrics.
- Why: Implements Milestone 2 data generation producing ground-truth raw telemetry for the downstream Glue PySpark ETL and Athena query benchmarks.
- Files: generator/__init__.py, generator/config.py, generator/generate.py, tests/test_generator.py, pyproject.toml, docs/results.md, CHANGELOG.md
- Verified by: Ran `make lint` (all 11 files clean) and `make test` (6/6 tests passed). Generated 1 full day (`2026-10-01`) producing 240 hourly CSV files totaling 4,385,219 rows (4,320,000 base + 65,219 duplicates) and 227.65 MB, matching ground-truth manifest `data/raw/_manifests/date=2026-10-01.json`.
- Impact: Validates Section 14 resume claims for 10 buildings, 500 sensors, 10-second intervals (~4.3M rows/day), and 1.98% injected duplicate and late records.

---

## 2026-10-21 – Standardize project and resource naming to aws-iot-data-lakehouse
- Type: config
- What changed: Standardized repository name, directory reference, documentation, and AWS resource name prefixes to `aws-iot-data-lakehouse` (replacing legacy `building-iot-lakehouse` references). Configured AWS profile `iot-lakehouse` targeting region `ap-south-1`.
- Why: User requested alignment of repository name, folder, README, and AWS resource prefixes to `aws-iot-data-lakehouse`.
- Files: README.md, CHANGELOG.md
- Verified by: Inspected README structure and tested AWS STS caller identity using the `iot-lakehouse` profile.
- Impact: AWS resource naming and Terraform modules will consistently use the `aws-iot-data-lakehouse` prefix.

---

## 2026-10-21 – Milestone 1: Initialize repository skeleton, tooling, and lint/test automation
- Type: config
- What changed: Created repository directory skeleton, .gitignore, cross-platform Makefile, pyproject.toml with ruff/pytest configs, initial smoke tests, problems log, and changelog. Configured Python 3.11 virtual environment with ruff and pytest.
- Why: Establishes repository foundation and testing harness required by Milestone 1 before implementing pipeline logic. Resolves git credential cache and Windows make installation issues.
- Files: .gitignore, Makefile, pyproject.toml, tests/__init__.py, tests/test_smoke.py, docs/problems.md, docs/results.md, CHANGELOG.md
- Verified by: Ran `make lint` and `make test`, both passed with zero lint errors and 2 passed unit tests.
- Impact: Satisfies Milestone 1 exit criteria. Prepares environment for Milestone 2 generator development.
