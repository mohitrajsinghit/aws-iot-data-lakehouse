# Cross-platform Makefile for AWS IoT Data Lakehouse
ifeq ($(OS),Windows_NT)
    VENV_PY := .venv/Scripts/python.exe
    PYTHON := $(wildcard $(VENV_PY))
    ifeq ($(PYTHON),)
        PYTHON := python
    endif
else
    VENV_PY := .venv/bin/python
    PYTHON := $(wildcard $(VENV_PY))
    ifeq ($(PYTHON),)
        PYTHON := python3
    endif
endif

.PHONY: help lint format test generate tf-plan benchmark

help:
	@echo Usage: make [target]
	@echo Targets:
	@echo   lint        Run ruff linter and format checks
	@echo   format      Auto-format source code with ruff
	@echo   test        Run pytest test suite
	@echo   generate    Run sensor telemetry data generator (1 day demo)
	@echo   tf-plan     Run terraform plan
	@echo   benchmark   Run Athena query benchmark suite

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

test:
	$(PYTHON) -m pytest tests/ -v

generate:
	$(PYTHON) generator/generate.py --start-date 2026-10-01 --days 1 --out ./data --seed 42

tf-plan:
	cd terraform && terraform plan

benchmark:
	$(PYTHON) athena/benchmark.py
