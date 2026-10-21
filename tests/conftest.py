"""Pytest fixtures for local PySpark testing."""

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a lightweight local SparkSession for testing."""
    project_root = Path(__file__).resolve().parent.parent

    # Configure local Windows Hadoop / winutils if present
    hadoop_home = project_root / ".hadoop"
    if hadoop_home.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] = f"{hadoop_home / 'bin'};{os.environ.get('PATH', '')}"

    # Ensure worker python matches active venv python
    venv_py = project_root / ".venv" / "Scripts" / "python.exe"
    if venv_py.exists():
        os.environ["PYSPARK_PYTHON"] = str(venv_py)
        os.environ["PYSPARK_DRIVER_PYTHON"] = str(venv_py)

    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("LakehouseUnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()
