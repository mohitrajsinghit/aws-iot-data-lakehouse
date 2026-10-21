# Problems and Solutions Log

This log documents real engineering problems encountered during the construction of the AWS IoT Data Lakehouse, along with root causes and fixes.

---

## Problem 1: Outdated GitHub Credentials in Windows Credential Manager

- **Symptom:** Git push/pull attempts defaulted to credentials for account `mohitrajit`, whereas repository `https://github.com/mohitrajsinghit/aws-iot-data-lakehouse` belongs to `mohitrajsinghit`.
- **Cause:** A legacy Windows Credential Manager entry (`LegacyGeneric:target=git:https://github.com`) was cached under the previous username.
- **Fix:** Purged the stale credential via `cmdkey /delete:LegacyGeneric:target=git:https://github.com`. Git Credential Manager will prompt to authenticate as `mohitrajsinghit`.

---

## Problem 2: Make Package Installation Hanging on Windows

- **Symptom:** `winget install GnuWin32.Make` stalled indefinitely and locked installer binaries in temp.
- **Cause:** The `GnuWin32.Make` package installer is an older 32-bit installer that attempts an interactive GUI/UAC prompt in non-elevated shells.
- **Fix:** Switched to the modern, portable `ezwinports.make` (Make 4.4.1) via winget (`winget install ezwinports.make --accept-source-agreements --accept-package-agreements`), and copied `make.exe` to `.venv/Scripts/make.exe` for reliable path resolution across both PowerShell and cmd.

## Problem 3: Windows PySpark Local Execution (`winutils.exe` & `PYSPARK_PYTHON`)

- **Symptom:** PySpark local DataFrame write failed with `java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`, and worker task failed with `Python worker failed to connect back` / `Python was not found`.
- **Cause:** Apache Hadoop requires `winutils.exe` and `hadoop.dll` on Windows to emulate POSIX filesystem operations when writing Parquet files. Additionally, PySpark worker sub-processes defaulted to calling system `python` rather than the project virtual environment.
- **Fix:** Placed Hadoop 3.3.6 `winutils.exe` and `hadoop.dll` in `.hadoop/bin` (git-ignored), and configured `HADOOP_HOME`, `PYSPARK_PYTHON`, and `PYSPARK_DRIVER_PYTHON` in local execution runners and test harnesses.

---
