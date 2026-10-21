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

---
