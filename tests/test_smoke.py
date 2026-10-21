"""Smoke test ensuring test environment and tooling are functional."""

import sys


def test_python_environment():
    """Verify Python version is at least 3.11."""
    assert sys.version_info >= (3, 11), f"Expected Python >= 3.11, got {sys.version_info}"


def test_basic_arithmetic():
    """Basic sanity check."""
    assert 1 + 1 == 2
