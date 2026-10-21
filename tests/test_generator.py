"""Unit tests for the IoT sensor telemetry generator."""

import json
from pathlib import Path

import numpy as np

from generator.config import DEFAULT_CONFIG, GeneratorConfig
from generator.generate import (
    build_sensor_topology,
    compute_diurnal_values,
    generate_day,
)


def test_sensor_topology_dimensions():
    """Verify topology matches 10 buildings, 500 sensors, 5 floors."""
    topology = build_sensor_topology(DEFAULT_CONFIG)

    assert len(topology) == 500
    assert topology[0]["sensor_id"] == "S0001"
    assert topology[-1]["sensor_id"] == "S0500"

    # Verify building distribution
    buildings = {s["building_id"] for s in topology}
    assert buildings == {f"B{i:02d}" for i in range(1, 11)}

    # Verify per-building counts and floors
    for bldg in buildings:
        bldg_sensors = [s for s in topology if s["building_id"] == bldg]
        assert len(bldg_sensors) == 50
        floors = {s["floor"] for s in bldg_sensors}
        assert floors == {1, 2, 3, 4, 5}
        for floor in floors:
            floor_sensors = [s for s in bldg_sensors if s["floor"] == floor]
            assert len(floor_sensors) == 10


def test_diurnal_values_physics():
    """Verify diurnal calculations follow expected physical bounds."""
    rng = np.random.default_rng(42)

    # Night time: 02:00 -> occupancy should be near 0
    t_night, e_night, c_night, occ_night = compute_diurnal_values(2.0, 0.0, 0.0, 1.0, rng)
    assert occ_night <= 1
    assert 18.0 <= t_night <= 28.0
    assert 400.0 <= c_night <= 500.0
    assert 0.0 <= e_night <= 1.0

    # Workday peak: 14:00 -> occupancy and CO2 higher
    t_day, e_day, c_day, occ_day = compute_diurnal_values(14.0, 0.0, 0.0, 1.0, rng)
    assert occ_day > 10
    assert 18.0 <= t_day <= 28.0
    assert c_day > c_night
    assert e_day > e_night


def test_generator_small_batch(tmp_path: Path):
    """Test generation on a scaled-down config for fast deterministic verification."""
    test_config = GeneratorConfig(
        buildings=["B01", "B02"],
        sensors_per_building=10,
        floors_per_building=2,
        interval_seconds=60,  # 60 intervals/hour
        seed=123,
    )

    out_dir = str(tmp_path / "telemetry")
    manifest = generate_day("2026-10-01", test_config, out_dir=out_dir)

    # 2 buildings * 10 sensors = 20 sensors
    # 24 hours * 60 intervals = 1440 intervals
    # Base rows = 20 * 1440 = 28,800
    expected_base = 20 * 1440
    assert manifest["base_rows"] == expected_base
    assert manifest["files_written"] == 48  # 2 buildings * 24 hours

    # Verify reconciliation formula
    defects = manifest["expected_defects"]
    quarantined = defects["null_values"] + defects["out_of_range"] + defects["bad_timestamps"]
    assert manifest["quarantined_expected"] == quarantined
    assert (
        manifest["total_rows"] == manifest["clean_rows"] + defects["exact_duplicates"] + quarantined
    )

    # Verify manifest JSON file exists and is parseable
    manifest_file = Path(out_dir) / "raw" / "_manifests" / "date=2026-10-01.json"
    assert manifest_file.exists()
    with open(manifest_file, encoding="utf-8") as f:
        disk_manifest = json.load(f)
    assert disk_manifest["total_rows"] == manifest["total_rows"]


def test_generator_determinism(tmp_path: Path):
    """Verify that identical seed yields identical row counts and defect counts."""
    cfg = GeneratorConfig(
        buildings=["B01"],
        sensors_per_building=5,
        floors_per_building=1,
        interval_seconds=300,
        seed=42,
    )
    out1 = str(tmp_path / "run1")
    out2 = str(tmp_path / "run2")

    m1 = generate_day("2026-10-01", cfg, out1)
    m2 = generate_day("2026-10-01", cfg, out2)

    assert m1["total_rows"] == m2["total_rows"]
    assert m1["expected_defects"] == m2["expected_defects"]
    assert m1["clean_rows"] == m2["clean_rows"]
