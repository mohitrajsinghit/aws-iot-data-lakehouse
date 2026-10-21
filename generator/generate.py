"""Synthetic IoT sensor telemetry generator with realistic diurnal cycles and injected defects."""

import argparse
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

try:
    from generator.config import DEFAULT_CONFIG, GeneratorConfig
except ImportError:
    from config import DEFAULT_CONFIG, GeneratorConfig

CSV_HEADER = (
    "event_time,building_id,floor,sensor_id,temperature_c,energy_kwh,co2_ppm,occupancy_count\n"
)


def build_sensor_topology(config: GeneratorConfig) -> list[dict[str, Any]]:
    """Build sensor metadata assigning sensors across buildings and floors."""
    topology = []
    sensor_counter = 1
    sensors_per_floor = config.sensors_per_building // config.floors_per_building

    for bldg in config.buildings:
        for floor in range(1, config.floors_per_building + 1):
            for _ in range(sensors_per_floor):
                sensor_id = f"S{sensor_counter:04d}"
                # Base offset unique to each sensor for realistic spatial variance
                temp_offset = (sensor_counter % 7 - 3) * 0.35
                co2_offset = (sensor_counter % 5 - 2) * 25.0
                energy_factor = 0.85 + (sensor_counter % 11) * 0.03
                topology.append(
                    {
                        "sensor_id": sensor_id,
                        "building_id": bldg,
                        "floor": floor,
                        "temp_offset": temp_offset,
                        "co2_offset": co2_offset,
                        "energy_factor": energy_factor,
                    }
                )
                sensor_counter += 1
    return topology


def compute_diurnal_values(
    hour_float: float,
    temp_offset: float,
    co2_offset: float,
    energy_factor: float,
    rng: np.random.Generator,
) -> tuple[float, float, float, int]:
    """Compute plausible physics-based values for a given time of day."""
    # 1. Occupancy: 0 at night (20:00-06:00), peaks around 14:00 (workday 08:00-18:00)
    if 7.0 <= hour_float < 19.0:
        # Peak around 14:00 with lunch dip around 12:30
        work_progress = (hour_float - 7.0) / 12.0
        occ_curve = math.sin(work_progress * math.pi)
        if 12.0 <= hour_float <= 13.5:
            occ_curve *= 0.75  # Lunch lull
        mean_occ = 5.0 + occ_curve * 32.0
        noise = rng.normal(0, 2.5)
        occupancy = max(0, min(50, int(round(mean_occ + noise))))
    else:
        # Night / off-hours
        occupancy = 1 if rng.random() < 0.04 else 0

    # 2. Temperature: outdoor diurnal cycle tempered by indoor HVAC
    # Peak warmest outdoors at 15:00, coolest at 05:00
    diurnal_temp = 21.5 + 2.5 * math.sin((hour_float - 9.0) * (2 * math.pi / 24.0))
    if 8.0 <= hour_float <= 18.0:
        # HVAC maintains comfortable 22-24 C during working hours
        hvac_target = 22.8 + temp_offset
        temp_c = 0.65 * hvac_target + 0.35 * diurnal_temp + rng.normal(0, 0.25)
    else:
        temp_c = diurnal_temp + temp_offset + rng.normal(0, 0.35)
    temp_c = max(18.0, min(28.0, round(temp_c, 2)))

    # 3. CO2: Base ambient ~410 ppm, rises with occupancy
    base_co2 = 415.0 + co2_offset
    co2 = base_co2 + occupancy * 21.0 + rng.normal(0, 8.0)
    co2 = max(400.0, min(1500.0, round(co2, 1)))

    # 4. Energy: Standby base ~0.25 kWh per 10s, rises with occupancy and HVAC
    base_energy = 0.22 * energy_factor
    active_energy = (occupancy / 50.0) * 2.8 + (temp_c / 28.0) * 0.8
    energy = (base_energy + active_energy + rng.normal(0, 0.08)) * energy_factor
    energy = max(0.01, min(5.0, round(energy, 3)))

    return temp_c, energy, co2, occupancy


def generate_day(
    target_date_str: str,
    config: GeneratorConfig = DEFAULT_CONFIG,
    out_dir: str = "./data",
) -> dict[str, Any]:
    """Generate 1 full day of telemetry partitioned by building and hour, with defect injection."""
    rng = np.random.default_rng(config.seed)
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

    topology = build_sensor_topology(config)
    bldg_sensors: dict[str, list[dict[str, Any]]] = {}
    for sensor in topology:
        bldg_sensors.setdefault(sensor["building_id"], []).append(sensor)

    out_path = Path(out_dir)
    raw_dir = out_path / "raw"
    manifests_dir = raw_dir / "_manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)

    # Step intervals: 360 intervals per hour (every 10s)
    intervals_per_hour = 3600 // config.interval_seconds

    # Defect counters (exact ground-truth)
    defect_counts = {
        "exact_duplicates": 0,
        "late_records": 0,
        "null_values": 0,
        "out_of_range": 0,
        "bad_timestamps": 0,
    }

    # Buffer for late/out-of-order records: (target_hour, building_id) -> list of CSV rows
    late_buffer: dict[tuple[int, str], list[str]] = {}

    total_files_written = 0
    total_rows_written = 0
    base_rows_generated = 0

    # Pre-calculate defect decision thresholds
    dup_thresh = config.duplicate_rate
    late_thresh = dup_thresh + config.late_rate
    null_thresh = late_thresh + config.null_rate
    oor_thresh = null_thresh + config.out_of_range_rate
    bad_ts_thresh = oor_thresh + config.bad_timestamp_rate

    # Generate hour by hour
    for hour in range(24):
        hour_str = f"{hour:02d}"

        for bldg, sensors in bldg_sensors.items():
            bldg_dir = raw_dir / f"building_id={bldg}" / f"date={target_date_str}"
            bldg_dir.mkdir(parents=True, exist_ok=True)
            csv_file = bldg_dir / f"readings_{hour_str}.csv"

            rows_to_write: list[str] = []

            # Flush any late records buffered for this hour and building
            if (hour, bldg) in late_buffer:
                delayed_rows = late_buffer.pop((hour, bldg))
                rows_to_write.extend(delayed_rows)

            for step in range(intervals_per_hour):
                sec_offset = hour * 3600 + step * config.interval_seconds
                event_dt = datetime(
                    target_date.year, target_date.month, target_date.day
                ) + timedelta(seconds=sec_offset)
                hour_float = hour + (step * config.interval_seconds) / 3600.0

                for s in sensors:
                    base_rows_generated += 1
                    event_time_str = event_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    temp_c, energy, co2, occupancy = compute_diurnal_values(
                        hour_float,
                        s["temp_offset"],
                        s["co2_offset"],
                        s["energy_factor"],
                        rng,
                    )

                    # Check for defect injection
                    defect_rand = rng.random()

                    if defect_rand < dup_thresh:
                        # 1. Exact Duplicate: emit row and inject duplicate copy
                        row_str = f"{event_time_str},{bldg},{s['floor']},{s['sensor_id']},{temp_c},{energy},{co2},{occupancy}\n"
                        rows_to_write.append(row_str)
                        rows_to_write.append(row_str)  # Duplicate copy
                        defect_counts["exact_duplicates"] += 1

                    elif defect_rand < late_thresh and hour < 23:
                        # 2. Late record: buffer to be written in a later hour of same day
                        target_delay_hour = rng.integers(hour + 1, 24)
                        late_row = f"{event_time_str},{bldg},{s['floor']},{s['sensor_id']},{temp_c},{energy},{co2},{occupancy}\n"
                        late_buffer.setdefault((int(target_delay_hour), bldg), []).append(late_row)
                        defect_counts["late_records"] += 1

                    elif defect_rand < null_thresh:
                        # 3. Null reading value: nullify one reading
                        null_target = rng.integers(0, 4)
                        t_val = "" if null_target == 0 else str(temp_c)
                        e_val = "" if null_target == 1 else str(energy)
                        c_val = "" if null_target == 2 else str(co2)
                        o_val = "" if null_target == 3 else str(occupancy)
                        row_str = f"{event_time_str},{bldg},{s['floor']},{s['sensor_id']},{t_val},{e_val},{c_val},{o_val}\n"
                        rows_to_write.append(row_str)
                        defect_counts["null_values"] += 1

                    elif defect_rand < oor_thresh:
                        # 4. Out-of-range value
                        oor_choice = rng.integers(0, 4)
                        t_val = 95.0 if oor_choice == 0 else temp_c
                        e_val = -3.5 if oor_choice == 1 else energy
                        c_val = 9999.0 if oor_choice == 2 else co2
                        o_val = -10 if oor_choice == 3 else occupancy
                        row_str = f"{event_time_str},{bldg},{s['floor']},{s['sensor_id']},{t_val},{e_val},{c_val},{o_val}\n"
                        rows_to_write.append(row_str)
                        defect_counts["out_of_range"] += 1

                    elif defect_rand < bad_ts_thresh:
                        # 5. Bad timestamp (future or wrong partition date)
                        if rng.random() < 0.5:
                            bad_ts = "2099-12-31T23:59:59Z"
                        else:
                            bad_date = target_date - timedelta(days=35)
                            bad_ts = bad_date.strftime("%Y-%m-%dT12:00:00Z")
                        row_str = f"{bad_ts},{bldg},{s['floor']},{s['sensor_id']},{temp_c},{energy},{co2},{occupancy}\n"
                        rows_to_write.append(row_str)
                        defect_counts["bad_timestamps"] += 1

                    else:
                        # Clean normal row
                        row_str = f"{event_time_str},{bldg},{s['floor']},{s['sensor_id']},{temp_c},{energy},{co2},{occupancy}\n"
                        rows_to_write.append(row_str)

            # Write the CSV file with header
            with open(csv_file, "w", encoding="utf-8") as f:
                f.write(CSV_HEADER)
                f.writelines(rows_to_write)

            total_files_written += 1
            total_rows_written += len(rows_to_write)

    # Flush any remaining late rows if any remain in late_buffer into hour 23
    for (_hour, bldg), delayed_rows in late_buffer.items():
        csv_file = raw_dir / f"building_id={bldg}" / f"date={target_date_str}" / "readings_23.csv"
        with open(csv_file, "a", encoding="utf-8") as f:
            f.writelines(delayed_rows)
        total_rows_written += len(delayed_rows)

    quarantined_expected = (
        defect_counts["null_values"]
        + defect_counts["out_of_range"]
        + defect_counts["bad_timestamps"]
    )
    clean_rows_expected = (
        total_rows_written - defect_counts["exact_duplicates"] - quarantined_expected
    )

    manifest_data = {
        "date": target_date_str,
        "files_written": total_files_written,
        "base_rows": base_rows_generated,
        "total_rows": total_rows_written,
        "clean_rows": clean_rows_expected,
        "expected_defects": defect_counts,
        "quarantined_expected": quarantined_expected,
        "seed": config.seed,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    manifest_file = manifests_dir / f"date={target_date_str}.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

    return manifest_data


def main():
    parser = argparse.ArgumentParser(description="Generate IoT building sensor telemetry data.")
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-10-01",
        help="Start date in YYYY-MM-DD format (default: 2026-10-01)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to generate (default: 1)",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="./data",
        help="Output directory (default: ./data)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )

    args = parser.parse_args()

    config = GeneratorConfig(seed=args.seed)
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").date()

    for day_idx in range(args.days):
        current_date_str = (start_dt + timedelta(days=day_idx)).strftime("%Y-%m-%d")
        print(f"Generating telemetry for {current_date_str}...")
        manifest = generate_day(current_date_str, config, args.out)
        print(f"  Files written: {manifest['files_written']}")
        print(f"  Total rows written: {manifest['total_rows']:,}")
        print(f"  Base rows: {manifest['base_rows']:,}")
        print(f"  Injected defects: {manifest['expected_defects']}")
        print(f"  Manifest saved: {args.out}/raw/_manifests/date={current_date_str}.json")


if __name__ == "__main__":
    main()
