"""Configuration parameters and defect rates for IoT telemetry simulation."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class GeneratorConfig:
    """Simulation configuration parameters."""

    # Topology
    buildings: list[str] = field(default_factory=lambda: [f"B{i:02d}" for i in range(1, 11)])
    sensors_per_building: int = 50
    floors_per_building: int = 5
    interval_seconds: int = 10
    seed: int = 42

    # Normal parameter ranges (min, max)
    temp_range: tuple[float, float] = (18.0, 28.0)
    energy_range: tuple[float, float] = (0.0, 5.0)
    co2_range: tuple[float, float] = (400.0, 1500.0)
    occupancy_range: tuple[int, int] = (0, 50)

    # Injected defect rates (Section 7.3)
    duplicate_rate: float = 0.015  # 1.5% exact duplicates (Check 4)
    late_rate: float = 0.005  # 0.5% out-of-order/late records
    null_rate: float = 0.005  # 0.5% null reading values (Check 2)
    out_of_range_rate: float = 0.003  # 0.3% out-of-range sensor readings (Check 3)
    bad_timestamp_rate: float = 0.002  # 0.2% invalid/future timestamps (Check 5)

    @property
    def total_sensors(self) -> int:
        return len(self.buildings) * self.sensors_per_building

    @property
    def intervals_per_day(self) -> int:
        return 86400 // self.interval_seconds  # 8,640 intervals per sensor

    @property
    def expected_base_rows_per_day(self) -> int:
        return self.total_sensors * self.intervals_per_day  # 4,320,000 rows


DEFAULT_CONFIG = GeneratorConfig()
