from dataclasses import dataclass
from pathlib import Path




@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def input(self) -> Path:
        return self.root / "data" / "input"

    @property
    def bronze(self) -> Path:
        return self.root / "data" / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "data" / "silver"

    @property
    def gold(self) -> Path:
        return self.root / "data" / "gold"


PATHS = ProjectPaths(
    root=Path(__file__).resolve().parents[1]
)





@dataclass(frozen=True)
class DataLimits:

    # Slightly larger range than Delhi all time min/max
    @property
    def temp_min(self) -> float:
        return -10
    @property
    def temp_max(self) -> float:
        return 60
    
    @property
    def humidity_min(self) -> float:
        return 0
    @property
    def humidity_max(self) -> float:
        return 100
    
    # Quite random max value for 5 min mean speed during a cyclone. So daily averages are definately not higher.
    @property
    def wind_speed_min(self) -> float:
        return 0
    @property
    def wind_speed_max(self) -> float:
        return 180
    
    # Based on inspection of the data to cut off outliers. Probably not best approach, but good enough for this assignment.
    @property
    def pressure_min(self) -> float:
        return 900
    @property
    def pressure_max(self) -> float:
        return 1100

LIMITS = DataLimits()



