from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

from src.config import LIMITS



# Raw data functions

def read_raw_csv(path: Path) -> pd.DataFrame:
    """
    Read an incoming raw batch CSV.
    """
    df = pd.read_csv(path)
    return df



# Bronze data functions

def write_bronze_batch(
    raw_df: pd.DataFrame,
    bronze_dir: Path,
    batch_id: str,
) -> Path:
    """
    Write raw data to Bronze as parquet.

    batch_id example: "01"
    output: data/bronze/batch_01.parquet
    """
    bronze_dir.mkdir(parents=True, exist_ok=True)
    df = raw_df.copy()
    batch_name = f"batch_{batch_id}"
    out_path = bronze_dir / f"{batch_name}.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def read_all_bronze(bronze_dir: Path) -> pd.DataFrame:
    """
    Load and concatenate all bronze batches.
    """
    files = sorted(bronze_dir.glob("batch_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No bronze batches found in: {bronze_dir}")

    dfs = [pd.read_parquet(p) for p in files]
    return pd.concat(dfs, ignore_index=True)



# Silver data functions

def build_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and fill in from bronze to silver:
    - Dates continuous and increasing.
    - Numeric values withing range and not missing.
    """
    df = bronze_df.copy()

    # Manage dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce")    # To pandas datetime, coerce errors to NaT
    df = df.dropna(subset=["date"])                             # Remove rows with invalid dates
    df = df.sort_values("date")                                 # Sort by date
    df = df.drop_duplicates(subset=["date"], keep="last")       # Remove duplicates, keep last
    df = df.set_index("date").asfreq("D").reset_index()         # Daily continuity, missing dates filled with NaN

    # Make sure columns are float
    float_cols = ["meantemp", "humidity", "wind_speed", "meanpressure"]
    df[float_cols] = df[float_cols].astype("float64")

    # Set numeric values outside of reasonable bounds to NaN
    df.loc[~df["meantemp"].between(LIMITS.temp_min, LIMITS.temp_max), "meantemp"] = pd.NA
    df.loc[~df["humidity"].between(LIMITS.humidity_min, LIMITS.humidity_max), "humidity"] = pd.NA
    df.loc[~df["wind_speed"].between(LIMITS.wind_speed_min, LIMITS.wind_speed_max), "wind_speed"] = pd.NA
    df.loc[~df["meanpressure"].between(LIMITS.pressure_min, LIMITS.pressure_max), "meanpressure"] = pd.NA

    # Fill missing numeric values by interpolating
    df[float_cols] = df[float_cols].interpolate(limit_direction="both")

    return df


def write_silver(silver_df: pd.DataFrame, silver_dir: Path) -> Path:
    """
    Write the silver data to parquet.

    output: data/silver/silver.parquet
    """
    silver_dir = Path(silver_dir)
    silver_dir.mkdir(parents=True, exist_ok=True)
    out_path = silver_dir / "silver.parquet"
    silver_df.to_parquet(out_path, index=False)
    return out_path



# Gold data functions

def build_gold_next_day_temp(silver_df: pd.DataFrame) -> pd.DataFrame:
    """
    Gold data for predicting next day temperature:
    - Target: next day mean temperature
    - Features: past mean temperature (today, lag 1, lag 7, rolling 7)
                past humidity (today, lag 1, rolling 7)
                past mean pressure (today, lag 1, rolling 7)
    """
    df = silver_df.copy()

    # Target: next day mean temperature
    df["target"] = df["meantemp"].shift(-1)

    # Features: today and past
    df["meantemp_lag_1"] = df["meantemp"].shift(1)
    df["meantemp_lag_7"] = df["meantemp"].shift(7)
    df["meantemp_roll_7"] = df["meantemp"].rolling(7, min_periods=7).mean()
    df = df.rename(columns={"meantemp": "meantemp_today"})

    df["humidity_lag_1"] = df["humidity"].shift(1)
    df["humidity_roll_7"] = df["humidity"].rolling(7, min_periods=7).mean()
    df = df.rename(columns={"humidity": "humidity_today"})

    df["meanpressure_lag_1"] = df["meanpressure"].shift(1)
    df["meanpressure_roll_7"] = df["meanpressure"].rolling(7, min_periods=7).mean()
    df = df.rename(columns={"meanpressure": "meanpressure_today"})

    # Drop rows with NaNs caused by lag/rolling/target shifting
    df = df.dropna().reset_index(drop=True)

    keep = [
        "date",
        "meantemp_today", "meantemp_lag_1", "meantemp_lag_7", "meantemp_roll_7",
        "humidity_today", "humidity_lag_1", "humidity_roll_7",
        "meanpressure_today", "meanpressure_lag_1", "meanpressure_roll_7",
        "target",
    ]
    df = df[keep]

    return df


def write_gold(gold_df: pd.DataFrame, gold_dir: Path, name: str) -> Path:
    """
    Write the gold data to parquet.

    output: data/gold/gold_{name}.parquet
    """
    gold_dir = Path(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / f"gold_{name}.parquet"
    gold_df.to_parquet(out_path, index=False)
    return out_path