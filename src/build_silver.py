import pandas as pd
from pathlib import Path

from src.config import LIMITS
from src.config import PATHS
from src.data_validation import SilverSchema



def _read_all_bronze() -> pd.DataFrame:
    """
    Load and concatenate all bronze batches.
    """
    files = sorted(PATHS.bronze.glob("batch_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No bronze batches found in: {PATHS.bronze}")

    dfs = [pd.read_parquet(p) for p in files]
    return pd.concat(dfs, ignore_index=True)


def _build_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
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


def _write_silver(silver_df: pd.DataFrame) -> Path:
    """
    Write the silver data to parquet.

    output: data/silver/silver.parquet
    """
    PATHS.silver.mkdir(parents=True, exist_ok=True)
    out_path = PATHS.silver / "silver.parquet"
    silver_df.to_parquet(out_path, index=False)
    return out_path






def main() -> None:
    """
    Read all bronze batches, build and validate silver, and write to silver/silver.parquet.
    """
    bronze_all = _read_all_bronze()
    silver = _build_silver(bronze_all)
    silver = SilverSchema.validate(silver)
    silver_path = _write_silver(silver)
    print(f"Wrote silver: {silver_path}")



if __name__ == "__main__":
    main()