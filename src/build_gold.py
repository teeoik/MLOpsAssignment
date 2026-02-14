import pandas as pd
from pathlib import Path

from src.config import LIMITS
from src.config import PATHS
from src.data_validation import GoldNextDayTempSchema




def _read_silver() -> pd.DataFrame:
    """
    Load silver/silver.parquet.
    """
    path = PATHS.silver / "silver.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No silver batch found at: {path}")
    df = pd.read_parquet(path)
    return df


def _build_gold_next_day_temp(silver_df: pd.DataFrame) -> pd.DataFrame:
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


def _write_gold(gold_df: pd.DataFrame, name: str) -> Path:
    """
    Write the gold data to parquet.

    output: data/gold/{name}.parquet
    """
    PATHS.gold.mkdir(parents=True, exist_ok=True)
    out_path = PATHS.gold / f"{name}.parquet"
    gold_df.to_parquet(out_path, index=False)
    return out_path




def main() -> None:
    """
    Read silver dataset, and build, validate and write gold date sets, including:
    - next_day_temp.parquet
    """
    silver = _read_silver()
    gold = _build_gold_next_day_temp(silver)
    gold = GoldNextDayTempSchema.validate(gold)
    gold_path = _write_gold(gold, "next_day_temp")
    print(f"Wrote gold: {gold_path}")



if __name__ == "__main__":
    main()