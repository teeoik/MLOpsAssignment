import pandas as pd
from pathlib import Path
import pytest

from src.config import PATHS
from src.data_validation import BronzeSchema
from src.data_validation import SilverSchema
from src.data_validation import GoldNextDayTempSchema



def test_bronze() -> None:
    """ Test all bronze batches. """
    paths = list(PATHS.bronze.glob("*.parquet"))
    if not paths:
        pytest.skip("No bronze batches found.")
    for path in paths:
        df = pd.read_parquet(path)
        BronzeSchema.validate(df)

def test_silver() -> None:
    path = PATHS.silver / "silver.parquet"
    if not path.exists():
        pytest.skip("No silver batch found.")
    df = pd.read_parquet(path)
    SilverSchema.validate(df)

def test_gold() -> None:
    """ Read gold parquet file and validate data quality. """
    path = PATHS.gold / "next_day_temp.parquet"
    if not path.exists():
        pytest.skip("No gold batch found.")
    df = pd.read_parquet(path)
    GoldNextDayTempSchema.validate(df)