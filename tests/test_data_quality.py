import pandas as pd
from pathlib import Path
import pytest

import src.data_validation as validation
from src.config import PATHS




def test_bronze() -> None:
    """ Test all bronze batches. """
    paths = list(PATHS.bronze.glob("*.parquet"))
    if not paths:
        pytest.skip("No bronze batches found.")
    for path in paths:
        df = pd.read_parquet(path)
        validation.validate_bronze(df)

def test_silver() -> None:
    path = PATHS.silver / "silver.parquet"
    if not path.exists():
        pytest.skip("No silver batch found.")
    df = pd.read_parquet(path)
    validation.validate_silver(df)

def test_gold() -> None:
    """ Read gold parquet file and validate data quality. """
    path = PATHS.gold / "gold_next_day_temp.parquet"
    if not path.exists():
        pytest.skip("No gold batch found.")
    df = pd.read_parquet(path)
    validation.validate_gold_next_day_temp(df)