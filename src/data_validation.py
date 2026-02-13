import pandas as pd
from src.config import LIMITS



# Validation functions

def validate_original_cols(df: pd.DataFrame) -> bool:
    """ Check if columns are as expected for bronze and silver."""
    expected_cols = {"date", "meantemp", "humidity", "wind_speed", "meanpressure"}
    return set(df.columns) == expected_cols


def validate_dates(df: pd.DataFrame) -> bool:
    """ Check if dates are continuous and increasing. """
    return df["date"].is_monotonic_increasing and df["date"].is_unique

def validate_no_missing_vals(df: pd.DataFrame) -> bool:
    """ Check if there are no missing values in the silver data. """
    return not df.isna().any().any()

def validate_ranges_silver(df: pd.DataFrame) -> bool:
    """ Check if numeric values are within expected ranges. """
    return (
        df["meantemp"].between(LIMITS.temp_min, LIMITS.temp_max).all() and
        df["humidity"].between(LIMITS.humidity_min, LIMITS.humidity_max).all() and
        df["wind_speed"].between(LIMITS.wind_speed_min, LIMITS.wind_speed_max).all() and
        df["meanpressure"].between(LIMITS.pressure_min, LIMITS.pressure_max).all()
    )

def validate_ranges_gold_next_day_temp(df: pd.DataFrame) -> bool:
    """ Check if numeric values are within expected ranges. """
    temp_cols = ["meantemp_today", "meantemp_lag_1", "meantemp_lag_7", "meantemp_roll_7", "target"]
    humidity_cols = ["humidity_today", "humidity_lag_1", "humidity_roll_7"]
    pressure_cols = ["meanpressure_today", "meanpressure_lag_1", "meanpressure_roll_7"]
    return (
        ((df[temp_cols] >= LIMITS.temp_min) & (df[temp_cols] <= LIMITS.temp_max)).all().all() and
        ((df[humidity_cols] >= LIMITS.humidity_min) & (df[humidity_cols] <= LIMITS.humidity_max)).all().all() and
        ((df[pressure_cols] >= LIMITS.pressure_min) & (df[pressure_cols] <= LIMITS.pressure_max)).all().all()
    )


# All validations

def validate_bronze(df: pd.DataFrame) -> None:
    """ Run all bronze validation checks. """
    if not validate_original_cols(df):
        raise ValueError("Bronze validation failed: Columns are not as expected.")

def validate_silver(df: pd.DataFrame) -> None:
    """ Run all silver validation checks. """
    if not validate_original_cols(df):
        raise ValueError("Silver validation failed: Columns are not as expected.")
    if not validate_dates(df):
        raise ValueError("Silver validation failed: Dates are not continuous and increasing.")
    if not validate_ranges_silver(df):
        raise ValueError("Silver validation failed: Numeric values are out of expected ranges.")
    if not validate_no_missing_vals(df):
        raise ValueError("Silver validation failed: Missing values found.")

def validate_gold_next_day_temp(df: pd.DataFrame) -> None:
    """ Run all gold validation checks. """
    if not validate_dates(df):
        raise ValueError("Gold validation failed: Dates are not continuous and increasing.")
    if not validate_ranges_gold_next_day_temp(df):
        raise ValueError("Gold validation failed: Numeric values are out of expected ranges.")
    if not validate_no_missing_vals(df):
        raise ValueError("Gold validation failed: Missing values found.")
