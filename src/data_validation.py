import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

from src.config import LIMITS




# Helper functions

def _check_daily_continuity(df: pd.DataFrame) -> bool:
    d = df["date"]
    if d.isna().any():
        return False
    full = pd.date_range(d.min(), d.max(), freq="D")
    return len(full) == len(d) and (d.values == full.values).all()






# Bronze:
# - At least required columns exists with correct types. Additional cols allowed.
# - No other checks

class BronzeSchema(pa.DataFrameModel):
    date: Series[pd.Timestamp]
    meantemp: Series[float]
    humidity: Series[float]
    wind_speed: Series[float]
    meanpressure: Series[float]

    class Config:
        strict = False
        coerce = True


# Silver:
# - Required columns with correct types, no additional columns.
# - Dates continuous and increasing.
# - Numeric values within expected ranges, no missing values.

class SilverSchema(pa.DataFrameModel):
    date: Series[pd.Timestamp] = pa.Field(nullable=False)
    meantemp: Series[float] = pa.Field(nullable=False, ge=LIMITS.temp_min, le=LIMITS.temp_max)
    humidity: Series[float] = pa.Field(nullable=False, ge=LIMITS.humidity_min, le=LIMITS.humidity_max)
    wind_speed: Series[float] = pa.Field(nullable=False, ge=LIMITS.wind_speed_min, le=LIMITS.wind_speed_max)
    meanpressure: Series[float] = pa.Field(nullable=False, ge=LIMITS.pressure_min, le=LIMITS.pressure_max)

    class Config:
        strict = True
        coerce = True

    @pa.dataframe_check
    def unique_dates(cls, df: pd.DataFrame) -> bool:
        return df["date"].is_unique

    @pa.dataframe_check
    def increasing_dates(cls, df: pd.DataFrame) -> bool:
        return df["date"].is_monotonic_increasing

    @pa.dataframe_check
    def continuous_dates(cls, df: pd.DataFrame) -> bool:
        return _check_daily_continuity(df)
    

# Gold:
# - Required columns with correct types, no additional columns.
# - Dates continuous and increasing.

class GoldSchema(pa.DataFrameModel):
    date: Series[pd.Timestamp] = pa.Field(nullable=False)

    class Config:
        strict = True
        coerce = True

    @pa.dataframe_check
    def unique_dates(cls, df: pd.DataFrame) -> bool:
        return df["date"].is_unique

    @pa.dataframe_check
    def increasing_dates(cls, df: pd.DataFrame) -> bool:
        return df["date"].is_monotonic_increasing

    @pa.dataframe_check
    def continuous_dates(cls, df: pd.DataFrame) -> bool:
        return _check_daily_continuity(df)


# Specific gold schema for next day temperature prediction:
# - Date manegement inherited from GoldSchema.
# - Required columns with correct types, no additional columns.
# - Numeric values within expected ranges, no missing values.

class GoldNextDayTempSchema(GoldSchema):
    meantemp_today: Series[float] = pa.Field(nullable=False, ge=LIMITS.temp_min, le=LIMITS.temp_max)
    meantemp_lag_1: Series[float] = pa.Field(nullable=False, ge=LIMITS.temp_min, le=LIMITS.temp_max)
    meantemp_lag_7: Series[float] = pa.Field(nullable=False, ge=LIMITS.temp_min, le=LIMITS.temp_max)
    meantemp_roll_7: Series[float] = pa.Field(nullable=False, ge=LIMITS.temp_min, le=LIMITS.temp_max)
    humidity_today: Series[float] = pa.Field(nullable=False, ge=LIMITS.humidity_min, le=LIMITS.humidity_max)
    humidity_lag_1: Series[float] = pa.Field(nullable=False, ge=LIMITS.humidity_min, le=LIMITS.humidity_max)
    humidity_roll_7: Series[float] = pa.Field(nullable=False, ge=LIMITS.humidity_min, le=LIMITS.humidity_max)
    meanpressure_today: Series[float] = pa.Field(nullable=False, ge=LIMITS.pressure_min, le=LIMITS.pressure_max)
    meanpressure_lag_1: Series[float] = pa.Field(nullable=False, ge=LIMITS.pressure_min, le=LIMITS.pressure_max)
    meanpressure_roll_7: Series[float] = pa.Field(nullable=False, ge=LIMITS.pressure_min, le=LIMITS.pressure_max)
    target: Series[float] = pa.Field(nullable=False, ge=LIMITS.temp_min, le=LIMITS.temp_max)