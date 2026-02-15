# Assignment 3: DataOps
DataOps data pipeline assignment. Data pipeline to prepare weather data for ML. ML part will be implemented later, but goal is to predict mean temperature for the next day.
Data used: [Daily Climate time series data](https://www.kaggle.com/datasets/sumanthvrao/daily-climate-time-series-data)'

Current version has functions to:
- Create new bronze dataset file for new batch.
- Update silver dataset, use all batches available.
- Update gold dataset designed for predicting next day mean temperature.
- Validate data after each step.
- Run data quality tests after.

Run pipeline for new batch:
`./scripts/run_pipeline.sh --batch-id {batch_id}`

## Data management
Data is managed as pandas data frames and saved as parquet files.

Broze data is just read from csv and checked if all necessary columns exist (date, meantemp, humidity, wind_speed, meanpressure).

For silver data following steps are done:
- Increasing order by date, remove duplicates, fill in missing dates.
- Remove vaules out of bounds. (TODO: Bounds explanation.)
- Fill in missing values by interpolaing. (TODO: Explanation.)

Initial gold dataset structure for predicting mean temperature of next day is:
- Date: "date"
- Mean temperature: "meantemp_today", "meantemp_lag_1", "meantemp_lag_7", "meantemp_roll_7"
- Humidity: "humidity_today", "humidity_lag_1", "humidity_roll_7"
- Pressure: "meanpressure_today", "meanpressure_lag_1", "meanpressure_roll_7"
- Next days mean temperature as target: "target"
This is pretty simple initial guess for usefull history information for prediction task. Testing models will give better idea what is usefull.

## Tech stack
Project uses Python and Pandas for data processing. uv is used as Python environment manager, and 'pyproject.toml' can be checked for used versions. Data quality validated using Pandera. Tests for saved parquet files are done by Pytest. DVC is used for data versioning + pipeline.

