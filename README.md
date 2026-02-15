# Assignment 3: DataOps
DataOps assignment. Local DataOps pipeline to prepare for predicting next day mean temperature from:
[Daily Climate time series data](https://www.kaggle.com/datasets/sumanthvrao/daily-climate-time-series-data)

ML part not implemented yet.

## Tech stack
- Python
- Pandas: Data processing
- uv: Python environment and versions
- Pandera: Data validation
- Pytest: Data quality tests for saved parquet files.
- DVC: data versioning and pipeline
- git: code repository

## Run pipeline
The pipeline is automated to ingest one batch at the time. To run pipeline for a new batch, run script:
`./scripts/run_pipeline.sh --batch-id {batch_id}`

Repeat for all batches. After all batches are run, commit 'dvc.lock' and 'data/bronze.dvc'. Then push dvc and git.

## Pipeline description
The script runs:
- src/ingest_batch.py (csv -> bronze)
- Calls DVC to run silver and gold stages (src/build_silver.py and src/build_gold.py)
- Run data quality tests with Pytest (tests/test_data_quality.py)

Data is validated with Pandera (see src/data_validation.py) after bronze/silver/gold data set is created to validate data. Also tests use same validation, but to test data quality of written parquet files.

NOTE: Make sure input batches are named correctly: 'data/input/batch_{batch_id}.csv', where id can be for example '01'

Pipeline creates:
- data/bronze/batch_{batch_id}.parquet for each batch
- data/silver/silver.parquet
- data/gold/next_day_temp.parquet (currently)

Other files in repository:
- src/config.py: Set data limits and data file paths
- DVC files
    - dvc.yaml: Defined actions and dependencies for silver and gold stages
    - dvc.lock: Current stage and dependencies of output data i.e. silver and gold data sets
    - data/bronze.dvc: Shows current stage of bronze data
- uv files
    - pyproject.toml: Package version requirements
    - uv.lock: Used exact versions for reproducibility





