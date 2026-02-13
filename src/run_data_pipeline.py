import argparse
from pathlib import Path

import src.data_processing as processing
from src.config import PATHS
import src.data_validation as validation




def main() -> None:
    """
    Run the data pipeline for one batch:
    - Read from input/batch_{batch_id}.csv
    - Create bronze and silver to batch_{batch_id}.parquet
    - Create gold datesets:
        - next_day_temp: features from day N, target is meantemp of day N+1
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True, help='Batch id like "01"')
    args = parser.parse_args()
    batch_id = args.batch_id
    
    raw = processing.read_raw_csv(PATHS.input / f"batch_{batch_id}.csv")
    validation.validate_bronze(raw)
    bronze_path = processing.write_bronze_batch(raw, PATHS.bronze, batch_id)
    print(f"Wrote bronze: {bronze_path}")

    bronze_all = processing.read_all_bronze(PATHS.bronze)
    silver = processing.build_silver(bronze_all)
    validation.validate_silver(silver)
    silver_path = processing.write_silver(silver, PATHS.silver)
    print(f"Wrote silver: {silver_path}")

    gold = processing.build_gold_next_day_temp(silver)
    validation.validate_gold_next_day_temp(gold)
    gold_path = processing.write_gold(gold, PATHS.gold, "next_day_temp")
    print(f"Wrote gold: {gold_path}")


if __name__ == "__main__":
    main()