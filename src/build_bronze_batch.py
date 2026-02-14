import argparse
import pandas as pd
from pathlib import Path

from src.config import PATHS
from src.data_validation import BronzeSchema





def _read_raw_csv(path: Path) -> pd.DataFrame:
    """
    Read an incoming raw batch CSV.
    """
    df = pd.read_csv(path)
    return df


def _write_bronze_batch(
    raw_df: pd.DataFrame,
    batch_id: str,
) -> Path:
    """
    Write raw data to Bronze as parquet.

    batch_id example: "01"
    output: data/bronze/batch_01.parquet
    """
    PATHS.bronze.mkdir(parents=True, exist_ok=True)
    df = raw_df.copy()
    batch_name = f"batch_{batch_id}"
    out_path = PATHS.bronze / f"{batch_name}.parquet"
    df.to_parquet(out_path, index=False)
    return out_path





def main() -> None:
    """
    Read new bronze batch (batch_{batch_id}.csv), check needed columns exist, and write bronze/batch_{batch_id}.parquet.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True, help='Batch id like "01"')
    args = parser.parse_args()
    batch_id = args.batch_id
    
    raw = _read_raw_csv(PATHS.input / f"batch_{batch_id}.csv")
    BronzeSchema.validate(raw)
    bronze_path = _write_bronze_batch(raw, batch_id)
    print(f"Wrote bronze: {bronze_path}")



if __name__ == "__main__":
    main()