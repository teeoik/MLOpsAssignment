import argparse
import pandas as pd
from pathlib import Path
import hashlib
import subprocess

from src.config import PATHS
from src.data_validation import BronzeSchema






def _source_sha(path: Path) -> str:
    """
    Get the SHA256 hash of the source file.
    """
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _git_commit_hash() -> str | None:
    """
    Return current git commit hash, or None if not available.
    """
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        return out
    except Exception:
        return None


def _add_metadata(
    df: pd.DataFrame,
    source_path: Path,
) -> pd.DataFrame:
    """
    Add metadata columns to the raw dataframe.
    Remember to update config INPUT_METADATA_COLS if you change the metadata columns.
    """
    df = df.copy()
    df["source_path"] = str(source_path)
    df["source_sha"] = _source_sha(source_path)
    df["git_commit_hash"] = _git_commit_hash()
    df["ingested_at"] = pd.Timestamp.now()
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
    
    batch_path = PATHS.input / f"batch_{batch_id}.csv"
    if not batch_path.exists():
        raise FileNotFoundError(f"Batch file not found: {batch_path}")
    raw = pd.read_csv(batch_path)
    bronze = _add_metadata(raw, batch_path)
    BronzeSchema.validate(bronze)
    bronze_path = _write_bronze_batch(bronze, batch_id)
    print(f"Wrote bronze: {bronze_path}")



if __name__ == "__main__":
    main()