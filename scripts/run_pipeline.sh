# Exit immediately if a command exits with a non-zero status, 
# if an undefined variable is used, or if any command in a pipeline fails.
set -euo pipefail

# Take '--batch-id {batch_number}' as an argument
BATCH_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --batch-id)
      BATCH_ID="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done
if [ -z "$BATCH_ID" ]; then
  echo "Usage: $0 --batch-id <id>"
  exit 1
fi

# Run pipeline

# Ingest new batch: csv -> bronze
uv run python -m src.ingest_batch --batch-id "$BATCH_ID"

# Run DVC pipeline: broze -> silver -> gold
dvc repro

# Run data quality tests
uv run python -m pytest -q
