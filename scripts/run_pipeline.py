"""Run the complete ETL pipeline from the terminal.

Usage:
    python scripts/run_pipeline.py
    python scripts/run_pipeline.py --input data/raw/sales_data.csv --output data/processed
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Sales Data ETL pipeline")
    parser.add_argument("--input", type=Path, default=None, help="Raw CSV file or directory (default: RAW_DATA_PATH)")
    parser.add_argument("--output", type=Path, default=None, help="Output directory (default: PROCESSED_DATA_PATH)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        from src.pipeline import run_pipeline
    except ImportError as exc:
        print(f"Missing dependency: {exc}. Install with: pip install -r requirements.txt", file=sys.stderr)
        return 2

    result = run_pipeline(input_path=args.input, output_dir=args.output)

    print()
    print(f"Pipeline status : {result.status}")
    print(f"Duration        : {result.duration_seconds}s")
    if result.success:
        stats = result.statistics
        cleaning = stats["cleaning"]
        print(f"Input records   : {stats['input_records']}")
        print(f"Cleaned records : {cleaning['records_after']}")
        print(f"Rejected records: {cleaning['records_rejected']} {cleaning['rejection_reasons']}")
        print("Outputs:")
        for name, path in result.outputs.items():
            print(f"  {name:<18} {path}")
        return 0
    print(f"Error           : {result.error}", file=sys.stderr)
    print("See logs/pipeline.log for details.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
