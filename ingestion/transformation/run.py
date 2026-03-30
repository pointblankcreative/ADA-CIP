"""CLI entrypoint for running the Funnel.io transformation.

Usage:
    python -m ingestion.transformation.run            # daily (default)
    python -m ingestion.transformation.run --mode=full # full history
"""

import argparse
import json
import sys
from pathlib import Path

# Ensure repo root is on the path so backend imports work
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from backend.services.transformation import run_transformation


def main():
    parser = argparse.ArgumentParser(description="Run Funnel.io → fact_digital_daily transformation")
    parser.add_argument("--mode", choices=["daily", "full"], default="daily",
                        help="daily = last 7 days (default), full = all history")
    args = parser.parse_args()

    print(f"Running transformation (mode={args.mode})…")
    result = run_transformation(args.mode)
    print(json.dumps(result, indent=2, default=str))

    if result.get("status") == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
