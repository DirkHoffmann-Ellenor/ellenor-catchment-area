"""Build a streamlined donor dataset for fast Streamlit loading."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_pipeline import prepare_donor_events

RAW_DONOR_CSV = Path("donation_events_geocoded.csv")
OUTPUT_PATH = Path("data/donor_events_optimised.parquet")


def main() -> None:
    if not RAW_DONOR_CSV.exists():
        raise FileNotFoundError(f"Cannot find expected donor CSV at {RAW_DONOR_CSV}")

    print(f"📂 Reading {RAW_DONOR_CSV}...")
    raw_df = pd.read_csv(RAW_DONOR_CSV)
    print(f"   Rows read: {len(raw_df):,}")

    prepared = prepare_donor_events(raw_df)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    prepared.to_parquet(OUTPUT_PATH, index=False)

    orig_size = RAW_DONOR_CSV.stat().st_size / (1024 * 1024)
    new_size = OUTPUT_PATH.stat().st_size / (1024 * 1024)
    print(f"✅ Saved optimised dataset to {OUTPUT_PATH}")
    print(f"   Original CSV size: {orig_size:.1f} MB")
    print(f"   Optimised parquet: {new_size:.1f} MB")
    print(f"   Remaining rows: {len(prepared):,}")


if __name__ == "__main__":
    main()
