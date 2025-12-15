"""Utility helpers for preparing donor datasets used by the Streamlit app."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

MIN_DONATION_DATE = pd.Timestamp("2022-01-01")

TEXT_DTYPES = {
    "postcode": "string",
    "postcode_clean": "string",
    "postcode_area": "string",
    "Donor_Type": "string",
    "Source": "string",
    "Application": "string",
    "country": "string",
    "month": "string",
}


def _normalise_postcode(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip().str.upper()
    cleaned = cleaned.where(cleaned.ne(""))
    return cleaned


def prepare_donor_events(raw_df: pd.DataFrame, min_date: pd.Timestamp | None = MIN_DONATION_DATE) -> pd.DataFrame:
    """Return the subset of donor columns needed by the app with fast-loading dtypes."""
    if raw_df.empty:
        return raw_df.copy()

    df = raw_df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Standardise column names we rely on downstream.
    column_aliases = {
        "Postcode": "postcode",
        "postcode": "postcode",
        "Postal Code": "postcode",
        "post code": "postcode",
        "Total_Amount": "Total_Amount",
        "total_amount": "Total_Amount",
        "Donor_Type": "Donor_Type",
        "Donor Type": "Donor_Type",
        "Month_Year": "Month_Year",
        "Month Year": "Month_Year",
    }

    for column, alias in list(column_aliases.items()):
        if column in df.columns and alias not in df.columns:
            df.rename(columns={column: alias}, inplace=True)

    postcode_col = "postcode"
    if postcode_col not in df.columns:
        raise ValueError("Dataset does not contain a postcode column")

    df[postcode_col] = _normalise_postcode(df[postcode_col])
    df = df[df[postcode_col].notna()].copy()

    df["postcode_area"] = df[postcode_col].str.extract(r"^([A-Z]{1,2})")
    df["postcode_clean"] = df[postcode_col].str.replace(r"\s+", "", regex=True)

    # Parse datetime columns once.
    if "Month_Year" not in df.columns:
        raise ValueError("Dataset does not contain a Month_Year column")

    df["Month_Year"] = df["Month_Year"].astype(str).str.strip()
    df["month_dt"] = pd.to_datetime(df["Month_Year"], format="%m/%Y", errors="coerce")
    df = df[df["month_dt"].notna()].copy()

    if min_date is not None:
        df = df[df["month_dt"] >= min_date].copy()

    df["year"] = df["month_dt"].dt.year.astype("int16")
    df["month"] = df["month_dt"].dt.to_period("M").astype(str)

    # Donation amounts come in with commas/pound symbols sometimes.
    total_amount = df.get("Total_Amount")
    if total_amount is not None:
        cleaned_amounts = (
            total_amount.astype(str)
            .str.replace(r"[£,]", "", regex=True)
            .str.strip()
            .replace("", "0")
        )
        donation_amount = pd.to_numeric(cleaned_amounts, errors="coerce")
    else:
        donation_amount = pd.Series(0.0, index=df.index)

    df["Donation Amount"] = donation_amount.fillna(0.0).astype("float32")

    # Numeric columns we plot.
    for coord in ("latitude", "longitude"):
        if coord in df.columns:
            df[coord] = pd.to_numeric(df[coord], errors="coerce").astype("float32")
        else:
            df[coord] = pd.Series(pd.NA, index=df.index, dtype="float32")

    df = df.dropna(subset=["latitude", "longitude"]).copy()

    # Make sure Source/Application exist (downstream logic expects them).
    for col_name in ("Source", "Application"):
        if col_name not in df.columns:
            df[col_name] = "Unknown"

    if "country" not in df.columns:
        df["country"] = "Unknown"
    else:
        df["country"] = df["country"].fillna("Unknown")

    # Coerce textual columns so PyArrow encodes them efficiently.
    for col_name, dtype in TEXT_DTYPES.items():
        if col_name in df.columns:
            df[col_name] = df[col_name].astype(dtype)

    # Keep only columns needed by the app to minimise payload size.
    selected_columns: Iterable[str] = [
        "postcode",
        "postcode_clean",
        "postcode_area",
        "latitude",
        "longitude",
        "country",
        "Donor_Type",
        "Source",
        "Application",
        "Donation Amount",
        "month",
        "month_dt",
        "year",
    ]

    available_columns = [col for col in selected_columns if col in df.columns]
    prepared = df[available_columns].reset_index(drop=True)
    return prepared


def save_optimised_parquet(raw_csv: Path, parquet_path: Path) -> Path:
    """Utility for CLI scripts to bake the optimised dataset."""
    df_raw = pd.read_csv(raw_csv)
    prepared = prepare_donor_events(df_raw)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    prepared.to_parquet(parquet_path, index=False)
    return parquet_path
