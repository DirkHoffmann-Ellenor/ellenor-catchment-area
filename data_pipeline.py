import argparse
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "data_cache"
CACHE_DIR.mkdir(exist_ok=True)

RAW_FILES = {
    "patients": BASE_DIR / "postcode_coordinates.csv",
    "donors": BASE_DIR / "donation_events_geocoded.csv",
    "shops": BASE_DIR / "shops_geocoded.csv",
}

AREA_INCOME_FILE = BASE_DIR / "Postcode_Income_Filtered.csv"

CACHE_FILES = {
    "patients": CACHE_DIR / "patients.parquet",
    "donors_unique": CACHE_DIR / "donors_unique.parquet",
    "donor_events": CACHE_DIR / "donor_events.parquet",
    "shops": CACHE_DIR / "shops.parquet",
    "area_income": CACHE_DIR / "area_income.parquet",
}


def _load_raw_csvs() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the source CSVs with only the columns we actually need."""
    patients = pd.read_csv(
        RAW_FILES["patients"],
        usecols=["postcode", "latitude", "longitude", "admin_district", "admin_county", "country"],
    )
    donors = pd.read_csv(
        RAW_FILES["donors"],
        usecols=[
            "Month_Year",
            "Postcode",
            "Donor_Type",
            "Total_Amount",
            "Source",
            "Application",
            "latitude",
            "longitude",
            "country",
        ],
    )
    shops = pd.read_csv(
        RAW_FILES["shops"],
        usecols=["postcode", "latitude", "longitude", "admin_district", "admin_county", "country", "name"],
    )
    return patients, donors, shops


def _normalise_dataframes() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Re-usable transformation that mirrors the Streamlit data prep."""
    patients, donors, shops = _load_raw_csvs()

    if "Postcode" in donors.columns and "postcode" not in donors.columns:
        donors.rename(columns={"Postcode": "postcode"}, inplace=True)

    donors["Month_Year"] = donors["Month_Year"].astype(str)
    donors["month_dt"] = pd.to_datetime(donors["Month_Year"], format="%m/%Y", errors="coerce")
    donors = donors[donors["month_dt"].notna()].copy()
    donors = donors[donors["month_dt"] >= pd.Timestamp("2022-01-01")].copy()
    donors["year"] = donors["month_dt"].dt.year  # type: ignore
    donors["month"] = donors["month_dt"].dt.to_period("M").astype(str)  # type: ignore

    donors["Donation Amount"] = (
        donors["Total_Amount"].astype(str).str.replace(r"[£,]", "", regex=True).astype(float).fillna(0.0)
    )

    for col in donors.columns:
        if col.lower() == "source":
            donors.rename(columns={col: "Source"}, inplace=True)
        if col.lower() == "application":
            donors.rename(columns={col: "Application"}, inplace=True)

    if "Source" not in donors.columns:
        donors["Source"] = "Unknown"
    if "Application" not in donors.columns:
        donors["Application"] = "Unknown"

    def _clean_postcodes(df: pd.DataFrame) -> pd.DataFrame:
        if "postcode" not in df.columns and "Postcode" in df.columns:
            df.rename(columns={"Postcode": "postcode"}, inplace=True)
        df["postcode"] = df["postcode"].astype(str).str.strip()
        df["postcode_area"] = df["postcode"].str.extract(r"^([A-Z]{1,2})")
        df["postcode_clean"] = df["postcode"].str.upper().str.replace(r"\s+", "", regex=True)
        if "country" in df.columns:
            df["country"] = df["country"].fillna("Unknown")
        else:
            df["country"] = "Unknown"
        return df

    patients = _clean_postcodes(patients)
    donors = _clean_postcodes(donors)
    shops = _clean_postcodes(shops)

    donors["latitude"] = donors["latitude"].astype(float)
    donors["longitude"] = donors["longitude"].astype(float)

    def _unique_join(values):
        cleaned = sorted({str(v).strip() for v in values if pd.notna(v) and str(v).strip()})
        return ", ".join(cleaned) if cleaned else "Unknown"

    def _collect_sources(values):
        flattened = []
        for item in values:
            if isinstance(item, (list, tuple)):
                flattened.extend(item)
            elif pd.notna(item) and str(item).strip():
                flattened.append(str(item).strip())
        return sorted({code for code in flattened if code})

    monthly = donors.groupby(["postcode", "month"], as_index=False).agg(
        latitude=("latitude", "first"),
        longitude=("longitude", "first"),
        country=("country", "first"),
        postcode_area=("postcode_area", "first"),
        postcode_clean=("postcode_clean", "first"),
        month_dt=("month_dt", "max"),
        donation_sum=("Donation Amount", "sum"),
        max_single=("Donation Amount", "max"),
        events_in_month=("Donation Amount", "size"),
        donor_type=("Donor_Type", _unique_join),
        source_list=("Source", _collect_sources),
    )

    monthly["Donation Amount"] = monthly["donation_sum"].astype(float)
    monthly["max_single_donation"] = monthly["max_single"].astype(float)
    monthly.drop(columns=["donation_sum", "max_single"], inplace=True)
    monthly["Source"] = monthly["source_list"].apply(lambda vals: vals[0] if len(vals) == 1 else ("Multiple" if vals else "Unknown"))

    patients["latitude"] = patients["latitude"].astype(float)
    patients["longitude"] = patients["longitude"].astype(float)
    shops["latitude"] = shops["latitude"].astype(float)
    shops["longitude"] = shops["longitude"].astype(float)

    donors_unique = (
        monthly[["postcode", "latitude", "longitude", "country", "postcode_area"]]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates(subset=["postcode"])
        .reset_index(drop=True)
    )

    return patients, donors_unique, monthly, shops


def _load_area_income() -> pd.DataFrame:
    """Bring in postcode-level income/age data if provided."""
    if not AREA_INCOME_FILE.exists():
        return pd.DataFrame(
            columns=[
                "postcode",
                "latitude",
                "longitude",
                "postcode_area",
                "postcode_clean",
                "country",
                "net_income",
            ]
        )

    df = pd.read_csv(AREA_INCOME_FILE)
    rename_map = {}
    if "pcd" in df.columns:
        rename_map["pcd"] = "postcode"
    if "lat" in df.columns:
        rename_map["lat"] = "latitude"
    if "long" in df.columns:
        rename_map["long"] = "longitude"
    df.rename(columns=rename_map, inplace=True)

    required = ["postcode", "latitude", "longitude"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Income dataset is missing required columns: {missing}")

    df["postcode"] = df["postcode"].astype(str).str.strip().str.upper()
    df["postcode_area"] = df["postcode"].str.extract(r"^([A-Z]{1,2})")
    df["postcode_clean"] = df["postcode"].str.replace(r"\s+", "", regex=True)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["country"] = df.get("country", "England").fillna("England")

    income_col = None
    for candidate in ("net_income", "total_income", "Net annual income (£)"):
        if candidate in df.columns:
            income_col = candidate
            break
    if income_col:
        df["net_income"] = pd.to_numeric(df[income_col], errors="coerce")
    else:
        df["net_income"] = pd.NA

    age_cols = [col for col in df.columns if "age" in col.lower()]
    for col in age_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    label_fields = ["msoa_name", "MSOA21NM", "la_name"]
    df["area_label"] = None
    for field in label_fields:
        if field in df.columns:
            df["area_label"] = df["area_label"].fillna(df[field].astype(str))
    df["area_label"] = df["area_label"].fillna(df["postcode_area"])

    return df


def write_cache() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build processed Parquet files so Streamlit can load them instantly."""
    patients, donors_unique, monthly, shops = _normalise_dataframes()
    area_income = _load_area_income()

    datasets: Dict[str, pd.DataFrame] = {
        "patients": patients,
        "donors_unique": donors_unique,
        "donor_events": monthly,
        "shops": shops,
        "area_income": area_income,
    }

    for key, df in datasets.items():
        df.to_parquet(CACHE_FILES[key], index=False)
    return patients, donors_unique, monthly, shops, area_income


def load_processed_data(force_rebuild: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load pre-processed data, rebuilding if the cache is missing or requested."""
    if not force_rebuild and all(path.exists() for path in CACHE_FILES.values()):
        return tuple(pd.read_parquet(path) for path in CACHE_FILES.values())  # type: ignore
    return write_cache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Precompute Parquet datasets for the Streamlit app.")
    parser.add_argument("--force", action="store_true", help="Force rebuilding the cache even if files exist.")
    args = parser.parse_args()

    load_processed_data(force_rebuild=args.force)
    print(f"Wrote processed datasets to {CACHE_DIR.resolve()}")
