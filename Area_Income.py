from pathlib import Path
import pandas as pd
import sys
import re

DATA_DIR = Path(__file__).parent

# -----------------------------------------
# Postcode prefix mapping
# -----------------------------------------
postcode_mapping = {
    "Kent": ["BR", "CT", "DA", "ME", "TN"],
    "Essex": ["CB", "CM", "CO", "EN", "IG", "RM", "SS"],
    "London": ["BR", "CR", "DA", "E", "EC", "EN", "HA", "IG", "KT", "N", "NW", "SE", "SM", "SW", "TW", "UB", "W", "WC", "WD"],
    "Sussex": ["BN", "RH", "TN"],
    "Surrey": ["CR", "GU", "KT", "RH", "SM", "SW", "TW"],
}

# Turn into a flat list of unique prefixes
ALLOWED_PREFIXES = sorted({p for lst in postcode_mapping.values() for p in lst})


def load_csv(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise RuntimeError(f"Failed to read {path}: {e}") from e


def extract_prefix(postcode: str) -> str:
    """
    Extract the outward code prefix, e.g.:
    'BR1 3AB' → 'BR'
    'EC1A 1BB' → 'EC'
    'W1A 0AX' → 'W'
    """
    if not isinstance(postcode, str):
        return None
    pc = postcode.strip().upper()
    # outward code = letters at start until digit
    m = re.match(r"^[A-Z]+", pc)
    return m.group(0) if m else None  # type: ignore


if __name__ == "__main__":
    try:
        income_df = load_csv("Total_Anual_Income.csv")
        postcode_df = load_csv("Postcode_Ref.csv")
    except Exception as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    # ---------------------------------------------------
    # Prepare postcode dataframe
    # ---------------------------------------------------
    postcode_df["prefix"] = postcode_df["pcd"].apply(extract_prefix)
    filtered_pc = postcode_df[postcode_df["prefix"].isin(ALLOWED_PREFIXES)].copy()

    # Keep only useful columns
    filtered_pc = filtered_pc[["pcd", "lat", "long", "msoa11"]]

    # ---------------------------------------------------
    # Prepare income dataframe
    # ---------------------------------------------------
    income_df.rename(
        columns={
            income_df.columns[0]: "msoa11",  # MSOA code
            income_df.columns[1]: "msoa_name",
            income_df.columns[2]: "la_code",
            income_df.columns[3]: "la_name",
            income_df.columns[6]: "total_income",
        },
        inplace=True,
    )

    # ---------------------------------------------------
    # Merge postcode-level location data with MSOA income
    # ---------------------------------------------------
    merged = filtered_pc.merge(income_df, on="msoa11", how="left")

    # ---------------------------------------------------
    # Save final CSV
    # ---------------------------------------------------
    output_path = DATA_DIR / "Postcode_Income_Filtered.csv"
    merged.to_csv(output_path, index=False)

    print(f"\nCreated: {output_path}")
    print(merged.head())
