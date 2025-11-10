import pandas as pd
import requests
import time
import os
from pathlib import Path

# ----------------------------
# CONFIG — adjust if needed
# ----------------------------
# PATIENTS_FILE = "EMIS Patient postcodes.csv"
DONOR_FILES = ["DonorData1.csv", "DonorData2.csv", "DonorData3.csv"]   # can be 1..n files
DONOR_DATE_COLUMN = "Donation Date"  # <-- change if your date column is named differently

# Outputs (used by the app)
PATIENTS_GEOCODED = "postcode_coordinates.csv"
DONORS_GEOCODED = "donor_postcode_coordinates.csv"         # unique donor postcodes
DONOR_EVENTS_GEOCODED = "donor_events_geocoded.csv"         # each donation with lat/lon and month


def get_postcode_coordinates(postcode):
    """
    Look up a single UK postcode via postcodes.io
    """
    postcode = str(postcode).strip().upper()
    if not postcode:
        return None

    try:
        r = requests.get(f"https://api.postcodes.io/postcodes/{postcode}", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == 200 and data.get("result"):
                res = data["result"]
                return {
                    "postcode": postcode,
                    "latitude": res["latitude"],
                    "longitude": res["longitude"],
                    "admin_district": res.get("admin_district", ""),
                    "admin_county": res.get("admin_county", ""),
                    "country": res.get("country", "")
                }
    except Exception:
        pass
    return None


def geocode_unique_postcodes(postcodes, outfile, existing_lookup=None, polite_delay=0.08):
    """
    Geocode a set of unique postcodes. Reuses any coordinates from an existing lookup dict or CSV.
    """
    results = []

    # Start with existing lookup if provided
    if existing_lookup is None:
        existing_lookup = {}

    # If an outfile already exists, merge it into the lookup too (so we never redo work)
    if Path(outfile).exists():
        prev = pd.read_csv(outfile)
        for _, row in prev.iterrows():
            existing_lookup[row["postcode"]] = {
                "postcode": row["postcode"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "admin_district": row.get("admin_district", ""),
                "admin_county": row.get("admin_county", ""),
                "country": row.get("country", "")
            }

    postcodes = [str(p).strip().upper() for p in postcodes if pd.notna(p)]
    postcodes = sorted(set(p for p in postcodes if p))

    for i, pc in enumerate(postcodes):
        if i % 50 == 0:
            print(f"Geocoding {i}/{len(postcodes)} ...")

        if pc in existing_lookup:
            results.append(existing_lookup[pc])
            continue

        rec = get_postcode_coordinates(pc)
        if rec:
            results.append(rec)
            existing_lookup[pc] = rec
        time.sleep(polite_delay)

    df = pd.DataFrame(results)
    if not df.empty:
        df.to_csv(outfile, index=False)
        print(f"✅ Saved {outfile} ({len(df):,} rows)")
    else:
        print(f"⚠️ No rows to save in {outfile}")

    return df, existing_lookup


def main():
    # 1) Patients -> unique postcodes -> geocode
    # patients = pd.read_csv(PATIENTS_FILE)
    # patient_postcodes = patients["Postcode"].dropna().drop_duplicates()
    # patients_geocoded, lookup = geocode_unique_postcodes(
    #     postcodes=patient_postcodes,
    #     outfile=PATIENTS_GEOCODED,
    #     existing_lookup={}
    # )

    # 2) Donor raw files -> concat -> standardise -> parse dates (DD/MM/YYYY) -> unique postcodes
    donor_frames = []
    for f in DONOR_FILES:
        if Path(f).exists():
            df = pd.read_csv(f)
            donor_frames.append(df)
        else:
            print(f"⚠️ Missing donor file: {f}")

    if not donor_frames:
        print("⚠️ No donor files found. Skipping donor processing.")
        return

    donors_raw = pd.concat(donor_frames, ignore_index=True)

    # Normalise column names
    donors_raw.columns = [c.strip() for c in donors_raw.columns]
    if "Postcode" not in donors_raw.columns:
        raise ValueError("Donor files must contain a 'Postcode' column.")
    if DONOR_DATE_COLUMN not in donors_raw.columns:
        raise ValueError(f"Donor files must contain a '{DONOR_DATE_COLUMN}' column.")

    # Date parsing fix — DD/MM/YYYY
    donors_raw["__date"] = pd.to_datetime(
        donors_raw[DONOR_DATE_COLUMN].astype(str).str.strip(),
        dayfirst=True,
        errors="coerce"
    )
    # Filter out bad dates
    donors_raw = donors_raw[donors_raw["__date"].notna()].copy()
    donors_raw["month"] = donors_raw["__date"].dt.to_period("M").astype(str)  # e.g. 2024-03

    # 3) Donors unique postcodes -> geocode (reusing patient lookup to avoid extra calls)
    donor_unique_postcodes = donors_raw["Postcode"].dropna().drop_duplicates()
    donors_geocoded, lookup = geocode_unique_postcodes(
        postcodes=donor_unique_postcodes,
        outfile=DONORS_GEOCODED,
    )

    # 4) Build donor EVENTS table with coordinates merged (no repeated geocoding)
    if donors_geocoded.empty:
        print("⚠️ Donor geocoded table is empty. Skipping events output.")
        return

    donors_geocoded_lookup = donors_geocoded.set_index("postcode")[["latitude", "longitude", "admin_district", "admin_county", "country"]].to_dict(orient="index")

    # Map coords onto each donation event
    def attach_coords(pc):
        pc = str(pc).strip().upper()
        return donors_geocoded_lookup.get(pc, None)

    coords = donors_raw["Postcode"].apply(attach_coords)
    donors_events = donors_raw.copy()
    donors_events["latitude"] = coords.apply(lambda x: x["latitude"] if x else None)
    donors_events["longitude"] = coords.apply(lambda x: x["longitude"] if x else None)
    donors_events["admin_district"] = coords.apply(lambda x: x.get("admin_district", "") if x else "")
    donors_events["admin_county"] = coords.apply(lambda x: x.get("admin_county", "") if x else "")
    donors_events["country"] = coords.apply(lambda x: x.get("country", "") if x else "")

    donors_events = donors_events.dropna(subset=["latitude", "longitude"]).copy()

    # Save donor events with coords + month
    donors_events.to_csv(DONOR_EVENTS_GEOCODED, index=False)
    print(f"✅ Saved {DONOR_EVENTS_GEOCODED} ({len(donors_events):,} rows)")

    print("\nAll done! Files ready for the Streamlit app:")
    print(f" - {PATIENTS_GEOCODED}")
    print(f" - {DONORS_GEOCODED}")
    print(f" - {DONOR_EVENTS_GEOCODED}")


if __name__ == "__main__":
    main()

# in build_postcode_dataset.py (one-off helper)
def build_donor_events_geocoded(raw_events_csv, postcode_lookup_csv, out_csv):
    events = pd.read_csv(raw_events_csv)  # must contain Postcode, Date, Donation Amount
    lookup = pd.read_csv(postcode_lookup_csv)  # columns: postcode, latitude, longitude, country, etc.

    # Clean & parse
    events["postcode"] = events["Postcode"].astype(str).str.upper().str.strip()
    events["__date"] = pd.to_datetime(events["Date"].astype(str).str.strip(), dayfirst=True, errors="coerce")
    events = events[events["__date"].notna()].copy()
    events["month"] = events["__date"].dt.to_period("M").astype(str)
    events["Donation Amount"] = (
        events["Donation Amount"].astype(str).str.replace("[£,]", "", regex=True).str.strip()
    )
    events["Donation Amount"] = pd.to_numeric(events["Donation Amount"], errors="coerce").fillna(0.0)

    # Join to lat/lon
    lookup["postcode"] = lookup["postcode"].astype(str).str.upper().str.strip()
    out = events.merge(lookup[["postcode","latitude","longitude","country"]], on="postcode", how="left")
    out = out.dropna(subset=["latitude","longitude"])
    out.to_csv(out_csv, index=False)
    print(f"Saved {out_csv} ({len(out):,} rows)")
