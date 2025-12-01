import pandas as pd
import requests
import time
from pathlib import Path

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FILE = "donation_results_2.csv"
OUTPUT_FILE = "donation_events_geocoded_2.csv"
CACHE_FILE = "postcode_cache.csv"  # Stores all postcodes we've ever looked up

def get_postcode_coordinates(postcode):
    """
    Look up a single UK postcode via postcodes.io
    Returns dict with postcode, latitude, longitude, etc. or None if failed
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
    except Exception as e:
        print(f"⚠️ Error fetching {postcode}: {e}")
    return None

def load_postcode_cache(cache_file):
    """
    Load existing postcode cache into a dictionary (hash map)
    """
    cache = {}
    if Path(cache_file).exists():
        df = pd.read_csv(cache_file)
        for _, row in df.iterrows():
            cache[row["postcode"]] = {
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "admin_district": row.get("admin_district", ""),
                "admin_county": row.get("admin_county", ""),
                "country": row.get("country", "")
            }
        print(f"✅ Loaded {len(cache):,} postcodes from cache")
    return cache


def save_postcode_cache(cache, cache_file):
    """
    Save the postcode cache dictionary back to CSV
    """
    records = []
    for pc, data in cache.items():
        records.append({
            "postcode": pc,
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "admin_district": data.get("admin_district", ""),
            "admin_county": data.get("admin_county", ""),
            "country": data.get("country", "")
        })
    df = pd.DataFrame(records)
    df.to_csv(cache_file, index=False)
    print(f"✅ Saved {len(df):,} postcodes to cache")


def geocode_donation_events(input_file, output_file, cache_file, polite_delay=0.08):
    """
    Main function: reads donation_events.csv, geocodes postcodes efficiently using cache,
    and saves output with lat/lon columns added
    """
    # Load the donation events
    print(f"📂 Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Normalize column names
    df.columns = [c.strip() for c in df.columns]
    
    # Check for Postcode column (try common variations)
    postcode_col = None
    for col in df.columns:
        if col.lower() in ["postcode", "post code", "postal code"]:
            postcode_col = col
            break
    
    if postcode_col is None:
        raise ValueError(f"Could not find a postcode column. Available columns: {list(df.columns)}")
    
    print(f"✅ Found postcode column: '{postcode_col}'")
    print(f"📊 Total rows: {len(df):,}")
    
    # Clean postcodes
    df["postcode_clean"] = df[postcode_col].astype(str).str.strip().str.upper()
    df["postcode_clean"] = df["postcode_clean"].replace({"NAN": "", "": pd.NA})
    
    # Count unique postcodes
    unique_postcodes = df["postcode_clean"].dropna().unique()
    print(f"🔍 Unique postcodes to geocode: {len(unique_postcodes):,}")
    
    # Load cache (hash map)
    cache = load_postcode_cache(cache_file)
    
    # Find postcodes we need to fetch
    postcodes_to_fetch = [pc for pc in unique_postcodes if pc not in cache]
    print(f"🌐 Need to fetch from API: {len(postcodes_to_fetch):,}")
    print(f"⚡ Already in cache: {len(unique_postcodes) - len(postcodes_to_fetch):,}")
    
    # Fetch missing postcodes
    if postcodes_to_fetch:
        print(f"\n🔄 Fetching {len(postcodes_to_fetch):,} postcodes from postcodes.io...")
        for i, pc in enumerate(postcodes_to_fetch):
            if i % 50 == 0 and i > 0:
                print(f"   Progress: {i}/{len(postcodes_to_fetch)} ({i*100//len(postcodes_to_fetch)}%)")
            
            result = get_postcode_coordinates(pc)
            if result:
                cache[pc] = {
                    "latitude": result["latitude"],
                    "longitude": result["longitude"],
                    "admin_district": result.get("admin_district", ""),
                    "admin_county": result.get("admin_county", ""),
                    "country": result.get("country", "")
                }
            time.sleep(polite_delay)  # Be polite to the API
        
        # Save updated cache
        save_postcode_cache(cache, cache_file)
    
    # Map coordinates to dataframe using the cache (hash map lookup - O(1))
    print(f"\n📍 Adding coordinates to all rows...")
    df["latitude"] = df["postcode_clean"].map(lambda pc: cache.get(pc, {}).get("latitude"))
    df["longitude"] = df["postcode_clean"].map(lambda pc: cache.get(pc, {}).get("longitude"))
    df["admin_district"] = df["postcode_clean"].map(lambda pc: cache.get(pc, {}).get("admin_district", ""))
    df["admin_county"] = df["postcode_clean"].map(lambda pc: cache.get(pc, {}).get("admin_county", ""))
    df["country"] = df["postcode_clean"].map(lambda pc: cache.get(pc, {}).get("country", ""))
    
    # Drop the temporary clean column
    df = df.drop(columns=["postcode_clean"])
    
    # Stats
    total_rows = len(df)
    geocoded_rows = df["latitude"].notna().sum()
    failed_rows = total_rows - geocoded_rows
    
    print(f"\n📊 Results:")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Successfully geocoded: {geocoded_rows:,} ({geocoded_rows*100//total_rows}%)")
    print(f"   Failed/missing: {failed_rows:,}")
    
    # Save output
    df.to_csv(output_file, index=False)
    print(f"\n✅ Saved geocoded file: {output_file}")
    print(f"   Columns added: latitude, longitude, admin_district, admin_county, country")


if __name__ == "__main__":
    geocode_donation_events(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        cache_file=CACHE_FILE
    )
    print("\n🎉 All done!")