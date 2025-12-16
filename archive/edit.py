import pandas as pd
from pathlib import Path

input_path = Path("donation_events_geocoded_2.csv")
output_path = Path("donation_events_geocoded_3.csv")

df = pd.read_csv(input_path)

# Ensure non-numeric/empty values become NaN, then keep only rows where both lat and lon are present
lat = pd.to_numeric(df['latitude'], errors='coerce')
lon = pd.to_numeric(df['longitude'], errors='coerce')
df_clean = df[lat.notna() & lon.notna()].copy()

df_clean.to_csv(output_path, index=False)