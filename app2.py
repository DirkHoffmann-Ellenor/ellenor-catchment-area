import streamlit as st
import pandas as pd
import pydeck as pdk
from datetime import datetime
import geopandas as gpd
import json

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(page_title="Postcode Coverage Map", layout="wide")
st.title("ellenor Data Map Explorer")

CATCHMENT_EAST = [
    "DA3", "DA11", "DA12", "DA13", "TN15"
]

CATCHMENT_WEST = [
    "DA1", "DA2",
    "DA4", "DA5", "DA6", "DA7", "DA8", "DA9", "DA10",
    "DA14", "DA15", "DA16", "DA17", "DA18",
    "BR8"
]

CATCHMENT_ALL = CATCHMENT_EAST + CATCHMENT_WEST

# ----------------------------------------------------
# 🎨 FIXED COLOUR MAP FOR DONATION SOURCES
# ----------------------------------------------------
DONATION_SOURCE_COLORS = {
    "LSPSWP":  [255, 165, 0, 220],   # Lottery Play money
    "LSPRDD":  [255, 140, 0, 220],   # Lottery play (variation)
    "REGSOL":  [0, 128, 255, 220],   # Regular Giving (solicited)
    "REGOLD":  [0, 102, 204, 220],   # Regular Giving (old)
    "IMOGEN":  [255, 105, 180, 220], # In Memory General
    "LSPLDD":  [255, 165, 0, 220],   # Lottery play money
    "LSPBBP":  [255, 165, 0, 220],   # Lottery play money
    "IMOMTR":  [219, 112, 147, 220], # Memory Tree
    "LOTDON":  [255, 165, 0, 220],   # Lottery donation
    "GDRTKT":  [50, 205, 50, 220],   # Prize Draw Tickets
    "LOLSOL":  [255, 215, 0, 220],   # Lights of Love
    "CFADON":  [30, 144, 255, 220],  # Community fundraising
    "TWIREG":  [138, 43, 226, 220],  # Twilight registration
    "APLSOL":  [0, 191, 255, 220],   # Appeal donations
    "TWISPO":  [148, 0, 211, 220],   # Twilight sponsorship
    "APLXMS":  [0, 255, 255, 220],   # Christmas Appeal
}

DEFAULT_SOURCE_COLOR = [200, 200, 200, 220]    # grey fallback

allmonths = []
# ----------------------------
# Data loading
# ----------------------------
@st.cache_data(show_spinner=True)
def load_data():
    """
    Load patient, donor and shop datasets + MSOA income parquet.
    Precompute income colours and GeoJSON for pydeck.
    """
    patients = pd.read_csv("postcode_coordinates.csv")
    donors = pd.read_csv("donation_events_geocoded.csv")
    shops = pd.read_csv("shops_geocoded.csv")
    
    # Normalise postcode column
    if "Postcode" in donors.columns and "postcode" not in donors.columns:
        donors.rename(columns={"Postcode": "postcode"}, inplace=True)

    # Parse Month_Year = "MM/YYYY"
    donors["Month_Year"] = donors["Month_Year"].astype(str)
    donors["month_dt"] = pd.to_datetime(donors["Month_Year"], format="%m/%Y", errors="coerce")
    donors = donors[donors["month_dt"].notna()].copy()

    # Filter to 2022+
    donors = donors[donors["month_dt"] >= pd.Timestamp("2022-01-01")].copy()

    # Year + timeline key
    donors["year"] = donors["month_dt"].dt.year
    donors["month"] = donors["month_dt"].dt.to_period("M").astype(str)

    # Donation Amount
    if "Total_Amount" in donors.columns:
        donors["Donation Amount"] = (
            donors["Total_Amount"]
            .astype(str)
            .str.replace(r"[£,]", "", regex=True)
            .astype(float)
            .fillna(0.0)
        )
    else:
        donors["Donation Amount"] = 0.0

    # Normalise Source / Application
    for col in donors.columns:
        if col.lower() == "source":
            donors.rename(columns={col: "Source"}, inplace=True)
        if col.lower() == "application":
            donors.rename(columns={col: "Application"}, inplace=True)

    if "Source" not in donors.columns:
        donors["Source"] = "Unknown"
    if "Application" not in donors.columns:
        donors["Application"] = "Unknown"

    # Extract postcode area
    for df in [patients, donors, shops]:
        if "postcode" not in df.columns and "Postcode" in df.columns:
            df.rename(columns={"Postcode": "postcode"}, inplace=True)

        df["postcode"] = df["postcode"].astype(str).str.strip()
        df["postcode_area"] = df["postcode"].str.extract(r"^([A-Z]{1,2})")

    # Unique donor postcodes
    donors_unique = (
        donors[["postcode", "latitude", "longitude", "country", "postcode_area"]]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates(subset=["postcode"])
        .reset_index(drop=True)
    )

    return patients, donors_unique, donors, shops


patients, donors_unique, donor_events, shops = load_data()

all_months = sorted(donor_events["month"].unique())

# ----------------------------
# Region mapping
# ----------------------------
REGION_GROUPS = {
    "London": ["EC","WC","E","N","NW","SE","SW","W"],
    "South East": ["BN","BR","CR","CT","DA","GU","HP","KT","ME","OX","PO","RG","RH","SL","SM","SO","TN","TW"],
    "South West": ["BA","BH","BS","EX","GL","PL","SN","SP","TA","TQ","TR"],
    "East of England": ["AL","CB","CM","CO","EN","IP","LU","NR","PE","SG"],
    "West Midlands": ["B","CV","DY","HR","ST","SY","TF","WR","WS","WV"],
    "East Midlands": ["DE","DN","LE","LN","NG","NN","SK","S"],
    "North West": ["BB","BL","CA","CH","CW","FY","LA","L","M","OL","PR","SK","WA","WN"],
    "Yorkshire & Humber": ["BD","DN","HD","HG","HU","HX","LS","S","WF","YO"],
    "North East": ["DH","DL","NE","SR","TS"],
    "Wales": ["CF","LD","LL","NP","SA","SY"],
    "Scotland": ["AB","DD","DG","EH","FK","G","HS","IV","KA","KW","KY","ML","PA","PH","TD","ZE"],
    "Northern Ireland": ["BT"]
}

# ----------------------------
# Login
# ----------------------------
def login():
    st.title("🔐 Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        if username in st.secrets["users"] and st.secrets["users"][username] == password:
            st.session_state["logged_in"] = True
            st.rerun()
        else:
            st.error("Invalid username or password")

if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    login()
    st.stop()
    
if "map_style" not in st.session_state:
    st.session_state["map_style"] = "mapbox://styles/mapbox/light-v11"

# ----------------------------
# Sidebar filters
# ----------------------------
st.sidebar.header("🔍 Filters")

# ----------------------------
# Layer visibility toggles
# ----------------------------
st.sidebar.subheader("🗺️ Show / Hide Layers")

show_patients = st.sidebar.checkbox("Show Patients", value=True)
show_donors = st.sidebar.checkbox("Show Donors", value=True)
show_shops = st.sidebar.checkbox("Show Shops", value=True)


st.sidebar.subheader("📅 Date Range")

start_month, end_month = st.sidebar.select_slider(
    "Select month range:",
    options=all_months,
    value=(all_months[0], all_months[-1])
)

st.sidebar.subheader("📍 Catchment Filter")
use_catchment = st.sidebar.checkbox(
    "Show ONLY ellenor catchment area",
    value=False
)

st.sidebar.subheader("📊 Differentiate Donor Sources")
Differentiate_Donor_Sources = st.sidebar.checkbox(
    "Differentiate Donor Sources on Map",
    value=False
)

# Donation range
st.sidebar.subheader("💷 Donation Amount Filter")
if not donor_events.empty:
    min_d, max_d = float(donor_events["Donation Amount"].min()), float(donor_events["Donation Amount"].max())
    min_input = st.sidebar.number_input("Min (£)", min_value=min_d, max_value=max_d, value=min_d)
    max_input = st.sidebar.number_input("Max (£)", min_value=min_input, max_value=max_d, value=max_d)
    donation_filter = (min_input, max_input)
else:
    donation_filter = (0, 0)

st.sidebar.subheader("🗺️ Map Style")

# Map style selector
map_styles = {
    "Satellite": "mapbox://styles/mapbox/satellite-v9",
    "Streets": "mapbox://styles/mapbox/streets-v12",
    "Light": "mapbox://styles/mapbox/light-v11",
    "Dark": "mapbox://styles/mapbox/dark-v11",
    "Outdoors": "mapbox://styles/mapbox/outdoors-v12",
    "Satellite Streets": "mapbox://styles/mapbox/satellite-streets-v12"
}

selected_style = st.sidebar.selectbox(
    "Choose map style:",
    options=list(map_styles.keys()),
    index=0  # Default to Satellite
)

map_style_url = map_styles[selected_style]

# Country
country_all = sorted(
    set(patients["country"].dropna())
    | set(donors_unique["country"].dropna())
    | set(donor_events["country"].dropna())
)
country_filter = st.sidebar.multiselect("Country:", country_all, default=country_all)

# Region → postcode areas
region_filter = st.sidebar.multiselect("UK Region:", list(REGION_GROUPS.keys()), default=list(REGION_GROUPS.keys()))
allowed_postcode_areas = [x for r in region_filter for x in REGION_GROUPS[r]]

def apply_filters(df):
    
    base = df[
        (df["country"].isin(country_filter)) &
        (df["postcode_area"].isin(allowed_postcode_areas))
    ].copy()
    
    # Apply catchment toggle
    if use_catchment:
        prefixes = tuple([p.upper().replace(" ", "") for p in CATCHMENT_ALL])
        postcode_norm = base["postcode"].astype(str).str.upper().str.replace(r"\s+", "", regex=True)
        base = base[postcode_norm.str.startswith(prefixes)].copy()

    # Donation filter
    if "Donation Amount" in base.columns:
        base = base[
            (base["Donation Amount"] >= donation_filter[0]) &
            (base["Donation Amount"] <= donation_filter[1])
        ].copy()
    
    # 📌 NEW: Month range filter
    if "month" in base.columns:
        base = base[(base["month"] >= start_month) & (base["month"] <= end_month)].copy()

    return base


pf = apply_filters(patients)
de = apply_filters(donor_events)
shops = apply_filters(shops)

# ---- Aggregation for tooltip ----
# Group donor events by postcode for the filtered time period
donor_summary = (
    de.groupby("postcode")
      .agg(
          total_events=("Total_Amount", "count"),
          max_donation=("Total_Amount", "max"),
          total_donation=("Total_Amount", "sum"),
          donor_type=("Donor_Type", lambda x: ", ".join(sorted(set(x))))
      )
      .reset_index()
)


# ----------------------------
# Timeline toggle
# ----------------------------
st.sidebar.subheader("🎞 Donor display mode")
show_timeline = not st.sidebar.checkbox("Show all donors at once (hide timeline)", value=True)

# ----------------------------
# Metrics
# ----------------------------
st.markdown("### 📊 Donation Summary")
st.metric("Total Donations", f"£{de['Donation Amount'].sum():,.2f}")



def create_pydeck_map(df_pat, df_don, df_shop, timeline_month=None,
                      show_patients=True, show_donors=True, show_shops=True, map_style="mapbox://styles/mapbox/satellite-v9", differentiate_donor_sources=False):


    # Timeline filtering
    if timeline_month is not None:
        df_don = df_don[df_don["month"] == timeline_month].copy()


    # Work on copies so we don't mutate original dataframes
    df_pat = df_pat.copy()
    df_don = df_don.copy()
    df_shop = df_shop.copy()

    # Patients
    if show_patients and not df_pat.empty:
        df_pat["kind"] = "Patient"
        df_pat["extra"] = ""  # nothing more to show (you can add more if you like)


    # Merge aggregated donor info
    if show_donors and not df_don.empty:
        df_don = df_don.merge(donor_summary, on="postcode", how="left")
        
        
    # Donors
    if show_donors and not df_don.empty:
        df_don["kind"] = "Donor"
        df_don["extra"] = (
            "Donor Type: " + df_don["donor_type"].astype(str) +
            "<br/>Source: " + df_don["Source"].astype(str) +                                 
            "<br/>Total Donation Amount: £" + df_don["total_donation"].round(2).astype(str) +
            "<br/>Max Single Donation: £" + df_don["max_donation"].round(2).astype(str) +
            "<br/>Number of Donations: " + df_don["total_events"].astype(int).astype(str) +
            "<br/>Latest Month: " + df_don["month"].astype(str) +
            "<br/>Latest Donation: £" + df_don["Donation Amount"].round(2).astype(str)
        )
        
    if differentiate_donor_sources:
        # map sources to colours
        df_don["color"] = df_don["Source"].map(DONATION_SOURCE_COLORS)

        # fallback for unknown or missing sources
        df_don["color"] = df_don["color"].apply(
            lambda x: x if isinstance(x, list) else DEFAULT_SOURCE_COLOR
        )
    else:
        # simple default colour (blue)
        df_don["color"] = [[0, 128, 255, 200]] * len(df_don)



    # Shops
    if show_shops and not df_shop.empty:
        df_shop["kind"] = "Shop"
        df_shop["extra"] = "Name: " + df_shop["name"].astype(str)

    # Combine coords to find centre
    combined = pd.concat([
        df_pat[["latitude","longitude"]],
        df_don[["latitude","longitude"]],
        df_shop[["latitude","longitude"]],
    ]).dropna()

    if combined.empty:
        return None

    center = [
        combined["latitude"].mean(),
        combined["longitude"].mean()
    ]

    layers = []

    # Patients
    if show_patients and not df_pat.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df_pat,
            get_position="[longitude, latitude]",
            get_radius=80,
            get_fill_color=[255, 0, 0, 180],
            pickable=True
        ))



    # Donors
    if show_donors and not df_don.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df_don,
            get_position="[longitude, latitude]",
            get_radius=80,
            get_fill_color="color",
            pickable=True
        ))

    # Shops
    if show_shops and not df_shop.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer",
            data=df_shop,
            get_position="[longitude, latitude]",
            get_radius=100,
            get_fill_color=[0, 255, 0, 180],
            pickable=True
        ))

    # Heatmap (donors)
    if show_donors and not df_don.empty:
        layers.append(pdk.Layer(
            "HeatmapLayer",
            data=df_don,
            get_position="[longitude, latitude]",
            get_weight="Donation Amount",
            radiusPixels=60,
        ))
        
    view_state = pdk.ViewState(
        latitude=center[0],
        longitude=center[1],
        zoom=6,
        pitch=0,
    )

    # ---- Global tooltip template (works for all layers) ----
    tooltip = {
        "html": "<b>{kind}</b><br/>Postcode: {postcode}<br/>{extra}",
        "style": {
            "color": "white",
            "backgroundColor": "rgba(0, 0, 0, 0.7)"
        }
    }

    return pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style=map_style,
        api_keys={"mapbox": st.secrets["MAPBOX_TOKEN"]["MAPBOX_TOKEN"]},
        tooltip=tooltip,
        parameters={"clearColor": [1.0, 1.0, 1.0, 1.0]},
    )

# ----------------------------
# Timeline UI
# ----------------------------
if show_timeline:
    timeline_month = st.select_slider(
        "Select month",
        options=sorted(de["month"].unique()),
        value=sorted(de["month"].unique())[0]
    )
else:
    timeline_month = None


deck_map = create_pydeck_map(pf, de, shops, timeline_month, show_donors=show_donors, show_patients=show_patients, show_shops=show_shops, map_style=map_style_url, differentiate_donor_sources=Differentiate_Donor_Sources)

# ----------------------------
# UI + map
# ----------------------------
if deck_map:
    st.pydeck_chart(deck_map, height=800)
else:
    st.warning("No data to show — adjust filters.")

# ----------------------------
# Download section
# ----------------------------
st.subheader("⬇️ Download filtered donor data")
st.download_button(
    "Download donor events (filtered)",
    data=de.to_csv(index=False),
    file_name="donor_events_filtered.csv",
    mime="text/csv"
)
