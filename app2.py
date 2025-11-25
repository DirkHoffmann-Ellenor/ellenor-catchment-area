import streamlit as st
import pandas as pd
import pydeck as pdk
from datetime import datetime

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(page_title="Postcode Coverage Map", layout="wide")
st.title("🏥 Patient + Donor Geographical Coverage Explorer (FAST)")

# ----------------------------
# Data loading
# ----------------------------
@st.cache_data
def load_data():
    """
    Load patient, donor and shop datasets.
    Convert Month_Year (MM/YYYY) → month_dt + YYYY-MM.
    Keep only events from 2022 onward.
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
    username = "DirkHoffmann"  # For testing only; remove in production
    password = "H0ffmann123"  # For testing only; remove in production
    if st.button("Login"):
        if username in st.secrets["users"] and st.secrets["users"][username] == password:
            st.session_state["logged_in"] = True
            st.rerun()
        else:
            st.error("Invalid username or password")

if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    login()
    st.stop()

# ----------------------------
# Sidebar filters
# ----------------------------
st.sidebar.header("🔍 Filters")

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

# Donation range
st.sidebar.subheader("💷 Donation Amount Filter")
if not donor_events.empty:
    min_d, max_d = float(donor_events["Donation Amount"].min()), float(donor_events["Donation Amount"].max())
    min_input = st.sidebar.number_input("Min (£)", min_value=min_d, max_value=max_d, value=min_d)
    max_input = st.sidebar.number_input("Max (£)", min_value=min_input, max_value=max_d, value=max_d)
    donation_filter = (min_input, max_input)
else:
    donation_filter = (0, 0)

def apply_filters(df):
    base = df[
        (df["country"].isin(country_filter)) &
        (df["postcode_area"].isin(allowed_postcode_areas))
    ]
    if "Donation Amount" in base.columns:
        return base[
            (base["Donation Amount"] >= donation_filter[0]) &
            (base["Donation Amount"] <= donation_filter[1])
        ].copy()
    return base.copy()

pf = apply_filters(patients)
du = apply_filters(donors_unique)
de = apply_filters(donor_events)
shops = apply_filters(shops)

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

# ----------------------------
# Pydeck map builder
# ----------------------------
def create_pydeck_map(df_pat, df_don, df_shop, timeline_month=None):
    
    # Timeline filter
    if timeline_month:
        df_don = df_don[df_don["month"] == timeline_month]

    # Center map
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
    
    osm_layer = pdk.Layer(
    "TileLayer",
    data=None,
    tile_size=256,
    min_zoom=0,
    max_zoom=19,
    opacity=1.0,
    tile_url_template="https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    )


    layers = [osm_layer]

    # Patients
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=df_pat,
        get_position="[longitude, latitude]",
        get_radius=80,
        get_fill_color=[255, 0, 0, 160],
        pickable=True
    ))

    # Donors
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=df_don,
        get_position="[longitude, latitude]",
        get_radius=80,
        get_fill_color=[0, 0, 255, 160],
        pickable=True
    ))

    # Shops
    layers.append(pdk.Layer(
        "ScatterplotLayer",
        data=df_shop,
        get_position="[longitude, latitude]",
        get_radius=100,
        get_fill_color=[0, 255, 0, 160],
        pickable=True
    ))

    # Heatmap
    if not df_don.empty:
        layers.append(pdk.Layer(
            "HeatmapLayer",
            data=df_don,
            get_position="[longitude, latitude]",
            get_weight="Donation Amount",
            radiusPixels=60,
        ))

    view = pdk.ViewState(
        latitude=center[0],
        longitude=center[1],
        zoom=6,
        pitch=0,
    )

    tooltip = {
        "html": "<b>{postcode}</b><br/>Donation: £{Donation Amount}<br/>Month: {month}",
        "style": {"color": "white"}
    }
    
    return pdk.Deck(
        layers=layers,
        initial_view_state=view,
        tooltip=tooltip,
        parameters={"clearColor": [1.0, 1.0, 1.0, 1.0]}  # <<< FIX: white background
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

deck_map = create_pydeck_map(pf, de, shops, timeline_month)

# ----------------------------
# UI + map
# ----------------------------
if deck_map:
    st.pydeck_chart(deck_map)
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
