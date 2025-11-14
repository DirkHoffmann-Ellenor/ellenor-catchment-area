import streamlit as st
import pandas as pd
import streamlit.components.v1 as components
import folium
from folium import plugins
from datetime import datetime
import json

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(page_title="Postcode Coverage Map", layout="wide")
st.title("🏥 Patient + Donor Geographical Coverage Explorer")

# ----------------------------
# Data loading
# ----------------------------
@st.cache_data
def load_data():
    """
    Loads three CSVs:
      - postcode_coordinates.csv               (patients; columns: postcode, latitude, longitude, country)
      - donor_postcode_coordinates.csv         (unique donor postcodes; columns: postcode, latitude, longitude, country)
      - donor_events_geocoded.csv              (each donation; columns: postcode, latitude, longitude, Date, Donation Amount[, month])

    Enrich:
      - postcode_area (first 1–2 letters)
      - month (YYYY-MM) derived from Date if missing
      - Donation Amount numeric (strip symbols/commas)
    """
    patients = pd.read_csv("postcode_coordinates.csv")
    donors_unique = pd.read_csv("donor_postcode_coordinates.csv")
    donor_events = pd.read_csv("donor_events_geocoded.csv")
    shops = pd.read_csv("shops_geocoded.csv")

    # postcode area (first 1–2 letters)
    for df in [patients, donors_unique, donor_events, shops]:
        df["postcode"] = df["postcode"].astype(str).str.strip()
        df["postcode_area"] = df["postcode"].str.extract(r"^([A-Z]{1,2})")

    # --- Date → month parsing (robust DD/MM/YYYY) ---
    if "month" not in donor_events.columns or donor_events["month"].isna().any():
        donor_events["__date"] = pd.to_datetime(
            donor_events["Date"].astype(str).str.strip(),
            dayfirst=True,
            errors="coerce"
        )
        donor_events = donor_events[donor_events["__date"].notna()].copy()
        donor_events["month"] = donor_events["__date"].dt.to_period("M").astype(str)  # YYYY-MM

    # --- Donation Amount to numeric ---
    if "Donation Amount" in donor_events.columns:
        donor_events["Donation Amount"] = (
            donor_events["Donation Amount"]
            .astype(str)
            .str.replace(r"[£,]", "", regex=True)
            .astype(float)
        )
        donor_events["Donation Amount"] = pd.to_numeric(
            donor_events["Donation Amount"], errors="coerce"
        ).fillna(0.0)
    else:
        donor_events["Donation Amount"] = 0.0

    return patients, donors_unique, donor_events, shops

patients, donors_unique, donor_events, shops = load_data()

# ----------------------------
# Region group mapping
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
    
        
# ----------------------------
# Sidebar filters (NO month UI here)
# ----------------------------
st.sidebar.header("🔍 Filter Options")

# Country filter across both datasets
country_all = sorted(set(patients["country"].dropna()).union(set(donors_unique["country"].dropna())).union(set(donor_events["country"].dropna())))
country_filter = st.sidebar.multiselect("Country:", country_all, default=country_all)

# Region → postcode areas
region_filter = st.sidebar.multiselect("UK Region:", list(REGION_GROUPS.keys()), default=list(REGION_GROUPS.keys()))
allowed_postcode_areas = []
for r in region_filter:
    allowed_postcode_areas.extend(REGION_GROUPS[r])

def apply_filters(df, donation_range=None):
    filtered = df[
        (df["country"].isin(country_filter)) &
        (df["postcode_area"].isin(allowed_postcode_areas))
    ]

    # Apply donation range only if donations column exists
    if donation_range and "Donation Amount" in df.columns:
        min_don, max_don = donation_range
        filtered = filtered[
            (filtered["Donation Amount"] >= min_don) &
            (filtered["Donation Amount"] <= max_don)
        ]

    return filtered.copy()




# ----------------------------
# Layer toggles / mode
# ----------------------------
st.sidebar.subheader("🧭 Layers")
show_patients_points = st.sidebar.checkbox("Patients — Points (Red)", True)
show_patients_heat   = st.sidebar.checkbox("Patients — Heat", True)
show_donors_points   = st.sidebar.checkbox("Donors — Points (Blue)", True)
show_donors_heat     = st.sidebar.checkbox("Donors — Heat", True)
show_shops_points    = st.sidebar.checkbox("Shops — Points (Green)", True)

# ----------------------------
# Donation Amount Filter (Improved)
# ----------------------------
st.sidebar.subheader("💷 Donation Amount Filter")

if not donor_events.empty:
    min_donation = float(donor_events["Donation Amount"].min())
    max_donation = float(donor_events["Donation Amount"].max())

    col_min, col_max = st.sidebar.columns(2)

    with col_min:
        min_input = st.number_input(
            "Min (£)",
            min_value=0.0,
            max_value=max_donation,
            value=0.0,
            step=1.0,
        )

    with col_max:
        max_input = st.number_input(
            "Max (£)",
            min_value=min_input,      # ensures max >= min
            max_value=max_donation,
            value=max_donation,
            step=1.0,
        )

    donation_filter = (min_input, max_input)

    # 🔥 Live count of donors matching the range
    count_in_range = donor_events[
        (donor_events["Donation Amount"] >= min_input) &
        (donor_events["Donation Amount"] <= max_input)
    ].shape[0]

    st.sidebar.markdown(
        f"**Donor events in this range:** {count_in_range:,}"
    )

else:
    donation_filter = (0.0, 0.0)
    st.sidebar.info("No donor data available.")


pf = apply_filters(patients)
du = apply_filters(donors_unique)
de = apply_filters(donor_events, donation_filter)
shops = apply_filters(shops)


# ----------------------------
# Timeline mode toggle
# ----------------------------
st.sidebar.subheader("🎞 Donor display mode")
show_all_donors = st.sidebar.checkbox(
    "Show all donors at once (hide timeline)",
    value=True,
    help=("When checked, the map shows every donor event simultaneously and the timeline control is removed. "
          "When unchecked, an on-map timeline animates donors month-by-month.")
)

# ----------------------------
# Metrics
# ----------------------------
total_donations = float(de["Donation Amount"].sum()) if not de.empty else 0.0
st.markdown("### 📊 Donation Summary")
st.metric("Total Donations", f"£{total_donations:,.2f}")

# ----------------------------
# Map builder
# ----------------------------
def create_map():
    # Use filtered datasets for map center
    combined_for_center = pd.concat(
        [pf[["latitude","longitude"]], du[["latitude","longitude"]], de[["latitude","longitude"]], shops[["latitude","longitude"]]],
        ignore_index=True
    ).dropna()
    if combined_for_center.empty:
        return None

    m = folium.Map(
        location=[combined_for_center["latitude"].mean(), combined_for_center["longitude"].mean()],
        zoom_start=6
    )

    # -------- Patients --------
    if show_patients_heat and not pf.empty:
        heat_pat = pf[["latitude","longitude"]].dropna().values.tolist()
        if len(heat_pat) > 0:
            plugins.HeatMap(
                heat_pat,
                radius=18, blur=14,
                gradient={0.2: 'yellow', 0.4: 'orange', 0.6: 'red', 1.0: 'darkred'},
                name="Patients Heat"
            ).add_to(m)

    if show_patients_points and not pf.empty:
        for _, row in pf.iterrows():
            folium.CircleMarker(
                [row.latitude, row.longitude],
                radius=4,
                color="red", fill=True, fill_opacity=0.85,
                popup=f"{row.get('postcode','')}"
            ).add_to(m)

    # -------- Shops --------
    if show_shops_points and not shops.empty:
        for _, row in shops.iterrows():
            folium.CircleMarker(
                [row.latitude, row.longitude],
                radius=4,
                color="green", fill=True, fill_opacity=0.85,
                popup=f"{row.get('name','')} ({row.get('postcode','')})"
            ).add_to(m)

    # -------- Donors (two mutually-exclusive modes) --------
    if show_all_donors:
        # STATIC DONOR POINT MODE (NO TIMELINE)
        if show_donors_heat and not de.empty:
            heat_don = de[["latitude","longitude"]].dropna().values.tolist()
            if len(heat_don) > 0:
                plugins.HeatMap(
                    heat_don,
                    radius=18, blur=14,
                    gradient={0.2: 'lightblue', 0.6: 'blue', 1.0: 'navy'},
                    name="Donors Heat"
                ).add_to(m)

        if show_donors_points and not de.empty:
            for _, row in de.dropna(subset=["latitude","longitude"]).iterrows():
                popup_html = f"""
                <b>Postcode:</b> {row.get('postcode','')}<br>
                <b>Month:</b> {row.get('month','')}<br>
                <b>Donation Amount:</b> £{row.get('Donation Amount',0)}
                """
                folium.CircleMarker(
                    [row.latitude, row.longitude],
                    radius=4,
                    color="blue", fill=True, fill_opacity=0.9,
                    popup=popup_html,
                    tooltip=f"{row.get('postcode','')} (£{row.get('Donation Amount',0)})"
                ).add_to(m)

    else:
        # TIMELINE MODE (DYNAMIC MONTH SLIDER)
        if show_donors_heat and not de.empty:
            heat_don = de[["latitude","longitude"]].dropna().values.tolist()
            if len(heat_don) > 0:
                plugins.HeatMap(
                    heat_don,
                    radius=18, blur=14,
                    gradient={0.2: 'lightblue', 0.6: 'blue', 1.0: 'navy'},
                    name="Donors Heat (all months)"
                ).add_to(m)

        # --- Build timeline feature collection ---
        def month_to_date(month_str):
            try:
                return pd.to_datetime(str(month_str) + "-01").strftime("%Y-%m-%d")
            except:
                return "1970-01-01"
            
        features = []
        for _, row in de.dropna(subset=["latitude","longitude","month"]).iterrows():
            popup_html = (
                f"<b>Postcode:</b> {row.get('postcode','')}<br>"
                f"<b>Month:</b> {row.get('month','')}<br>"
                f"<b>Donation Amount:</b> £{row.get('Donation Amount',0)}"
            )
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(row["longitude"]), float(row["latitude"])]},
                "properties": {
                    "time": month_to_date(row["month"]),
                    "style": {"color": "blue", "fillColor": "blue", "radius": 4},
                    "icon": "circle",
                    "popup": popup_html,   # keep the popup content
                },
            })

        tgjson = plugins.TimestampedGeoJson(
            {
                "type": "FeatureCollection",
                "features": features
            },
            period="P1M",
            add_last_point=True,
            auto_play=False,
            loop=False,
            max_speed=1,
            loop_button=True,
            date_options="YYYY-MM",
            time_slider_drag_update=True,
            duration="P1M"
        )
        tgjson.add_to(m)

        # --- Bind popups robustly for all timeline layers (rebind on time events) ---
        m.get_root().html.add_child(folium.Element("""
        <script>
        (function () {
        function findMap(cb) {
            function tryFind() {
            for (const k in window) {
                if (window[k] instanceof L.Map) { cb(window[k]); return; }
            }
            setTimeout(tryFind, 200);
            }
            tryFind();
        }

        function bindPopupsDeep(layer) {
            if (!layer) return;

            // If this layer is a feature with popup content, bind it (once)
            if (layer.feature && layer.feature.properties && layer.feature.properties.popup) {
            const hasPopup = (typeof layer.getPopup === 'function') && !!layer.getPopup();
            if (!hasPopup && typeof layer.bindPopup === 'function') {
                layer.bindPopup(layer.feature.properties.popup);
            }
            }

            // Recurse into children (covers LayerGroup, GeoJSON, TimeDimension slices, etc.)
            if (typeof layer.eachLayer === 'function') {
            layer.eachLayer(function (child) { bindPopupsDeep(child); });
            } else if (layer._layers) {
            Object.values(layer._layers).forEach(bindPopupsDeep);
            }
        }

        findMap(function (map) {
            function bindAll() { map.eachLayer(bindPopupsDeep); }

            // Initial bind for currently-visible layers
            bindAll();

            // Re-bind whenever the time changes or a new time slice loads
            if (map.timeDimension) {
            map.timeDimension.on('timeload', bindAll);
            map.timeDimension.on('timechanged', bindAll);
            }

            // Also re-bind when layers are added (e.g., toggling overlays)
            map.on('layeradd', function () { setTimeout(bindAll, 0); });
        });
        })();
        </script>
        """))




        # --- MONTH TOTALS + FLOATING BOX (ONLY HERE) ---
        month_totals_map = (
            de.groupby("month")["Donation Amount"].sum().round(2).to_dict()
        )
        month_totals_js = json.dumps(month_totals_map)

        m.get_root().html.add_child(folium.Element(f"""
        <style>
        #monthDonationBox {{
            position: absolute;
            top: 80px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 999999;
            background: white;
            padding: 8px 14px;
            border-radius: 6px;
            border: 1px solid #ccc;
            font-size: 16px;
            font-weight: 600;
            box-shadow: 0 2px 6px rgba(0,0,0,0.2);
        }}
        </style>

        <div id="monthDonationBox">Total donations this month: £0.00</div>

        <script>
        const MONTH_TOTALS = {month_totals_js};

        function fmtGBP(num) {{
            return Number(num || 0).toLocaleString('en-GB', {{
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
            }});
        }}

        function monthKeyFromMillis(ms) {{
            const d = new Date(ms);
            if (isNaN(d)) return null;
            return d.toISOString().slice(0, 7);
        }}

        function updateDonationBox(map) {{
            const ms = map.timeDimension.getCurrentTime();
            const key = monthKeyFromMillis(ms);
            const val = MONTH_TOTALS[key] || 0;
            document.getElementById('monthDonationBox').innerHTML =
            "Total donations this month: £" + fmtGBP(val);
        }}

        function findMap() {{
            for (const k in window) {{
            if (window[k] instanceof L.Map) return window[k];
            }}
            return null;
        }}

        function init() {{
            const map = findMap();
            if (!map || !map.timeDimension) {{
            setTimeout(init, 300);
            return;
            }}
            updateDonationBox(map);
            map.timeDimension.on('timechanged', () => updateDonationBox(map));
            map.timeDimension.on('timeload', () => updateDonationBox(map));
        }}
        document.addEventListener('DOMContentLoaded', init);
        </script>
        """))

    if show_shops_points and not shops.empty:
        for _, row in shops.dropna(subset=["latitude","longitude"]).iterrows():
            folium.CircleMarker(
                [row.latitude, row.longitude],
                radius=4,
                color="green", fill=True, fill_opacity=0.9,
                popup=f"{row.get('name','')} ({row.get('postcode','')})"
            ).add_to(m)

    folium.LayerControl().add_to(m)
    return m


m = create_map()

# ----------------------------
# UI + map
# ----------------------------
st.markdown("### Displaying:")
col1, col2, col3 = st.columns(3)
with col1:
    st.write(f"🔴 Patients (unique): **{len(pf):,}**")
with col2:
    st.write(f"📍 Donor postcodes (unique): **{len(du):,}**")
with col3:
    st.write(f"🧾 Donor events (current filters): **{len(de):,}**")

if m:
    m.save("temp_filtered_map.html")
    components.html(open("temp_filtered_map.html", "r", encoding="utf-8").read(), height=740, scrolling=True)
else:
    st.warning("No data to show — adjust filters.")

# ----------------------------
# Optional data views + download
# ----------------------------
with st.expander("Patient data (filtered)"):
    st.dataframe(pf)

with st.expander("Donor events (filtered by country/region)"):
    st.dataframe(de)

st.download_button(
    "⬇️ Download donor events (filtered)",
    data=de.to_csv(index=False),
    file_name="donor_events_filtered.csv",
    mime="text/csv"
)
