import geopandas as gpd

INCOME_FIELD = "Net annual income (£)"  # change if your column name differs
gdf = gpd.read_parquet("msoa_income_age_merged.parquet")
print(gdf.head())
print(gdf.columns)
print(gdf.geometry.head())
print(gdf[INCOME_FIELD].describe())
