import pandas as pd

data = pd.read_csv("donation_events_geocoded.csv")

# ("Counts in full dataset:")
print(data["Source"].value_counts().head(50))



