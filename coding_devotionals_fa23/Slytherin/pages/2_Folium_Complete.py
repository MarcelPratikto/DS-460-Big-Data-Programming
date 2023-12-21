
import streamlit as st
import folium
import pandas as pd
from streamlit_folium import folium_static

idaho_places = pd.read_parquet("./data/idaho_places.parquet")

lds_churches = idaho_places[idaho_places['location_name'] == "The Church of Jesus Christ of Latter day Saints"] 

# Adjust the zoom_start value to a more appropriate level
map = folium.Map(location=[lds_churches.latitude.mean(), lds_churches.longitude.mean()], zoom_start=8, control_scale=True)

for index, location_info in lds_churches.iterrows():
    folium.Marker([location_info["latitude"], location_info["longitude"]], popup=location_info["city"]).add_to(map)

# Render the map in Streamlit
folium_static(map)
