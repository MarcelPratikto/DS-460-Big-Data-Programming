# %%
#documentation for folium: https://python-visualization.github.io/folium/latest/index.html
import streamlit as st
import folium
import pandas as pd
from streamlit_folium import folium_static

# %%
idaho_places = pd.read_parquet("./data/idaho_places.parquet")

st.title("DEMO: Folium")


# %%
lds_churches = idaho_places[idaho_places['location_name'] == "The Church of Jesus Christ of Latter day Saints"] 

# %%
# example code
# the dataset "bike_station_locations" contains 3 columns: Latitude, Longitude and Name
map = folium.Map(location=[lds_churches.latitude.mean(), lds_churches.longitude.mean()], zoom_start=14, control_scale=True)
for index, location_info in lds_churches.iterrows():
   folium.Marker([location_info["latitude"], location_info["longitude"]], popup=location_info["city"]).add_to(map)

# %%
folium_static(map)
# %%

#type streamlit run folium_demo.py in terminal to render streamlit application