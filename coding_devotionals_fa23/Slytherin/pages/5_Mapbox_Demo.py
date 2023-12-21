
import streamlit as st
import pandas as pd
import plotly.express as px


px.set_mapbox_access_token("your token here")

raw = pd.read_parquet("./data/utah_georgia_places.parquet")

utah_georgia = raw[raw["websites"] == "lds.org"]

utah = utah_georgia[(utah_georgia["region"] == "UT")]
georgia = utah_georgia[(utah_georgia["region"] == "GA")]


# Title
st.title("")

# Header
st.header("")

# Create the drop-down list for the user to 
# choose what data to view
selection = st.selectbox(
    "Select State", 
    options = ["Utah", "Georgia", "Both"])

# Filter the data based on user selection
if selection == "Utah":
    dat = utah
elif selection == "Georgia":
    dat = georgia
else: 
    dat = utah_georgia

# Create the map visual
fig = px.scatter_mapbox(
    data_frame = dat, # Data
    lat = "latitude", 
    lon = "longitude",
    zoom = 4, # Zoom level - higher number means closer in
    color = "region", # What column's values are we basing the color on
    color_discrete_map = {
        # a dictionary to tell the map visual what colors to draw each value
        "UT": "red",
        "GA": "blue"
        },
    hover_data = ["location_name"] # Additional column's values to show in the tooltip
    )

# Make it so that the plot will actually show in the app
st.plotly_chart(fig)