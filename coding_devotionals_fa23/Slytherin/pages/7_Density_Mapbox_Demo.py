# Import required libraries

import plotly.express as px
import pandas as pd

# Read Parquet files into Pandas DataFrames
# patterns = pd.read_parquet("./data/patterns.parquet")
# places_idaho = pd.read_parquet('./data/places.parquet')

# Joining the DataFrames
# joined_df = patterns.merge(
#     places_idaho,
#     on="placekey",
#     how="inner"
# )
joined_df = pd.read_parquet("./data/joined_idaho.parquet")

# Filtering the DataFrame
joined_df = joined_df[joined_df["location_name"].str.contains("[L|l]atter|lds|LDS", regex=True)]


# Import streamlit Here ******

import streamlit as st

# Function to create a customized heatmap with Plotly using joined_df
def create_customized_heatmap(joined_df):
    # Extract relevant columns from joined_df
    heatmap_data = joined_df[['latitude', 'longitude', 'raw_visitor_counts']]

    # Create Plotly figure with customization
    fig = px.density_mapbox(
        heatmap_data,
        lat='latitude',
        lon='longitude',
        z='raw_visitor_counts',
        radius= 5,  # input a value here
        zoom= 4,  # input a value here to adjust the initial zoom level
        mapbox_style="stamen-terrain",
        title="Customized Heatmap of Raw Visitor Counts",
        labels={'raw_visitor_counts': 'Raw Visitor Counts'},
        opacity=0.7,  # Adjust the opacity for a more transparent effect
        center=dict(lat=heatmap_data['latitude'].mean(), lon=heatmap_data['longitude'].mean()),  # Center the map on the mean coordinates
        color_continuous_scale='Viridis',  # Use the Viridis color scale
        range_color=[0, 200],  # Set the color scale range
    )

    # Display the customized heatmap
    fig.show()

# Run the function with your joined_df
create_customized_heatmap(joined_df)