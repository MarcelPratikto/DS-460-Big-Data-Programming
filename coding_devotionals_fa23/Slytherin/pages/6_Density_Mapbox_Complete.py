# Import required libraries
import streamlit as st
import plotly.express as px
import pandas as pd


# px.set_mapbox_access_token("pk.eyJ1IjoiZHBlY2s3NiIsImEiOiJjbG95djNiYmowNm80MnFwZ2dtM3R6MnZ1In0.XfZ48C-Gi00KJAUgrAckIQ")
# Read Parquet files into Pandas DataFrames
# patterns = pd.read_parquet("./data/patterns.parquet")
# places_idaho = pd.read_parquet('./data/places.parquet')

# # Joining the DataFrames
# joined_df = patterns.merge(
#     places_idaho,
#     on="placekey",
#     how="inner"
# )

joined_df = pd.read_parquet("../data/joined_idaho.parquet")

# Filtering the DataFrame
joined_df = joined_df[joined_df["location_name"].str.contains("[L|l]atter|lds|LDS", regex=True)]


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
        radius=5,  # Set a constant radius for all points
        zoom=4,  # Adjust the initial zoom level
        mapbox_style="stamen-terrain",
        title="Customized Heatmap of Raw Visitor Counts",
        labels={'raw_visitor_counts': 'Raw Visitor Counts'},
        center=dict(lat=heatmap_data['latitude'].mean(), lon=heatmap_data['longitude'].mean()),  # Center the map on the mean coordinates
        color_continuous_scale='Viridis',  # Use the Viridis color scale
        range_color=[0, 200],  # Set the color scale range
    )

    # Display the customized heatmap
    st.plotly_chart(fig)

# Run the function with your joined_df
create_customized_heatmap(joined_df)