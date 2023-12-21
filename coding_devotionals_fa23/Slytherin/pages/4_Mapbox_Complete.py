
import streamlit as st
import pandas as pd
import plotly.express as px


px.set_mapbox_access_token(st.secrets["MAPBOX_TOKEN"])

try:
    raw = pd.read_parquet("../data/utah_georgia_places.parquet")
    if raw.empty:
        st.error("Data file is empty or not properly loaded.")
    else:
        # Check if 'websites' and 'region' columns are in the dataframe
        if 'websites' in raw.columns and 'region' in raw.columns:
            utah_georgia = raw[raw["location_name"] == "The Church of Jesus Christ of Latter day Saints"] 
            
            if utah_georgia.empty:
                st.error("No matching entries for 'lds.org' in 'websites' column.")
            else:
                utah = utah_georgia[(utah_georgia["region"] == "UT")]
                georgia = utah_georgia[(utah_georgia["region"] == "GA")]

                selection = st.selectbox("Select State", options=["Utah", "Georgia", "Both"])

                if selection == "Utah":
                    dat = utah
                elif selection == "Georgia":
                    dat = georgia
                else: 
                    dat = utah_georgia

                fig = px.scatter_mapbox(
                    data_frame=dat, 
                    lat="latitude", 
                    lon="longitude",
                    zoom=4, 
                    color="region", 
                    color_discrete_map={"UT": "red", "GA": "blue"},
                    hover_data=["location_name"]
                )

                st.plotly_chart(fig)
        else:
            st.error("Required columns 'websites' or 'region' not found in data.")
except Exception as e:
    st.error(f"Error loading data: {e}")
