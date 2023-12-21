'''
A brief look at temple data
for project proposal

Marcel Pratikto
'''

#%%
# imports
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go

#%%
# read parquet files
temples = pl.read_parquet("../temple_details.parquet", use_pyarrow = True)

# %%
# OPERATING vs CONSTRUCTION
opvsco = temples\
    .group_by("status")\
    .agg(
        status_count = pl.col("status").count()
    )
total = opvsco.select(pl.sum("status_count"))

# %%
opvsco_fig = px.bar(
    opvsco,
    x = "status",
    y = "status_count",
    title = "LDS Temple Status"
)

# %%
# figure out unique differences
# operating vs construction elevation_ft (US only)
US_elevation = temples\
    .filter(
        pl.col("country") == "United States"
    )\
    .select("elevation_ft", "announcement_year")\
    .drop_nulls()\
    .group_by("announcement_year")\
    .agg(
        avg_elevation = pl.col("elevation_ft").mean()
    )\
    .sort("announcement_year")


#%%
US_elevation_fig = px.scatter(
    US_elevation,
    x = "announcement_year",
    y = "avg_elevation",
    title = "Temple Elevation Based on Announcement Year"
).update_layout(
    xaxis_title = "Year Announced",
    yaxis_title = "Average Elevation (ft)"
)

#%%
# operating vs construction acreage (US only)
US_acreage = temples\
    .filter(
        pl.col("country") == "United States"
    )\
    .select(
        "acreage", "announcement_year"
    )\
    .drop_nulls()\
    .group_by("announcement_year")\
    .agg(
        avg_acreage = pl.col("acreage").mean()
    )\
    .sort("announcement_year")

#%%
US_acreage_fig = px.scatter(
    US_acreage,
    x = "announcement_year",
    y = "avg_acreage",
    title = "Temple Acreage Based on Announcement Year"
).update_layout(
    xaxis_title = "Year Announced",
    yaxis_title = "Average Acreage"
)    
# %%
USTemples = temples\
    .filter(
        pl.col("country") == "United States"
    )

# %%
# Temple elevation based on state (US only)
USTemples_elevation = USTemples\
    .select("elevation_ft", "stateRegion")\
    .drop_nulls()\
    .group_by("stateRegion")\
    .agg(
        avg_elevation = pl.col("elevation_ft").mean()
    )\
    .sort("stateRegion")

# %%
USTemples_elevation_fig = px.scatter(
    USTemples_elevation,
    x = "stateRegion",
    y = "avg_elevation",
    title = "Temple Elevation Based on State"
).update_layout(
    xaxis_title = "State",
    yaxis_title = "Average Elevation (ft)"
)  

# %%
# temple acreage based on state (US only)
USTemples_acreage = USTemples\
    .select("acreage", "stateRegion")\
    .drop_nulls()\
    .group_by("stateRegion")\
    .agg(
        avg_acreage = pl.col("acreage").mean()
    )\
    .sort("stateRegion")

# %%
USTemples_acreage_fig = px.scatter(
    USTemples_acreage,
    x = "stateRegion",
    y = "avg_acreage",
    title = "Temple Acreage Based on State"
).update_layout(
    font = dict(size = 10),
    xaxis_title = "State",
    yaxis_title = "Average Acreage"
)
# %%
