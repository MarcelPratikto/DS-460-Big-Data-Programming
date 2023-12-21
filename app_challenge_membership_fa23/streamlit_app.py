#=======================================================================================================
# IMPORTS
#=======================================================================================================
from collections import namedtuple
import altair as alt
import math
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from urllib.request import urlopen
import json
with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

#=======================================================================================================
# DATA
#=======================================================================================================
county = pl.read_parquet('data/active_members_county.parquet')
tract = pl.read_parquet("data/active_members_tract.parquet")
chapel_scrape = pl.read_parquet("data/full_church_building_data-20.parquet")
chapel_safegraph = pl.read_parquet("data/safegraph_chapel.parquet")
temples = pl.from_arrow(pq.read_table("data/temple_details_spatial.parquet"))
tract_nearest = pl.from_arrow(pq.read_table("data/tract_distance_to_nearest_temple.parquet"))

state_fips = {
   "Alabama"                :  "01",
   "Alaska"                 :  "02",
   "Arizona"                :  "04",
   "Arkansas"               :  "05",
   "California"             :  "06",
   "Colorado"               :  "08",
   "Connecticut"            :  "09",
   "Delaware"               :  "10",
   "District of Columbia"   :  "11",
   "Florida"                :  "12",
   "Geogia"                 :  "13",
   "Hawaii"                 :  "15",
   "Idaho"                  :  "16",
   "Illinois"               :  "17",
   "Indiana"                :  "18",
   "Iowa"                   :  "19",
   "Kansas"                 :  "20",
   "Kentucky"               :  "21",
   "Louisiana"              :  "22",
   "Maine"                  :  "23",
   "Maryland"               :  "24",
   "Massachusetts"          :  "25",
   "Michigan"               :  "26",
   "Minnesota"              :  "27",
   "Mississippi"            :  "28",
   "Missouri"               :  "29",
   "Montana"                :  "30",
   "Nebraska"               :  "31",
   "Nevada"                 :  "32",
   "New Hampshire"          :  "33",
   "New Jersey"             :  "34",
   "New Mexico"             :  "35",
   "New York"               :  "36",
   "North Carolina"         :  "37",
   "North Dakota"           :  "38",
   "Ohio"                   :  "39",
   "Oklahoma"               :  "40",
   "Oregon"                 :  "41",
   "Pennsylvania"           :  "42",
   "Rhode Island"           :  "44",
   "South Carolina"         :  "45",
   "South Dakota"           :  "46",
   "Tennessee"              :  "47",
   "Texas"                  :  "48",
   "Utah"                   :  "49",
   "Vermont"                :  "50",
   "Virginia"               :  "51",
   "Washington"             :  "53",
   "West Virginia"          :  "54",
   "Wisconsin"              :  "55",
   "Wyoming"                :  "56"
}

#=======================================================================================================
# FEATURE CHALLENGE
#=======================================================================================================
st.title("Marcel Pratikto")
st.title("Feature Challenge")

features_parquet = pl.read_parquet("feature_scripts/marcel_pratikto_features.parquet")

features = features_parquet\
    .with_columns(
        (pl.col("tract").str.slice(0,5)).alias("fips"),
        (pl.col("tract").str.slice(0,2)).alias("state_fips")
    )

features_state_select = st.selectbox(
    key = "feature_state_select",
    label = "Select State",
    options = [state for state in state_fips],
    index = 12
)

features_state = features\
    .filter(
        pl.col("state_fips") == state_fips[features_state_select]
    )

st.dataframe(
    data = features_state,
    hide_index = True,
    height = 200
)

features_pd = features_state.to_pandas()

features_fig = px.choropleth(
    data_frame=features_pd,
    geojson=counties,
    locations="fips",
    color = "estimated_avg_house_price",
    color_continuous_scale="Viridis",
    scope = 'usa',
    fitbounds="locations",
    labels = {"estimated_avg_house_price" : "Average House Price"}
)

st.plotly_chart(features_fig)

#=======================================================================================================
# APP CHALLENGE
#=======================================================================================================
st.title("App Challenge")

#=======================================================================================================
# QUESTION 1
#=======================================================================================================
st.markdown(
    "## 1. How does the number of chapels in Safegraph " +
    "compare to the number of chapels from the church website web scrape?")

chapel_safegraph_len = len(chapel_safegraph)
chapel_scrape_len = len(chapel_scrape)

safegraph_vs_scrape = pl.DataFrame(
    data = {"name" : ["safegraph", "scrape"], "count": [chapel_safegraph_len, chapel_scrape_len]},
    schema = {"name": pl.Utf8,"count": pl.Int64}
)

svs_fig = px.bar(
    safegraph_vs_scrape,
    x = "name",
    y = "count",
    color="name",
    title="Number of Chapels",
    text="count"
)
st.plotly_chart(svs_fig)

st.text(
    f"There are significantly more chapels from the church website web scrape.\n" +
    f"{chapel_scrape_len} from the web scrape vs. {chapel_safegraph_len} from safegraph.\n" +
    f"This tells us that safegraph only has {round(chapel_safegraph_len/chapel_scrape_len*100,2)}% of the data we need.\n" +
    f"It's missing {round(100 - chapel_safegraph_len/chapel_scrape_len*100,2)}% of the chapels based on the church's website."
)

#=======================================================================================================
# QUESTION 2
#=======================================================================================================
st.markdown(
    "## 2. Does the active member estimate look reasonable " +
    "as compared to the tract population?")

tract_county_state_fips = tract\
    .with_columns(
        (pl.col("home").str.slice(0,2)).alias("state_fips"),
        (pl.col("home").str.slice(2,3)).alias("county_fips"),
        (pl.col("home").str.slice(5,6)).alias("tract_fips")
    )

chartCol1, chartCol2 = st.columns(2)
with chartCol1:
    q2_state_select = st.selectbox(
        key = "q2_state_select",
        label = "Select State",
        options = [state for state in state_fips],
        index = 12
    )
with chartCol2:
    county_by_state = tract_county_state_fips\
        .filter(pl.col("state_fips") == state_fips[q2_state_select])\
        .select("county_fips").unique()\
        .sort("county_fips", descending = False)
    q2_county_select = st.selectbox(
        key = "q2_county_select",
        label = "Select County",
        options = [county for county in county_by_state["county_fips"]]
    )

tract_by_county_state = tract_county_state_fips\
    .filter(
        (pl.col("state_fips") == state_fips[q2_state_select])
        & (pl.col("county_fips") == q2_county_select)
    )\
    .select(
        ["state_fips", "county_fips", "tract_fips", "population", "active_members_estimate", "proportion"]
    )

st.dataframe(
    data = tract_by_county_state,
    hide_index = True,
    height = 200
)

st.markdown("##### Tract Distribution Charts of Selected Counties by State")

q2_tract_dist_hist = px.histogram(
    data_frame = tract_by_county_state,
    x = "tract_fips",
    y = ["population", "active_members_estimate"]
).update_xaxes(
    type = "category"
)

st.plotly_chart(q2_tract_dist_hist)

active_avg = tract\
    .with_columns(
        ((pl.col("population")))
    )\
    .group_by("population")\
    .agg(
        (pl.col("active_members_estimate")).mean()
    )\
    .sort("population", descending=False)

st.markdown("##### Population vs Number of Active Members (US)")

active_vs_tract_fig = px.scatter(
    data_frame = active_avg,
    x = "population",
    y = "active_members_estimate"
)

st.plotly_chart(active_vs_tract_fig)

proportion_avg = tract\
    .with_columns(
        ((pl.col("population")/10000).round(0) * 10).alias("population_k")
    )\
    .group_by("population_k")\
    .agg(
        (pl.col("proportion")).mean()
    )\
    .sort("population_k", descending=False)

st.markdown("##### Population vs. Proportion of Active members (US Average)")

tract_avg_fig = px.line(
    data_frame = proportion_avg,
    x = "population_k",
    y = "proportion"
).update_yaxes(
    range = [0, 0.03]
)

st.plotly_chart(tract_avg_fig)

"""
Personally, I think that the estimated number active LDS members is a bit low, but that is because I live in one of the highest LDS populated areas in the US.
When I googled the percentage of the US population that is LDS, the result is around 1.2%. The second chart above is very close to that percentage,
where the average proportion does not reach 2%. Therefore, the active member estimate looks reasonable compared to the tract population.
"""

#=======================================================================================================
# QUESTION 3
#=======================================================================================================
st.markdown(
    "## 3. Does the active member estimate look reasonable " +
    "as compared to the religious census estimates by county?")

q3_selection = st.selectbox(
    key = "q3",
    label = "Select State",
    options = [state for state in state_fips],
    index = 12
)

fips_state = county\
    .filter(
        pl.col("STATEFP") == state_fips[q3_selection]
    )\
    .with_columns(
        (pl.col("STATEFP") + pl.col("COUNTYFP")).alias("fips")
    )

fips_state_pd = fips_state.to_pandas()

q3_active_fig = px.choropleth(
    data_frame=fips_state_pd,
    geojson=counties,
    locations="fips",
    color = "active_members_estimate",
    color_continuous_scale="Viridis",
    scope = 'usa',
    fitbounds="locations",
    labels = {"active_members_estimate" : "# of LDS members"}
).update_layout(
    width = 800,
    height = 400
)

q3_religious_fig = px.choropleth(
    data_frame=fips_state_pd,
    geojson=counties,
    locations="fips",
    color = "ratio_census",
    color_continuous_scale="Viridis",
    scope = 'usa',
    fitbounds="locations",
    labels = {"ratio_census" : "ratio of religious people"}
).update_layout(
    width = 800,
    height = 400
)

st.markdown("##### Active LDS Members")
left, middle, right = st.columns((0.1, 5, 2))
with middle:
    st.plotly_chart(q3_active_fig)

st.markdown("##### Census' Ratio of People that are Religious")
left, middle, right = st.columns((0.3, 5, 2))
with middle:
    st.plotly_chart(q3_religious_fig)

"""
I chose to display the state of Idaho as the default because I live in Idaho, 
and that is the state that I am most familiar with when it comes to knowing which areas are LDS-concentrated.
You can, however, change the state and see the county data for that state.

If you look at both of the charts above, you will see that certain areas have a higher percentage of people that are religious.
Coincidentally, those areas are also areas where there are large concentrations of LDS members.
For Idaho, those areas are: Rexburg / Idaho Falls, Pocatello, and Boise.
Therefore, it is safe to assume that the estimated active member looks reasonable in comparison to census estimate
of religious people by county.
"""

#=======================================================================================================
# QUESTION 4
#=======================================================================================================
st.markdown(
    "## 4. How does the current temple placement look by state " +
    "as compared to the county active membership estimates?")

# draw a map with temples on it,
# then overlay on top of choropleth map

q4_selection = st.selectbox(
    key = "q4",
    label = "Select State",
    options = [state for state in state_fips],
    index = 12
)

fips_state = county\
    .filter(
        pl.col("STATEFP") == state_fips[q4_selection]
    )\
    .with_columns(
        (pl.col("STATEFP") + pl.col("COUNTYFP")).alias("fips")
    )

fips_state_pd = fips_state.to_pandas()

temples_US = temples\
    .filter(
        # (pl.col("country") == "United States") &
        (pl.col("stateRegion") == q4_selection)
    )\
    .with_columns(
        (pl.col("STATEFP") + pl.col("COUNTYFP")).alias("fips")
    )

temples_pd = temples_US.to_pandas()

q4_toggle = st.toggle(
    key = "q4_toggle",
    label = "Show Temple?",
    value = True,
    help = "Press the toggle button to show / hide the temples."
)

if q4_toggle:
    temples_fig = px.choropleth(
        data_frame = fips_state_pd,
        geojson = counties,
        locations ="fips",
        color = "active_members_estimate",
        color_continuous_scale="Viridis",
        scope = 'usa',
        fitbounds="locations",
        labels = {"active_members_estimate" : "# of LDS members"}
    ).add_trace(
        go.Scattergeo(
            lat = temples_pd["lat"],
            lon = temples_pd["long"],
            text = temples_pd["temple"],
            mode = "markers",
            marker = dict(
                size = 8,
                symbol = "square"
            ),
            marker_color = "red"
        )
    )
else:
    temples_fig = px.choropleth(
        data_frame = fips_state_pd,
        geojson = counties,
        locations ="fips",
        color = "active_members_estimate",
        color_continuous_scale="Viridis",
        scope = 'usa',
        fitbounds="locations",
        labels = {"active_members_estimate" : "# of LDS members"}
    )

st.markdown("### Temple Placement vs. Active LDS members")
st.plotly_chart(temples_fig)

"""
Temples are usually placed in areas where there are higher concentrations of LDS members.

Press the toggle button to show / hide the temples.
"""

# TODO:
# Tract distribution charts (boxplots and histograms) based on selected counties within a state.
# A scaling input that allows the user to input a value between 0 and 1 that will adjust the active membership estimates proportionally.

#=======================================================================================================
# ORIGINAL CODE FROM PROFESSOR
# DELETE WHEN DONE
# Keep in case we need example code
#=======================================================================================================
# st.markdown("# This is changing!")

# with st.echo(code_location='below'):
#     total_points = st.slider("Number of points in spiral", 1, 5000, 2000)
#     num_turns = st.slider("Number of turns in spiral", 1, 100, 9)

#     Point = namedtuple('Point', 'x y')
#     data = []

#     points_per_turn = total_points / num_turns

#     for curr_point_num in range(total_points):
#         curr_turn, i = divmod(curr_point_num, points_per_turn)
#         angle = (curr_turn + 1) * 2 * math.pi * i / points_per_turn
#         radius = curr_point_num / total_points
#         x = radius * math.cos(angle)
#         y = radius * math.sin(angle)
#         data.append(Point(x, y))

#     st.altair_chart(alt.Chart(pd.DataFrame(data), height=500, width=500)
#         .mark_circle(color='#0068c9', opacity=0.5)
#         .encode(x='x:Q', y='y:Q'))