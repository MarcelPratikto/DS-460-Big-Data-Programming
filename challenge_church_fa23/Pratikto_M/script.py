# %%
# My jupyter notebook
# assignment: Church Buildings in Utah vs Georgia

# export to html / py before submitting to canvas

# %%
# imports
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go

# %%
# read parquet files
# patterns = pl.read_parquet("../data/parquet/patterns.parquet", use_pyarrow = True)
# places = pl.read_parquet("../data/parquet/places.parquet")
# using this because the regular way had issues where I was getting null data
patterns = pl.from_arrow(pq.read_table("../data/parquet/patterns.parquet"))
places = pl.from_arrow(pq.read_table("../data/parquet/places.parquet"))

#%%
# -----------------------------------------------------------------------------------------
# Question 1
# What differences are there between iPhone and Android users 
# when comparing visits to The Church of Jesus Christ buildings 
# of Latter-day Saints in Utah and Georgia?
# -----------------------------------------------------------------------------------------
device = patterns\
    .select("placekey", "date_range_start", "device_type")\
    .explode("device_type")\
    .unnest("device_type")\
    .pivot(
        index=["placekey","date_range_start"],
        values="value",
        columns="key",
        aggregate_function="first"
        )\
    .join(places,               # combine patterns with places based on placekey
          on=["placekey"],
          how="inner"
        )\
    .filter(                    # only look at data for LDS church buildings
        pl.col("location_name").str.contains("[L|l]atter|lds|LDS")
    )\
    .select(                    # get only columns that are relevant
        pl.col(["placekey","date_range_start","android","ios","region","location_name"])
    )
# count all the different variations of the location_name that we specified above
device.group_by("location_name").count().sort("count")

#%%
# compare the total number of android and ios users
# in GA vs UT
fig1_dat = device\
    .group_by("region").agg(
        pl.col("android").sum().alias("android_total"),
        pl.col("ios").sum().alias("ios_total")
    )
fig1_dat

#%%
# visualization using plotly
fig1 = px.bar(
    data_frame = fig1_dat, 
    x="region", y=["android_total", "ios_total"], 
    barmode="group", title="iPhone vs Android Users in LDS churches in UT and GA"
    )
fig1.show()

#%%
# convert polars to pandas
# so we can get markdown table
markdown1 = fig1_dat.head().to_pandas().to_markdown()

#%%
# -----------------------------------------------------------------------------------------
# Question 2
# Compare hourly usage patterns between 
# The Church of Jesus Christ of Latter-day Saints 
# and the other churches in each state.
# -----------------------------------------------------------------------------------------
cols_we_want = ["placekey", "raw_visitor_counts", "popularity_by_hour", "location_name", "region"]
lds_hours = patterns\
    .select("placekey", "raw_visitor_counts", "popularity_by_hour")\
    .join(places,               # combine patterns with places based on placekey
          on=["placekey"],
          how="inner"
        )\
    .filter(
        pl.col("location_name").str.contains("[L|l]atter|lds|LDS")
    )\
    .select(pl.col(cols_we_want))\
    .with_columns(
        LDS_others = pl.lit("LDS")
    )

other_hours = patterns\
    .select("placekey", "raw_visitor_counts", "popularity_by_hour")\
    .join(places,               # combine patterns with places based on placekey
          on=["placekey"],
          how="inner"
        )\
    .filter(
        ~pl.col("location_name").str.contains("[L|l]atter|lds|LDS")
    )\
    .select(pl.col(cols_we_want))\
    .with_columns(
        LDS_others = pl.lit("others")
    )
    
hours = pl.concat([lds_hours,other_hours])

#%%
# 
hourly_avg = hours\
    .lazy()\
    .select([
        pl.all().exclude(["popularity_by_hour"]),
        pl.col("popularity_by_hour").list.get(0).alias("12am"),
        pl.col("popularity_by_hour").list.get(1).alias("1am"),
        pl.col("popularity_by_hour").list.get(2).alias("2am"),
        pl.col("popularity_by_hour").list.get(3).alias("3am"),
        pl.col("popularity_by_hour").list.get(4).alias("4am"),
        pl.col("popularity_by_hour").list.get(5).alias("5am"),
        pl.col("popularity_by_hour").list.get(6).alias("6am"),
        pl.col("popularity_by_hour").list.get(7).alias("7am"),
        pl.col("popularity_by_hour").list.get(8).alias("8am"),
        pl.col("popularity_by_hour").list.get(9).alias("9am"),
        pl.col("popularity_by_hour").list.get(10).alias("10am"),
        pl.col("popularity_by_hour").list.get(11).alias("11am"),
        pl.col("popularity_by_hour").list.get(12).alias("12pm"),
        pl.col("popularity_by_hour").list.get(13).alias("1pm"),
        pl.col("popularity_by_hour").list.get(14).alias("2pm"),
        pl.col("popularity_by_hour").list.get(15).alias("3pm"),
        pl.col("popularity_by_hour").list.get(16).alias("4pm"),
        pl.col("popularity_by_hour").list.get(17).alias("5pm"),
        pl.col("popularity_by_hour").list.get(18).alias("6pm"),
        pl.col("popularity_by_hour").list.get(19).alias("7pm"),
        pl.col("popularity_by_hour").list.get(20).alias("8pm"),
        pl.col("popularity_by_hour").list.get(21).alias("9pm"),
        pl.col("popularity_by_hour").list.get(22).alias("10pm"),
        pl.col("popularity_by_hour").list.get(23).alias("11pm")
    ])\
    .collect()\
    .group_by(["region", "LDS_others"])\
    .agg(
        pl.mean("12am").alias("12am_mean"),
        pl.mean("1am").alias("1am_mean"),
        pl.mean("2am").alias("2am_mean"),
        pl.mean("3am").alias("3am_mean"),
        pl.mean("4am").alias("4am_mean"),
        pl.mean("5am").alias("5am_mean"),
        pl.mean("6am").alias("6am_mean"),
        pl.mean("7am").alias("7am_mean"),
        pl.mean("8am").alias("8am_mean"),
        pl.mean("9am").alias("9am_mean"),
        pl.mean("10am").alias("10am_mean"),
        pl.mean("11am").alias("11am_mean"),
        pl.mean("12pm").alias("12pm_mean"),
        pl.mean("1pm").alias("1pm_mean"),
        pl.mean("2pm").alias("2pm_mean"),
        pl.mean("3pm").alias("3pm_mean"),
        pl.mean("4pm").alias("4pm_mean"),
        pl.mean("5pm").alias("5pm_mean"),
        pl.mean("6pm").alias("6pm_mean"),
        pl.mean("7pm").alias("7pm_mean"),
        pl.mean("8pm").alias("8pm_mean"),
        pl.mean("9pm").alias("9pm_mean"),
        pl.mean("10pm").alias("10pm_mean"),
        pl.mean("11pm").alias("11pm_mean")
    )

#%%
hourly_avg_UT = hourly_avg\
    .filter(
        pl.col("region") == "UT"
    )

#%%
fig_hourly_avg_UT_LDS = px.line(
    x = [i for i in range(24)],
    y = [hourly_avg_UT.item(0,y) for y in range(2,26)]
).update_traces(
    line=dict(color='red'),
    name="UT-LDS",
    showlegend=True
)
fig_hourly_avg_UT_LDS.show()

#%%
fig_hourly_avg_UT_others = px.line(
    x = [i for i in range(24)],
    y = [hourly_avg_UT.item(1,y) for y in range(2,26)]
).update_traces(
    line=dict(color='pink'),
    name="UT-others",
    showlegend=True
)
fig_hourly_avg_UT_others.show()

#%%
hourly_avg_GA = hourly_avg\
    .filter(
        pl.col("region") == "GA"
    )

#%%
fig_hourly_avg_GA_LDS = px.line(
    x = [i for i in range(24)],
    y = [hourly_avg_GA.item(0,y) for y in range(2,26)]
).update_traces(
    line=dict(color='blue'),
    name="GA-LDS",
    showlegend=True
)
fig_hourly_avg_GA_LDS.show()

#%%
fig_hourly_avg_GA_others = px.line(
    x = [i for i in range(24)],
    y = [hourly_avg_GA.item(1,y) for y in range(2,26)]
).update_traces(
    line=dict(color='lightblue'),
    name="GA-others",
    showlegend=True
)
fig_hourly_avg_GA_others.show()

#%%
x_axis_tick_names = [
"12am", "1am", "2am", "3am", "4am", "5am",
"6am", "7am", "8am", "9am", "10am", "11am",
"12pm", "1pm", "2pm", "3pm", "4pm", "5pm",
"6pm", "7pm", "8pm", "9pm", "10pm", "11pm"
]
fig2 = go.Figure(
    data = fig_hourly_avg_GA_LDS.data + fig_hourly_avg_GA_others.data\
    + fig_hourly_avg_UT_LDS.data + fig_hourly_avg_UT_others.data
).update_layout(
    title = "Average number of church visitors per hour UT vs GA",
    xaxis = dict(
        tickmode = "array",
        tickvals = [i for i in range(0,24)],
        ticktext = x_axis_tick_names
    )
).update_xaxes(
    title_text = "hours",
    tickangle=-45
).update_yaxes(
    title_text="average number of visitors"
)

fig2.show()

#%%
# convert polars to pandas
# so we can get markdown table
markdown2 = hourly_avg.head().to_pandas().to_markdown()

#%%
# -----------------------------------------------------------------------------------------
# Question 3
# Contrast the related_same_day_brand brands between 
# those who visit the Church of Jesus Christ of Latter-day Saints 
# and those who visit other churches.
# -----------------------------------------------------------------------------------------
scol = "related_same_day_brand"

LDS_places = places\
    .filter(
        pl.col("location_name").str.contains("[L|l]atter|lds|LDS")
    )\
    .with_columns(
        LDS_other = pl.lit("LDS")
    )

other_places = places\
    .filter(
        ~pl.col("location_name").str.contains("[L|l]atter|lds|LDS")
    )\
    .with_columns(
        LDS_other = pl.lit("other")
    )

LDS_other_places = pl.concat([LDS_places, other_places])

brand = patterns\
    .unique(subset=["placekey", "date_range_start"])\
    .select("placekey", "date_range_start", "raw_visit_counts", scol)\
    .explode(scol)\
    .unnest(scol)\
    .rename(
        {"key":"brand", "value":"brand_percent"}
    )\
    .with_columns(
        brand_visit_counts = pl.col("brand_percent")/100*pl.col("raw_visit_counts")
    )\
    .join(
        LDS_other_places,
        on="placekey",
        how="inner"
    )\
    .select(
        "placekey", "date_range_start",
        "brand", "brand_visit_counts",
        "LDS_other"
    )\
    .unique(subset=["placekey", "date_range_start", "brand"])\
    .sort("brand_visit_counts", descending=True)\
    .drop_nulls()

#%%
oct_2019_other = brand\
    .filter(
        (pl.col("date_range_start").str.contains("2019-10")) &
        (pl.col("LDS_other") == "other")
    )\
    .group_by(
        "brand"
    )\
    .agg(
        pl.col("brand_visit_counts").sum()
    )\
    .sort("brand_visit_counts", descending=True)

oct_2019_LDS = brand\
    .filter(
        (pl.col("date_range_start").str.contains("2019-10")) &
        (pl.col("LDS_other") == "LDS")
    )\
    .group_by(
        "brand"
    )\
    .agg(
        pl.col("brand_visit_counts").sum()
    )\
    .sort("brand_visit_counts", descending=True)

oct_2019_join = oct_2019_LDS\
    .join(
        oct_2019_other,
        on="brand"
    )\
    .rename(
        {
            "brand_visit_counts":"LDS_brands",
            "brand_visit_counts_right":"other_brands",
        }
    )\
    .sort(
        "LDS_brands", descending=True
    )

#%%
nov_2019_other = brand\
    .filter(
        (pl.col("date_range_start").str.contains("2019-11")) &
        (pl.col("LDS_other") == "other")
    )\
    .group_by(
        "brand"
    )\
    .agg(
        pl.col("brand_visit_counts").sum()
    )\
    .sort("brand_visit_counts", descending=True)

nov_2019_LDS = brand\
    .filter(
        (pl.col("date_range_start").str.contains("2019-11")) &
        (pl.col("LDS_other") == "LDS")
    )\
    .group_by(
        "brand"
    )\
    .agg(
        pl.col("brand_visit_counts").sum()
    )\
    .sort("brand_visit_counts", descending=True)

nov_2019_join = nov_2019_LDS\
    .join(
        nov_2019_other,
        on="brand"
    )\
    .rename(
        {
            "brand_visit_counts":"LDS_brands",
            "brand_visit_counts_right":"other_brands",
        }
    )\
    .sort(
        "LDS_brands", descending=True
    )

# %%
oct_nov_join = nov_2019_join\
    .join(
        oct_2019_join,
        on="brand"
    )\
    .rename(
        {
            "LDS_brands":"LDS_brands_oct",
            "other_brands":"other_brands_oct",
            "LDS_brands_right":"LDS_brands_nov",
            "other_brands_right":"other_brands_nov"
        }
    )\
    .sort(
        "LDS_brands_oct", descending=True
    )

#%%
dec_2019_other = brand\
    .filter(
        (pl.col("date_range_start").str.contains("2019-12")) &
        (pl.col("LDS_other") == "other")
    )\
    .group_by(
        "brand"
    )\
    .agg(
        pl.col("brand_visit_counts").sum()
    )\
    .sort("brand_visit_counts", descending=True)

dec_2019_LDS = brand\
    .filter(
        (pl.col("date_range_start").str.contains("2019-12")) &
        (pl.col("LDS_other") == "LDS")
    )\
    .group_by(
        "brand"
    )\
    .agg(
        pl.col("brand_visit_counts").sum()
    )\
    .sort("brand_visit_counts", descending=True)

dec_2019_join = dec_2019_LDS\
    .join(
        dec_2019_other,
        on="brand"
    )\
    .rename(
        {
            "brand_visit_counts":"LDS_brands",
            "brand_visit_counts_right":"other_brands",
        }
    )\
    .sort(
        "LDS_brands", descending=True
    )

# %%
oct_nov_dec_join = dec_2019_join\
    .join(
        oct_nov_join,
        on="brand"
    )\
    .rename(
        {
            "LDS_brands":"LDS_brands_dec",
            "other_brands":"other_brands_dec"
        }
    )\
    .sort(
        "LDS_brands_oct", descending=True
    )

#%%
newnames = {
    "LDS_brands_oct"    : "LDS (October)",
    "other_brands_oct"  : "Non-LDS (October)",
    "LDS_brands_nov"    : "LDS (November)",
    "other_brands_nov"  : "Non-LDS (November)",
    "LDS_brands_dec"    : "LDS (December)",
    "other_brands_dec"  : "Non-LDS (December)",
}

oct_nov_dec_fig = px.bar(
    oct_nov_dec_join.head(10),
    x = "brand",
    y = [
            "LDS_brands_oct", "other_brands_oct",
            "LDS_brands_nov", "other_brands_nov",
            "LDS_brands_dec", "other_brands_dec"
        ],
    barmode="group",
    color_discrete_map={
        "LDS_brands_oct"    : "red", 
        "other_brands_oct"  : "pink",
        "LDS_brands_nov"    : "blue", 
        "other_brands_nov"  : "lightblue",
        "LDS_brands_dec"    : "green", 
        "other_brands_dec"  : "lightgreen"
    }
).update_layout(
    title = "2019 Top Same Day Brands (LDS vs Non-LDS)",
    legend = dict(
        title=""
    )
).update_xaxes(
    title_text = "Brand names"
).update_yaxes(
    title_text="Number of visitors"
)\
.for_each_trace(lambda t: t.update(name = newnames[t.name]))

oct_nov_dec_fig.show()

#%%
# for the report
print(brand.head().to_pandas().to_markdown())
# %%
