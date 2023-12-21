# %% 
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
# %%
patterns = pl.from_arrow(pq.read_table("../california_church_store/patterns.parquet"))
places = pl.from_arrow(pq.read_table("../california_church_store/places.parquet"))
spatial = pl.from_arrow(pq.read_table("../california_church_store/spatial.parquet"))
temples = pl.read_parquet("../temple_details.parquet")

# %%
# delicate method that depends on identical rows for each structure.
# also need to deal with 
hours = patterns\
    .select("bucketed_dwell_times")\
    .explode("bucketed_dwell_times")\
    .unnest("bucketed_dwell_times")

hours1 = patterns\
    .head(1)\
    .select("bucketed_dwell_times")\
    .explode("bucketed_dwell_times")\
    .unnest("bucketed_dwell_times")

keys = patterns.select("placekey").to_series().to_list()
dates = patterns.select("date_range_start").to_series().to_list()
unique_vals = hours1.shape[0]
keys_long = [ele for ele in keys for i in range(unique_vals)]
dates_long = [ele for ele in dates for i in range(unique_vals)]

hours\
    .with_columns([
        pl.Series(name = "placekey", values = keys_long),
        pl.Series(name = "date_range_start", values = dates_long)
        ])

# %%
# robust method that handles different struct sizes but it is slow
brands = patterns\
    .select("placekey", "date_range_start", "related_same_day_brand")

brands_explode = pl.DataFrame({"key": "empty", "value": 1, "placekey": "empty", "date_range_start": "empty"})
for i in range(brands.shape[0]):
    print(i)
    row_i = brands.row(i)
    df_i = pl.DataFrame(row_i[2])\
       .with_columns([
           pl.lit(row_i[0]).alias("placekey"),
           pl.lit(row_i[1]).alias("date_range_start"),
           ])
    if df_i.shape[1] == 3:
        brands_explode = pl.concat([brands_explode, df_i], rechunk = True)

# %%
# fast method that handles varied list sizes
new_names = dict([("field_" + str(i), str(i+1)) for i in range(20)])
brands = patterns\
    .select("placekey", "related_same_day_brand")\
    .with_columns(pl.col("related_same_day_brand").list.to_struct().alias("explode_col"))\
    .select("placekey","explode_col")\
    .unnest("explode_col")\
    .rename(new_names)\
    .melt(id_vars="placekey")\
    .unnest("value")\
    .with_columns(pl.col("variable").cast(pl.Int64).alias("rank"))\
    .rename({"value":"percent"})\
    .drop("variable")\
    .filter(pl.col("key").is_not_null())


# %%
# retry dwell time
explode_col = "bucketed_dwell_times"
number_levels = 7
new_names = dict([("field_" + str(i), str(i+1)) for i in range(number_levels)])
patterns\
    .select("placekey", "date_range_start", explode_col)\
    .with_columns(pl.col(explode_col).list.to_struct().alias("explode_col"))\
    .select("placekey", "date_range_start", "explode_col")\
    .unnest("explode_col")\
    .rename(new_names)\
    .melt(id_vars=["placekey", "date_range_start"])\
    .unnest("value")\
    .filter(pl.col("key").is_not_null())\
    .sort(["placekey", "date_range_start"])
# %%
explode_col = "visitor_home_cbgs"
patterns\
    .select("placekey", "date_range_start", explode_col)\
    .with_columns(pl.col(explode_col).list.to_struct().alias("explode_col"))\
    .select("placekey","date_range_start","explode_col")\
    .unnest("explode_col")\
    .melt(id_vars=["placekey", "date_range_start"])\
    .unnest("value")\
    .filter(pl.col("key").is_not_null())\
    .drop("variable")\
    .sort(["placekey", "date_range_start"])

# %%
explode_col = "device_type"
patterns\
    .select("placekey","date_range_start", explode_col)\
    .with_columns(pl.col(explode_col).list.to_struct().alias("explode_col"))\
    .select("placekey","date_range_start","explode_col")\
    .unnest("explode_col")\
    .melt(id_vars=["placekey", "date_range_start"])\
    .unnest("value")\
    .filter(pl.col("key").is_not_null())\
    .drop("variable")\
    .sort(["placekey", "date_range_start"])
# %%
