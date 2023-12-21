'''
Convert our crime dataset to parquet
'''

#%%
# imports
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import math
import os

#%%
# store crime data in a dataframe
crime_data = pl.read_csv("crime_data_w_population_and_crime_rate.csv")

#%%
# read the county_meta dataset
direc = "county_meta/"
files = os.listdir(direc)
files = [f for f in files if os.path.isfile(direc+f)]

county_meta = None
for f in files:
    if county_meta is None:
        county_meta = pl.read_parquet(direc+f, use_pyarrow = True)
        print("if")
    else:
        temp = pl.read_parquet(direc+f, use_pyarrow = True)
        county_meta = county_meta.vstack(temp)
        print("else")

#%%
# make sure that crime and county_data
# has the same number of rows
county_meta1 = county_meta.with_row_count()

county_name = county_meta1\
    .select("row_nr", "name", "fips")\
    .sort("name", descending=False)

#%%
crime_name = crime_data\
    .with_columns([
        pl.col("county_name")
        .str.split_exact(",", 1)
        .struct.rename_fields(["name", "name_st"])
        .alias("fields")
    ])\
    .unnest("fields")\
    .drop(["name_st", "county_name"])\
    .sort("name", descending=False)
#%%
missing = county_meta1\
    .select("row_nr", "name", "fips")\
    .filter(
        ~(
            pl.col("name")
            .is_in(crime_name.get_column("name"))
        )
    )
print(missing.to_pandas().to_markdown())

#%%
# find all the counties row_nr
denali = county_meta1\
    .filter(
        pl.col("name") == "Denali Borough"
    )

doa = county_meta1\
    .filter(
        pl.col("name") == "Do̱a Ana County"
    )

hoonah = county_meta1\
    .filter(
        pl.col("name") == "Hoonah-Angoon Census Area"
    )

petersburg = county_meta1\
    .filter(
        pl.col("name") == "Petersburg Borough"
    )

prince = county_meta1\
    .filter(
        pl.col("name") == "Prince of Wales-Hyder Census Area"
    )

skagway = county_meta1\
    .filter(
        pl.col("name") == "Skagway Municipality"
    )

wrangell = county_meta1\
    .filter(
        pl.col("name") == "Wrangell City and Borough"
    )

#%%
nulls = county_meta1\
    .filter(
        pl.col("name").is_null()
    )
print(nulls.to_pandas().to_markdown())

#%%
#combine crime_data fips_st and fips_cty to get fips
crime_data_fips = crime_data\
    .with_columns(
        pl.concat_str(
            [
                pl.col("FIPS_ST"),
                pl.col("FIPS_CTY")
            ]
        )
        .alias("fips")
        .cast(pl.Int64)
    )\
    .with_columns([
        pl.col("county_name")
        .str.split_exact(",", 1)
        .struct.rename_fields(["name", "name_st"])
        .alias("fields")
    ])\
    .unnest("fields")\
    .drop(["name_st", "county_name"])\
    .sort("name", descending=False)

#%%
# crime with added missing rows
empty_data = {"name":["x"], "fips":[-1]}
empty = pl.DataFrame(empty_data, schema={"name":str, "fips":pl.Int64})
missing_drop = missing.select("name","fips")
nulls_drop = nulls.select("name","fips")
# crime_data_fips_missing = crime_data_fips\
#     .join(missing_drop, on=["fips", "name"], how="left")\
#     .join(nulls_drop, on=["fips", "name"], how="left")
crime_data_fips_missing = pl.concat(
        [crime_data_fips, missing_drop, nulls_drop, empty],
        how="diagonal"
    )


#%%
crime = crime_data_fips_missing\
    .select(
        [
            'fips',
            'crime_rate_per_100000',
            'EDITION',
            'PART',
            'IDNO',
            'CPOPARST',
            'CPOPCRIM',
            'AG_ARRST',
            'AG_OFF',
            'COVIND',
            'INDEX',
            'MODINDX',
            'MURDER',
            'RAPE',
            'ROBBERY',
            'AGASSLT',
            'BURGLRY',
            'LARCENY',
            'MVTHEFT',
            'ARSON',
            'population'
        ]
    ).sort(by="fips")

# %%
# separate the data into chunks
# with each parquet files
# no larger than 20MB
path = "crime/"
num_rows = crime.height
split = 5
length = math.floor(num_rows / split)

last_offset = 0
for i in range(split):
    temp = crime.slice(last_offset, length)
    # temp_row = temp.row(-1)
    # last_offset = temp_row[1]
    last_offset += length
    temp.write_parquet(path+"crime"+str(i)+".parquet")
# %%
