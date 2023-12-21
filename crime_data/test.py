'''
py file to do some data wrangling

figure out how many rows in county_meta data
'''

#%%
# imports
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os

# %%
# read all the parquet files
# and store data in county_meta

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
# %%
county_meta\
    .select(
        pl.col("fips").count()
    )
# %%
