# Databricks notebook source
# %run ../build_database
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md # Preprocessing

# COMMAND ----------

safegraph = spark.sql("""
    SELECT places.location_name,
        patterns.placekey,
        patterns.date_range_start,
        patterns.popularity_by_day,
        patterns.visits_by_day,
        patterns.visitor_home_aggregation,
        patterns.raw_visit_counts,
        patterns.normalized_visits_by_state_scaling,
        places.top_category
    FROM safegraph.patterns AS patterns
    LEFT JOIN safegraph.places AS places
        ON patterns.placekey = places.placekey
""")

# COMMAND ----------

churches = safegraph.select(
        "*",
        *[F.col("popularity_by_day")[key].alias(key) for key in safegraph.select("popularity_by_day").first()[0].keys()]
    ).filter(
        (F.col("Sunday") > F.col("Monday")) &
        (F.col("Sunday") > F.col("Tuesday")) &
        (F.col("Sunday") > F.col("Wednesday")) &
        (F.col("Sunday") > F.col("Thursday")) &
        (F.col("Sunday") > F.col("Friday")) &
        (F.col("Sunday") > F.col("Saturday"))
    ).filter(
        (F.col("top_category") == "Religious Organizations") &
        (F.col("location_name").rlike("Latter|latter|Saints|saints|LDS|\b[Ww]ard\b")) &
        (F.col("location_name").rlike("^((?!Reorganized).)*$")) &
        (F.col("location_name").rlike("^((?!All Saints).)*$")) &
        (F.col("location_name").rlike("^((?![cC]ath).)*$")) &
        (F.col("location_name").rlike("^((?![Bb]ody).)*$")) &
        (F.col("location_name").rlike("^((?![Pp]eter).)*$")) &
        (F.col("location_name").rlike("^((?![Cc]atholic).)*$")) &
        (F.col("location_name").rlike("^((?![Pp]res).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]inist).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]ission).)*$")) &
        (F.col("location_name").rlike("^((?![Ww]orship).)*$")) &
        (F.col("location_name").rlike("^((?![Rr]ain).)*$")) &
        (F.col("location_name").rlike("^((?![Bb]aptist).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]eth).)*$")) &
        (F.col("location_name").rlike("^((?![Ee]vang).)*$")) &
        (F.col("location_name").rlike("^((?![Ll]utheran).)*$")) &
        (F.col("location_name").rlike("^((?![Oo]rthodox).)*$")) &
        (F.col("location_name").rlike("^((?![Ee]piscopal).)*$")) &
        (F.col("location_name").rlike("^((?![Tt]abernacle).)*$")) &
        (F.col("location_name").rlike("^((?![Hh]arvest).)*$")) &
        (F.col("location_name").rlike("^((?![Aa]ssem).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]edia).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]artha).)*$")) &
        (F.col("location_name").rlike("^((?![Cc]hristian).)*$")) &
        (F.col("location_name").rlike("^((?![Uu]nited).)*$")) &
        (F.col("location_name").rlike("^((?![Ff]ellowship).)*$")) &
        (F.col("location_name").rlike("^((?![Ww]esl).)*$")) &
        (F.col("location_name").rlike("^((?![C]cosmas).)*$")) &
        (F.col("location_name").rlike("^((?![Gg]reater).)*$")) &
        (F.col("location_name").rlike("^((?![Pp]rison).)*$")) &
        (F.col("location_name").rlike("^((?![Cc]ommuni).)*$")) &
        (F.col("location_name").rlike("^((?![Cc]lement).)*$")) &
        (F.col("location_name").rlike("^((?![Vv]iridian).)*$")) &
        (F.col("location_name").rlike("^((?![Dd]iocese).)*$")) &
        (F.col("location_name").rlike("^((?![Hh]istory).)*$")) &
        (F.col("location_name").rlike("^((?![Ss]chool).)*$")) &
        (F.col("location_name").rlike("^((?![Tt]hougt).)*$")) &
        (F.col("location_name").rlike("^((?![Hh]oliness).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]artyr).)*$")) &
        (F.col("location_name").rlike("^((?![Jj]ames).)*$")) &
        (F.col("location_name").rlike("^((?![Ff]ellowship).)*$")) &
        (F.col("location_name").rlike("^((?![Hh]ouse).)*$")) &
        (F.col("location_name").rlike("^((?![Gg]lory).)*$")) &
        (F.col("location_name").rlike("^((?![Aa]nglican).)*$")) &
        (F.col("location_name").rlike("^((?![Pp]oetic).)*$")) &
        (F.col("location_name").rlike("^((?![Ss]anctuary).)*$")) &
        (F.col("location_name").rlike("^((?![Ee]quipping).)*$")) &
        (F.col("location_name").rlike("^((?![Jj]ohn).)*$")) &
        (F.col("location_name").rlike("^((?![Aa]ndrew).)*$")) &
        (F.col("location_name").rlike("^((?![Ee]manuel).)*$")) &
        (F.col("location_name").rlike("^((?![Rr]edeemed).)*$")) &
        (F.col("location_name").rlike("^((?![Pp]erfecting).)*$")) &
        (F.col("location_name").rlike("^((?![Aa]ngel).)*$")) &
        (F.col("location_name").rlike("^((?![Aa]rchangel).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]icheal).)*$")) &
        (F.col("location_name").rlike("^((?![Tt]hought).)*$")) &
        (F.col("location_name").rlike("^((?![Pp]ariosse).)*$")) &
        (F.col("location_name").rlike("^((?![Cc]osmas).)*$")) &
        (F.col("location_name").rlike("^((?![Dd]eliverance).)*$")) &
        (F.col("location_name").rlike("^((?![Ss]ociete).)*$")) &
        (F.col("location_name").rlike("^((?![Tt]emple).)*$")) &
        (F.col("location_name").rlike("^((?![Ss]eminary).)*$")) &
        (F.col("location_name").rlike("^((?![Ee]mployment).)*$")) &
        (F.col("location_name").rlike("^((?![Ii]nstitute).)*$")) &
        (F.col("location_name").rlike("^((?![Cc]amp).)*$")) &
        (F.col("location_name").rlike("^((?![Ss]tudent).)*$")) &
        (F.col("location_name").rlike("^((?![Ee]ducation).)*$")) &
        (F.col("location_name").rlike("^((?![Ss]ocial).)*$")) &
        (F.col("location_name").rlike("^((?![Ww]welfare).)*$")) &
        (F.col("location_name").rlike("^((?![Cc][Ee][Ss]).)*$")) &
        (F.col("location_name").rlike("^((?![Ff]amily).)*$")) &
        (F.col("location_name").rlike("^((?![Mm]ary).)*$")) &
        (F.col("location_name").rlike("^((?![Rr]ussian).)*$")) &
        (F.col("location_name").rlike("^((?![Bb]eautif).)*$")) &
        (F.col("location_name").rlike("^((?![Hh]eaven).)*$")) &    
        (F.col("location_name").rlike("^((?!Inc).)*$")) &
        (F.col("location_name").rlike("^((?!God).)*$"))
    ).dropDuplicates(
        ['placekey', 'date_range_start']
    )     

# COMMAND ----------

# MAGIC %md # Normalizing Sunday Visits

# COMMAND ----------

def avg_accumulator(acc, x):
    count = acc.count + 1
    sum = acc.sum + x
    return F.struct(count.alias("count"), sum.alias("sum"))

churches = churches.withColumns({
        'date_range_start': F.to_timestamp(F.col('date_range_start'), format="yyyy-MM-dd'T'HH:mm:ssxxx"),
        'state_sampling_rate': F.col('normalized_visits_by_state_scaling') / F.col('raw_visit_counts'),
        'visits_by_sunday': F.array_remove(
        F.transform('visits_by_day', lambda visits, i: F.when(
                i % 7 ==\
                    (F.dayofmonth(F.col('date_range_start')) - F.dayofweek(F.col('date_range_start')) + 7) % 7 # index of first Sunday
                , visits
            ).otherwise(-99999)
        ),
        -99999
        )
    }).select(
        'placekey',
        'date_range_start',
        'visitor_home_aggregation',
        F.aggregate( # sum
            F.map_values('visitor_home_aggregation'),
            F.lit(0),
            lambda acc, x: acc + x
        ).alias('reported_home_visitor_agg_total'),
        F.aggregate( # average
            F.transform('visits_by_sunday', lambda visits: F.floor(visits * F.col('state_sampling_rate'))),
            F.struct(F.lit(0).alias("count"), F.lit(0.0).alias("sum")),
            avg_accumulator,
            lambda acc: acc.sum / acc.count
        ).alias('avg_weekly_sunday_visits_scaled') # proxy for active church-goers
    )

# COMMAND ----------

# MAGIC %md # Scaling Visitor Counts by Tract

# COMMAND ----------

tract_target = churches.select(
        'placekey',
        'date_range_start',
        F.explode(
                F.transform_values(
                'visitor_home_aggregation',
                lambda _, visits: F.floor(visits * (F.col('avg_weekly_sunday_visits_scaled') / F.col('reported_home_visitor_agg_total')))
            )
        ).alias('tract', 'scaled_visitor_count')
    ).groupBy(
        "tract",
        "placekey"
    ).agg(
        F.median("scaled_visitor_count")
    )\
    .groupBy("tract")\
    .sum('median(scaled_visitor_count)')\
    .withColumnRenamed("sum(median(scaled_visitor_count))", "target")

# COMMAND ----------

display(tract_target.limit(100))

# COMMAND ----------

# MAGIC %md # Visualizations

# COMMAND ----------

import plotly.express as px

# COMMAND ----------

# MAGIC %md Choropleth

# COMMAND ----------

tract_target.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("tract_target")
county_target = spark.sql("""
    SELECT * 
    FROM tract_target AS target
    LEFT JOIN safegraph.tract_table AS tract
        ON target.tract = tract.tractcode
""")

# COMMAND ----------

pd_county_target = county_target\
    .groupBy('lat', 'long', 'county_name', 'county')\
    .sum('target')\
    .withColumnRenamed("sum(target)", "target")\
    .toPandas()

# COMMAND ----------


fig = px.scatter_geo(pd_county_target,
                    lat=pd_county_target["lat"],
                    lon=pd_county_target["long"],
                    scope="usa",
                    hover_name=pd_county_target["county_name"],
                    hover_data=["county"],
                    title="Active Members in Idaho by County",
                    width=1200,
                    height=800,
                    color=pd_county_target["target"],
                    range_color=[0,1000],
                    color_continuous_scale=px.colors.sequential.Bluered,
                    size=pd_county_target["target"],
                    size_max=75
                    )

fig.update_geos(fitbounds="locations")
fig.show()

# COMMAND ----------

# MAGIC %md Treemap

# COMMAND ----------

with_temple = tract_target.select("*").filter("tract IN (16019971200, 16001001900, 16083000900, 16065950302, 16001010335, 16005001101)").withColumn("has_temple", F.lit(1))
spark.sql("USE safegraph")
tract_table = spark.sql("SELECT tractcode, county_name FROM tract_table")
tree_map = tract_target.join(with_temple, on=["tract", "target"], how="left").fillna(0).filter(F.col("target") > 150).join(tract_table, on=tract_table["tractcode"] == tract_target["tract"], how="left")

tree_map_pd = tree_map.na.drop().toPandas()
fig = px.treemap(tree_map_pd, color="has_temple", path=["county_name", "tract"], values="target")
fig.update_traces(root_color="lightgrey")
fig.update(layout_coloraxis_showscale=False)
fig.show()
