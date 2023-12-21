# Databricks notebook source
# MAGIC %run ./build_database

# COMMAND ----------

spark.sql("USE safegraph")
censusblock = spark.sql("SELECT * FROM censusblock_table")
censustract_pkmap = spark.sql("SELECT * FROM censustract_pkmap")

places = spark.sql("SELECT * FROM places")
patterns = spark.sql("SELECT * FROM patterns")
tract_table = spark.sql("SELECT * FROM tract_table")
spatial = spark.sql("SELECT * FROM spatial")

# COMMAND ----------

places_patterns = places.join(patterns, on=["placekey"], how="inner")
places_patterns.count()

# COMMAND ----------

from pyspark.sql.functions import *

def get_Sundays(days_list, day):
    sundays = []
    if day == 1:
        for i in range(len(days_list)):
            if i % 7 == 0:
                sundays.append(days_list[i])
    else:
        diff = 8 - day
        diff_list = days_list[diff:]
        for i in range(len(diff_list)):
            if i % 7 == 0:
                sundays.append(diff_list[i])
    return sundays

get_Sundays_UDF = udf(lambda x, y:get_Sundays(x, y))

churches = places_patterns.filter("category_tags = 'Churches'")\
                          .filter("location_name NOT LIKE '%Temple%' OR location_name NOT LIKE '%temple%'")\
                          .filter("location_name NOT LIKE '%Reorganized%'")\
                          .filter(lower(col("location_name")).like("%lds%") | lower(col("location_name")).like("%latter%saints%"))\
                          .withColumn("day_of_week", dayofweek(col("date_range_start")))\
                          .withColumn("visits_by_sunday", get_Sundays_UDF(col("visits_by_day"), col("day_of_week")))\
                          .withColumn("total_sunday_visits", col("popularity_by_day.Sunday"))
churches.display()
churches.count()

# COMMAND ----------

churches_weekly = churches.select("*", *[col("popularity_by_day")[key].alias(key) for key in churches.select("popularity_by_day").first()[0].keys()])

# COMMAND ----------

churches_weekly = churches_weekly.select("placekey", "location_name", "latitude", "longitude", "date_range_start",\
                                     "raw_visit_counts", "visitor_home_aggregation", "normalized_visits_by_state_scaling",\
                                     "day_of_week", "visits_by_sunday", "Monday", "Thursday", "Friday", "Sunday", "Wednesday",\
                                     "Tuesday", "Saturday")\
                                    .withColumn("sample_rate", round(col("normalized_visits_by_state_scaling") / col("raw_visit_counts"), 2))\
                                    .filter(
                                    (col("Sunday") > col("Monday")) &
                                    (col("Sunday") > col("Tuesday")) &
                                    (col("Sunday") > col("Wednesday")) &
                                    (col("Sunday") > col("Thursday")) &
                                    (col("Sunday") > col("Friday")) &
                                    (col("Sunday") > col("Saturday")))\
                                    .orderBy(["placekey", "date_range_start"])
churches_weekly.display()

# COMMAND ----------

churches_final = churches_weekly.select("*", explode("visitor_home_aggregation").alias("tract", "visitors"))\
                                        .withColumn("final_visitors (target)", round(col("sample_rate") * col("visitors")))\
                                        .orderBy("date_range_start", "placekey")
churches_final.display()

# COMMAND ----------

tracts_monthly = churches_final.select("*").groupBy("tract").agg(avg("final_visitors (target)").alias("target")).orderBy("tract")


tracts_monthly.display()
tracts_monthly.count()

# COMMAND ----------

county = tracts_monthly.join(tract_table, (tracts_monthly["tract"] == tract_table["tractcode"]), how="inner")
county.display()
