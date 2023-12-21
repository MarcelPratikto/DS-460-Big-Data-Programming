# Databricks notebook source
!pip install polars

# COMMAND ----------


import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.functions import dayofweek, date_add, to_date, weekofyear
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StructType, StructField

# COMMAND ----------

# MAGIC %md
# MAGIC Read in Idaho Places and Patterns data

# COMMAND ----------

patterns = spark.read.parquet("dbfs:/FileStore/patterns__2_.parquet")
idaho_places = spark.read.parquet("dbfs:/data/idaho/places.parquet")
tract_table = spark.read.parquet("dbfs:/data/idaho/tract_table.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Filter to just LDS locations in places using regex

# COMMAND ----------

lds_church = idaho_places.filter(
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
  )

lds_churches = lds_church.dropDuplicates(['placekey'])

# COMMAND ----------

# MAGIC %md
# MAGIC Create count column to count the number of months for each placekey

# COMMAND ----------


distinct = patterns.dropDuplicates(['placekey', 'date_range_start'])



count = distinct.groupby("placekey").count()

patterns_w_count = distinct.join(count, 'placekey', how = 'left')


# COMMAND ----------

# MAGIC %md
# MAGIC Join filtered places with patterns data

# COMMAND ----------

lds_patterns = patterns_w_count.join(lds_churches, 'placekey', how = 'inner')
lds_patterns.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Sunday Ratio column and 

# COMMAND ----------

lds_patterns1 = lds_patterns.withColumn("day_of_week", dayofweek(to_date("date_range_start")))
row = lds_patterns1.select("date_range_start", "day_of_week").first()

print("Date:", row['date_range_start'])
print("Day of the week (1=Sunday):", row['day_of_week'])


# COMMAND ----------

def get_sunday_indices(start_day_of_week, visit_by_day_len):
    offset = (7-start_day_of_week+1) % 7 
    sunday_indices = [i for i in range(offset, visit_by_day_len,7)]
    return sunday_indices

# COMMAND ----------

#lds_patterns.display()

# COMMAND ----------

def sum_and_count_sundays(day_of_week, visits_by_day):
    sunday_indices = get_sunday_indices(day_of_week, len(visits_by_day))
    number_of_sundays = len(sunday_indices)
    sum_of_visits_on_sundays = sum(visits_by_day[i] for i in sunday_indices)
    return (sum_of_visits_on_sundays, number_of_sundays)


# COMMAND ----------

# Python function to a UDF ( User Defined )
sum_and_count_sundays_schema = StructType([
    StructField("sum_of_visits", IntegerType(), nullable=False),
    StructField("number_of_sundays", IntegerType(), nullable=False)
])

sum_and_count_sundays_udf = udf(sum_and_count_sundays, sum_and_count_sundays_schema)

lds_patterns2 = lds_patterns1.withColumn(
    "sunday_metrics",
    sum_and_count_sundays_udf("day_of_week", "visits_by_day")
)

lds_patterns3 = lds_patterns2.withColumn("sunday_visits", lds_patterns2["sunday_metrics"]["sum_of_visits"])
lds_patterns4 = lds_patterns3.withColumn("number_of_sundays", lds_patterns3["sunday_metrics"]["number_of_sundays"])

lds_patterns4 = lds_patterns4.withColumn("sunday_ratio", 
                                       F.when(lds_patterns4["number_of_sundays"] > 0, 
                                              lds_patterns4["sunday_visits"] / lds_patterns4["raw_visit_counts"])
                                       .otherwise(0))  



# COMMAND ----------


#lds_patterns.display()

# COMMAND ----------

from pyspark.sql.functions import explode, col
exploded_df = lds_patterns4.select("placekey", "raw_visitor_counts", "location_name", "date_range_start", "popularity_by_hour","day_of_week", "sunday_metrics","sunday_visits", "number_of_sundays","sunday_ratio", explode(col("popularity_by_day")).alias("day", "popularity"))

# Display the exploded DataFrame
#display(exploded_df)

# COMMAND ----------

from pyspark.sql.functions import explode, col, when

exploded_df2 = exploded_df.withColumn("is_sunday", when(col("day") == "Sunday", True).otherwise(False))

# Display the modified DataFrame
#display(exploded_df2)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window



# Group by 'placekey' and calculate the maximum popularity for each group
max_popularity_df = exploded_df2.groupBy("placekey").agg(F.max("popularity").alias("max_popularity"))

# Calculate the maximum popularity for Sunday
sunday_max_popularity = exploded_df2.filter(exploded_df2["is_sunday"]).groupBy("placekey").agg(F.max("popularity").alias("sunday_max_popularity"))

# Join the data with the maximum popularity for each day
final_result = exploded_df2.join(max_popularity_df, ["placekey"], "left")
final_result = final_result.join(sunday_max_popularity, ["placekey"], "left")

# Add a column 'most_popular_day' to indicate the most popular day
final_result = final_result.withColumn("most_popular_day", F.when(final_result["max_popularity"] == final_result["sunday_max_popularity"], "Sunday").otherwise("Other"))

# Display the final result


sunday_ratio_table = final_result.groupBy("placekey").agg(F.avg("sunday_ratio"))


# COMMAND ----------

# MAGIC %md
# MAGIC Add more columns 

# COMMAND ----------

# Create the 'max_popularity_difference' column
final_result = final_result.withColumn("max_popularity_difference", final_result["max_popularity"] - final_result["sunday_max_popularity"])

# Display the final result
#display(final_result)

# COMMAND ----------

from pyspark.sql.functions import col

# Create the 'percentage_difference' column
final_result = final_result.withColumn("percentage_difference", col("sunday_max_popularity") / col("max_popularity"))

# Display the final result
#display(final_result)


# COMMAND ----------

from pyspark.sql import functions as F

# Group the data by 'placekey' and calculate the sum of 'max_popularity_difference' for each group
grouped_final_result = final_result.groupBy("placekey").agg((F.sum("max_popularity_difference") / 14).alias("average_difference"))

# Display the grouped result
#display(grouped_final_result)


# COMMAND ----------

from pyspark.sql import functions as F

# Group the data by 'placekey' and calculate the average 'percentage_difference' for each group
grouped_final_result = final_result.groupBy("placekey").agg(F.avg("percentage_difference").alias("average_percentage_difference"))

from pyspark.sql.functions import desc

# Order the DataFrame by "average_percentage_difference" in descending order
ordered_final_result = grouped_final_result.orderBy(desc("average_percentage_difference"))

# Display the ordered result
#display(ordered_final_result)

from pyspark.sql import functions as F

# Group the data by 'placekey' and calculate the average 'percentage_difference' for each group
grouped_final_result = final_result.groupBy("placekey").agg(
    F.max("max_popularity").alias("max_popularity"),  # Include additional columns as needed
    F.max("sunday_max_popularity").alias("sunday_max_popularity"),
    F.avg("max_popularity_difference").alias("average_difference"),
    F.avg("percentage_difference").alias("average_percentage_difference")
)

from pyspark.sql.functions import desc

# Order the DataFrame by "average_percentage_difference" in descending order
ordered_final_result = grouped_final_result.orderBy(desc("average_percentage_difference"))

# Display the ordered result






# COMMAND ----------


filtered_patterns = patterns_w_count.join(lds_churches, 'placekey', how = 'inner')

# COMMAND ----------

normalized_values = filtered_patterns.withColumn('sampling_ratio',
    col('normalized_visits_by_state_scaling') / col('raw_visit_counts'))


# COMMAND ----------

join1 = normalized_values.join(ordered_final_result, "placekey", how = "left")

final_table = join1.join(sunday_ratio_table, "placekey", how = "left")


# COMMAND ----------

tract_stuff = final_table.select("sampling_ratio", "average_difference", "average_percentage_difference", "avg(sunday_ratio)","count", explode("visitor_home_aggregation").alias("tractcode", "num_of_visitors"))

# COMMAND ----------

tract_stuff_final = tract_stuff.withColumn("calculated_tract_pop", ((tract_stuff["num_of_visitors"] / tract_stuff["count"])  * tract_stuff["sampling_ratio"]  ) * tract_stuff["avg(sunday_ratio)"])
                                         

# COMMAND ----------

from pyspark.sql.functions import col, ceil
tract_final = tract_stuff_final.groupBy("tractcode").agg(
    F.sum("calculated_tract_pop").alias("num_of_LDS_member")
)
tract_final2 = tract_final.withColumn("rounded_member_value", ceil(col("num_of_LDS_member"))).drop("num_of_LDS_member")

# COMMAND ----------

tract_table2 = tract_table.join(tract_final2, "tractcode", how = "left").fillna(0)

# COMMAND ----------

display(tract_table2)
