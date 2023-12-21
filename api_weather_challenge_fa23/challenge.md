## Extract, Transform, and Load (API Databricks Style)

In the modern age of data science, we have access to APIs (Application Programming Interfaces) to __extract__ data from external sources. That data often comes in a [JSON format](https://en.wikipedia.org/wiki/JSON). We will need to __transform__ (parse and minor manipulations) that data and __load__ it into our cloud data environment.

Too often, big data examples skip over the ETL portion of the process. This challenge will force your team to experience part of the process. Once your team has completed this challenge, you will gain access to a professional Databricks _big_ data environment to continue our SafeGraph challenge. We often think about batch processing, but streaming ETL is also possible with APIs when thinking about ETL. This challenge will focus on a batch ETL.

### Challenge Details

You are expected to complete the following. You will complete a pull request for the items and submit your work to this repository. In addition, you will submit your `.html` file in Canvas.  

1. Select a latitude and longitude within your hometown (use the location of a family friend if you are not from the US).
2. Pull data from the [Daily Forecast 16-day API](https://openweathermap.org/forecast16) for the CBG for the next 16 days and print a snippet of the JSON file in your notebook.
3. Pull data from the [history API](https://openweathermap.org/history) for seven days in the past year for the same CBG and print a snippet of the JSON file in your notebook.
4. Store the information from each API request in a Spark DataFrame and display your dataframe in your notebook (see the image below and the table schema for examples of how your table should look).   
5. Use the data from the created tables to create two visualizations (You can't use bar charts).
6. Use the data from the created tables to create two summary tables (tables you would see in a report).
7. A short document that explains the differences between REST and GraphQL APIs (2-3 paragraphs).

Please don't use code that solves this specific problem from students or generative AI as support. You can use Google and others as much as possible with those two exceptions. 

### References

#### Code Help

- [Using the Open Weather Map API with Python](https://knasmueller.net/using-the-open-weather-map-api-with-python)
- [OpenWeatherMap API Python](https://rapidapi.com/blog/openweathermap-api-overview/python/)
- [Postman](https://www.postman.com/)
- [PySpark Read JSON File into DataFrame](https://sparkbyexamples.com/pyspark/pyspark-read-json-file-into-dataframe/)
- [Rest API to Spark DataFrame](https://stackoverflow.com/questions/68287551/nested-json-from-rest-api-to-pyspark-dataframe)
- [spark.sparkContext.parallelize()](https://sparkbyexamples.com/pyspark/pyspark-parallelize-create-rdd/)
- [Flatten Struct Columns in DataFrames](https://stackoverflow.com/questions/38753898/how-to-flatten-a-struct-in-a-spark-dataframe)

#### Background

- [Batch ETL vs. Streaming ETL](https://www.upsolver.com/blog/etl-process-flow-batch-etl-vs-streaming-etl)
- [Wikipedia on JSON](https://en.wikipedia.org/wiki/JSON)
- [API background](APIs/readme.md)

### Table image

![](screenshot.png)

### Table Schema

__History__

```
root
 |-- id: long (nullable = true)
 |-- lat: string (nullable = false)
 |-- lon: string (nullable = false)
 |-- dt: string (nullable = true)
 |-- wind_speed: double (nullable = true)
 |-- wind_gust: double (nullable = true)
 |-- wind_deg: long (nullable = true)
 |-- clouds_all: long (nullable = true)
 |-- pressure: long (nullable = true)
 |-- rain_1h: double (nullable = true)
 |-- temp_min: double (nullable = true)
 |-- temp_max: double (nullable = true)
 |-- temp: double (nullable = true)
 |-- weather: struct (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- icon: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- main: string (nullable = true)

```

__Forecast__

```
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- lon: double (nullable = true)
 |-- timezone: long (nullable = true)
 |-- population: long (nullable = true)
 |-- dt: string (nullable = true)
 |-- sunrise: string (nullable = true)
 |-- sunset: string (nullable = true)
 |-- pop: double (nullable = true)
 |-- speed: double (nullable = true)
 |-- gust: double (nullable = true)
 |-- deg: long (nullable = true)
 |-- clouds: long (nullable = true)
 |-- pressure: long (nullable = true)
 |-- rain: double (nullable = true)
 |-- temp_min: double (nullable = true)
 |-- temp_max: double (nullable = true)
 |-- temp_morn: double (nullable = true)
 |-- temp_day: double (nullable = true)
 |-- temp_eve: double (nullable = true)
 |-- temp_night: double (nullable = true)
 |-- feels_like_morn: double (nullable = true)
 |-- feels_like_day: double (nullable = true)
 |-- feels_like_eve: double (nullable = true)
 |-- feels_like_night: double (nullable = true)
 |-- weather: struct (nullable = true)
 |    |-- description: string (nullable = true)
 |    |-- icon: string (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- main: string (nullable = true)
```
