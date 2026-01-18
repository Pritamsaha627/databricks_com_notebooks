# Databricks notebook source
# MAGIC %md
# MAGIC List the airlines that operate flights to all available destination.

# COMMAND ----------

air_data = [(1,'Airline A'),(2,'Airline B'),(3,'Airline C')]

air_schema = ['airline_id' ,'airline_name']

air_df = spark.createDataFrame(air_data,air_schema)
air_df.show()

# COMMAND ----------

flight_data = [(1,1,101),(2,1,102),(3,2,101),(4,2,103),(5,3,101),(6,3,102),(7,3,103)]

flight_schema = ['flight_id','airline_id' ,'airport_id']

flight_df = spark.createDataFrame(flight_data,flight_schema)
flight_df.show()

# COMMAND ----------

count_airport = flight_df.select('airport_id').distinct().count()
display(count_airport)

# COMMAND ----------

from pyspark.sql.functions import countDistinct, col

flight_filter_df = flight_df.groupBy('airline_id').agg(countDistinct('airport_id').alias('distinct_airport_count')).filter(col('distinct_airport_count')==count_airport)
flight_filter_df.show()

# COMMAND ----------

final_df = flight_filter_df.join(air_df,['airline_id']).select('airline_id','airline_name')
final_df.show()
