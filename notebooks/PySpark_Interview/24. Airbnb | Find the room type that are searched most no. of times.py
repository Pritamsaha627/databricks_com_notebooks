# Databricks notebook source
'''
Find the room type that are searched most no. of times. Output the room type alongside the number of searches for it. If the filter for room types has more than one room type, consider each unique room type as a seperate row. Sort the result based on no. of searches in descending order.
'''

# COMMAND ----------

data = [(1,'2022-01-01','entire home,couple room,private room'),(2,'2022-01-02','entire home,shared room'),(3,'2022-01-02','private room'),(4,'2022-01-03','private room'),(5,'2022-01-04','entire home,private room,shared room,couple room'),(6,'2022-01-05','entire home,shared room'),(7,'2022-01-06','private room,couple room,private room'),(8,'2022-01-07','entire home,shared room'),(9,'2022-01-08','private room,shared room'),(10,'2022-01-09','entire home')]

schema = ['user_id','date_searched','filter_room_types']
df = spark.createDataFrame(data,schema)
df.show(truncate = False)

# COMMAND ----------

Output:
+-----------------+---+
|filter_room_types|cnt|
+-----------------+---+
|     private room|  7|
|      entire home|  6|
|      shared room|  5|
|      couple room|  3|
+-----------------+---+

# COMMAND ----------

from pyspark.sql.functions import col, explode, split, count, desc

# COMMAND ----------

new_df = df.withColumn('filter_room_types',split('filter_room_types',','))
new_df.show(truncate = False)

# COMMAND ----------

new_df = new_df.withColumn('filter_room_types',explode('filter_room_types'))
new_df.show()

# COMMAND ----------

final_df = new_df.groupBy('filter_room_types').agg(count('filter_room_types').alias('cnt')).orderBy(desc('cnt'))
final_df.show()
