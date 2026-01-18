# Databricks notebook source
'''We have data on rental properties and their owners. Write a query that figures out how many different apartments (use unit_id) are owned by people under 30, broken down by their nationality. We want to see which nationality owns the most apartments, so make sure to sort the results accordingly.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

airbnb_hosts = spark.read.csv('dbfs:/FileStore/airbnb_hosts.csv',header = True)
airbnb_hosts.display()
airbnb_units = spark.read.csv('dbfs:/FileStore/airbnb_units.csv',header = True)
airbnb_units.display()

# COMMAND ----------

df = airbnb_hosts.join(airbnb_units,['host_id'],'inner')
df.display()

# COMMAND ----------

new_df = df.filter((col('age') <= 30) & (col('unit_type') == 'Apartment'))
new_df.display()

# COMMAND ----------

final_df = new_df.groupBy('nationality').agg(countDistinct('unit_id').alias('apartment_count')).orderBy(desc('apartment_count')).limit(1)

final_df.display()

# COMMAND ----------


# Output :
+-----------+---------------+
|nationality|apartment_count|
+-----------+---------------+
|        USA|              2|
+-----------+---------------+

