# Databricks notebook source
'''You're given a dataset of searches for properties on Airbnb. For simplicity, let's say that each search result (i.e., each row) represents a unique host. Find the city with the most amenities across all their host's properties. Output the name of the city.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/airbnb_search_details.txt',sep = '\t',header = True)
df = df.withColumn('host_since',to_date(col('host_since'),'dd-MM-yyyy'))
df.display()

# COMMAND ----------

# Output : 
+----+
|city|
+----+
| NYC|
+----+

# COMMAND ----------

new_df = df.withColumn('amenities',explode(split('amenities',','))).withColumn('amenities',regexp_replace('amenities','[{\"\ \}]+',""))
new_df.display()

# COMMAND ----------

final_df = new_df.groupBy('city').agg(countDistinct('amenities').alias('amenities_count')).orderBy(desc('amenities_count')).limit(1).select('city')
final_df.display()
