# Databricks notebook source
'''Find the top 10 ranked songs in 2010. Output the rank, group name, and song name, but do not show the same song twice. Sort the result based on the rank in ascending order.'''

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/spotify_song_data.csv',header = True)
df = df.withColumn('year_rank',col('year_rank').cast('int'))
df.display()


# COMMAND ----------

new_df = df.filter((col('year') == '2010') & (col('year_rank').between(1,10)))
display(new_df)

# COMMAND ----------

final_df = new_df.select('year_rank','group_name','song_name').dropDuplicates().orderBy(asc('year_rank'))
display(final_df)

# COMMAND ----------

# Output : 
