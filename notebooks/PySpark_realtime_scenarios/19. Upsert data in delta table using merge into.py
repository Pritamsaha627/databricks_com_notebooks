# Databricks notebook source
import pyspark.sql.functions as F
from delta.tables import *

# COMMAND ----------

raw_df = spark.read.csv('dbfs:/FileStore/source_file/races.csv',header= True)
raw_df.display()

# COMMAND ----------

df1 = raw_df.filter(F.col('raceID') <= 10).withColumn('current_date',F.current_timestamp()).withColumn('updated_date',F.lit('Null').cast('timestamp'))
df1.display()

# COMMAND ----------

df1.write.format('delta').save('/dbfs/FileStore/sink_file')

# COMMAND ----------

df2 = raw_df.filter('raceID between 6 and 20').withColumn('current_date',F.current_timestamp()).withColumn('updated_date',F.lit('Null').cast('timestamp'))
df2 = df2.withColumn('circuitId',F.concat(F.col('round'),F.col('circuitId')))
df2.display()

# COMMAND ----------

deltatable = DeltaTable.forPath(spark,'/dbfs/FileStore/sink_file')
deltatable.alias('target').merge(df2.alias('source'),"target.raceId = source.raceId").whenMatchedUpdate(set = {'updated_date':'current_timestamp','target.circuitId':'source.circuitId'}).whenNotMatchedInsertAll().execute()


# COMMAND ----------

df = spark.read.format('delta').load('/dbfs/FileStore/sink_file')
df.display()
