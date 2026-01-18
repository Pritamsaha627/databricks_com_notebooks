# Databricks notebook source
df = spark.read.csv('dbfs:/FileStore/source_file/races.csv',header=True)
df.show()
df.count()

# COMMAND ----------

df.write.format('csv').mode('overwrite').option('maxRecordsPerFile','46').save("dbfs:/tmp/race_part")

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df_new = spark.read.csv('dbfs:/tmp/race_part',header=True).withColumn('file_name',input_file_name())
display(df_new.groupBy("file_name").count())

# COMMAND ----------


