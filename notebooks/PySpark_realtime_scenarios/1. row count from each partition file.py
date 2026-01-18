# Databricks notebook source
# MAGIC %fs
# MAGIC ls dbfs:/temp/student

# COMMAND ----------

df = spark.read.csv("dbfs:/temp/student/*.csv",header=True)
df.show()

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df = df.withColumn("filename",input_file_name())
df.show(truncate = False)

# COMMAND ----------

df.groupBy('filename').count().show(truncate=False)
