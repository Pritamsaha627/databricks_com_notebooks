# Databricks notebook source
# MAGIC %fs
# MAGIC ls "/databricks-datasets/COVID/ESRI_hospital_beds/"

# COMMAND ----------

df = spark.read.csv('/databricks-datasets/COVID/ESRI_hospital_beds/*.csv',header=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df = df.withColumn('filename',input_file_name())
display(df)

# COMMAND ----------

display(df.groupBy("filename").count())

# COMMAND ----------


