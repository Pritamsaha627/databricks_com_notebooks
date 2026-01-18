# Databricks notebook source
from pyspark.sql.types import DateType, TimestampType
from datetime import datetime
from pyspark.sql.functions import col, input_file_name, regexp_extract,date_format, to_date


file_df = spark.read.csv('dbfs:/FileStore/source_file/EmpDetails_2024_01_16_111530_131.csv',header=True).withColumn('sourcefile_path', input_file_name())
file_df.show(truncate=False)


# COMMAND ----------

regex_str = "[A-Za-z]+:/[A-Za-z]+/([A-Za-z0-9]+(_[A-Za-z0-9]+)+)/([A-Za-z0-9]+(_[A-Za-z0-9]+)+)\.[A-Za-z]+"
file_df = file_df.withColumn("sourcefile", regexp_extract("sourcefile_path",regex_str,3)).drop('sourcefile_path')
file_df.show(truncate=False)

# COMMAND ----------

filename = "EmpDetails_%Y_%m_%d_%H%M%S_%f"
func =  udf (lambda x: datetime.strptime(x, filename), TimestampType())

# COMMAND ----------

final_df = file_df.withColumn("SourceFileDateTime",date_format(func(col('sourcefile')), 'yyyy-MM-dd HH:mm:ss.SSS'))

display(final_df)
