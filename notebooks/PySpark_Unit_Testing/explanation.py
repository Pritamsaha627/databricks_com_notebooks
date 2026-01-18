# Databricks notebook source
file_location = "dbfs:/FileStore/source_file/races.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","


df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
df_final = df.groupBy('year').count().filter("count > 20")
display(df)
display(df_final)

# COMMAND ----------


