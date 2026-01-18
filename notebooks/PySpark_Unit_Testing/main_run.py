# Databricks notebook source
# MAGIC %run ./transformations

# COMMAND ----------

file_path = '/FileStore/source_file/races.csv'

race_df = load_file(spark,file_path)
final_df = count_by_year(race_df)

final_df.show()

# COMMAND ----------


