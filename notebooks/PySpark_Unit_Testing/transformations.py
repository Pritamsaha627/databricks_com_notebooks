# Databricks notebook source
def load_file(spark,data_file):
    return spark.read.option("header","true").option("inferschema","true").csv(data_file)

# COMMAND ----------

def count_by_year(race_df):
    return race_df.groupBy('year').count().filter("count > 20")
