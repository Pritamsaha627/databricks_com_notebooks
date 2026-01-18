# Databricks notebook source
from pyspark.sql.functions import col, to_date,concat,lit
from delta.tables import *

# COMMAND ----------

sales_data = [(1,320),(1,330),(2,340),(2,350),(2,360),(2,370),(2,380),(2,390),(2,420),(3,440),(4,460),(5,430),(5,490),]

sales_schema = ['product_id', 'sale_value']

sales_df = spark.createDataFrame(sales_data,sales_schema)
