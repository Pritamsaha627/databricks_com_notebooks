# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test1").master("local[5]").getOrCreate()

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/source_file/BrazilEduPanel_Municipal.csv',sep=",",header=True,inferSchema=True)


# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

print(len(df.columns))

# COMMAND ----------

df1 = spark.read.csv('dbfs:/FileStore/source_file/BrazilEduPanel_School.csv',sep=",",header=True,inferSchema=True)


# COMMAND ----------

df1.limit(10).display()

# COMMAND ----------

print(len(df1.columns))

# COMMAND ----------

df
