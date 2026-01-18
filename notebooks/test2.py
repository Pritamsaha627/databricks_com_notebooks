# Databricks notebook source
from pyspark.sql.functions import col, desc, dense_rank

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test2").master("local[5]").getOrCreate()

# COMMAND ----------

demo_df = spark.read.csv('dbfs:/FileStore/demo12q4.txt',sep="$",header=True,inferSchema=True)


# COMMAND ----------

ddf = demo_df.filter(col('caseid') == '4011541')
ddf.display()

# COMMAND ----------

w_df = Window.partitionBy('caseid').orderBy(desc('caseversion'))

new_df = demo_df.withColumn('rnk',dense_rank().over(w_df))
final_df = new_df.filter(col('rnk') == 1).drop('rnk')

final_df.display()
