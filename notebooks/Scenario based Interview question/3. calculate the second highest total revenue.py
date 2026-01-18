# Databricks notebook source
'''Write PySpark solution to calculate the second highest total revenue'''

# COMMAND ----------

data = [('North','Feb',100),('South','Jan',500),('East','Apr',200),('West','July',700),('Central','Jan',300),
        ('North','Mar',100),('South','Apr',100),('East','May',400),('West','June',300),('Central','July',100)]

schema = ['region', 'month','revenue']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, max, rank
from pyspark.sql.window import Window
df = df.groupBy('region').agg(sum('revenue').alias('total_revenue'))
df.show()

# COMMAND ----------

df_win = Window.orderBy(col('total_revenue').desc(),col('region').asc())
final_df = df.withColumn('rank',rank().over(df_win))
final_df.show()

# COMMAND ----------

final_df.filter(col('rank')==2).show()
