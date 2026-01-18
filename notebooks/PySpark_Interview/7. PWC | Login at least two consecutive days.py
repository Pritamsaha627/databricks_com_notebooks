# Databricks notebook source
""" Retrieve information about cosecutive login streaks for employee who have logged in for at least two consecutive days."""

# COMMAND ----------

data = [(101,'02-01-2024','N'),
        (101,'03-01-2024','Y'),
        (101,'04-01-2024','N'),
        (101,'07-01-2024','Y'),
        (102,'01-01-2024','N'),
        (102,'02-01-2024','Y'),
        (102,'03-01-2024','Y'),
        (102,'04-01-2024','N'),
        (102,'05-01-2024','Y'),
        (102,'06-01-2024','Y'),
        (102,'07-01-2024','Y'),
        (103,'01-01-2024','N'),
        (103,'04-01-2024','N'),
        (103,'05-01-2024','Y'),
        (103,'06-01-2024','Y'),
        (103,'07-01-2024','N')]
schema = "emp_id int, log_date string, flag string"

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import to_date, col
df =  df.select('*',to_date(col('log_date'),"dd-MM-yyyy").alias('log_date_format')).drop('log_date')
df.show()

# COMMAND ----------

filter_df = df.filter(col('flag') == 'Y')
filter_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, day, abs

w_df = Window.partitionBy(col('emp_id')).orderBy('log_date_format')
filter_df = filter_df.withColumn('rank',rank().over(w_df)).withColumn('date',day('log_date_format')).withColumn('diff', abs(col('rank')-col('date')))
filter_df.show()

# COMMAND ----------

from pyspark.sql.functions import max, min, count
final_df = filter_df.groupBy('emp_id','diff').agg(count('*').alias('consicutive_day'),min('log_date_format').alias('start_date'),max('log_date_format').alias('end_date')).drop('diff').filter(col('consicutive_day')>=2)
final_df.show()

# COMMAND ----------


