# Databricks notebook source
'''Find all the users who were active for 3 consecutive days or more.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/sf_events-1.csv',header = True)

df = df.withColumn('record_date',to_date('record_date','dd-MM-yyyy'))
df.display()

# COMMAND ----------

# Output :

+-------+
|user_id|
+-------+
|     U4|
+-------+

# COMMAND ----------

new_df = df.withColumn('prev_date',lag('record_date').over(Window.partitionBy('user_id').orderBy('record_date')))\
            .withColumn('next_date',lead('record_date').over(Window.partitionBy('user_id').orderBy('record_date')))
new_df.display()

# COMMAND ----------

final_df = new_df.filter((datediff('record_date','prev_date') == 1) & (datediff('next_date','record_date') == 1)).select('user_id')
final_df.display()
