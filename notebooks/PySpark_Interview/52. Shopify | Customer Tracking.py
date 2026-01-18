# Databricks notebook source
'''Given the users' sessions logs on a particular day, calculate how many hours each user was active that day.
Note: The session starts when state=1 and ends when state=0.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/cust_tracking.csv',header = True)

df = df.withColumn('timestamp',to_timestamp(col('timestamp'),'dd-MM-yyyy HH:mm'))
df.display()

# COMMAND ----------

# Output :

+-------+-----------+
|cust_id|active_time|
+-------+-----------+
|   c001|        5.0|
|   c002|        4.5|
|   c003|        1.5|
|   c004|        2.0|
|   c005|        7.5|
+-------+-----------+


# COMMAND ----------

new_df = df.withColumn('logOut',lead('timestamp').over(Window.partitionBy('cust_id').orderBy('timestamp'))).filter(col('state') == 1)
new_df.display()

# COMMAND ----------

final_df = new_df.withColumn('active_time',((col('logOut').cast('long') - col('timestamp').cast('long'))/3600))
final_df = final_df.groupBy('cust_id').agg(sum('active_time').alias('total_active_time'))
final_df.display()
