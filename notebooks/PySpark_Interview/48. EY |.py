# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [('success','01-08-2025'),('success','02-08-2025'),('fail','03-08-2025'),('fail','04-08-2025'),('success','13-08-2025')]
schema = ['job_status','run_date']

df = spark.createDataFrame(data,schema)
df = df.withColumn('run_date',to_date('run_date','dd-MM-yyyy'))
df.display()

# COMMAND ----------

new_df = df.withColumn('row_number1',row_number().over(Window.orderBy('run_date'))).withColumn('row_number2',row_number().over(Window.partitionBy('job_status').orderBy('run_date')))
new_df.show()

# COMMAND ----------

final_df = new_df.withColumn('diff',col('row_number1') - col('row_number2'))
final_df = final_df.groupBy('job_status','diff').agg(min('run_date').alias('start_date'),max('run_date').alias('end_date')).drop('diff')
final_df.show()

# COMMAND ----------

# Output :
+----------+----------+----------+
|job_status|start_date|  end_date|
+----------+----------+----------+
|      fail|2025-08-03|2025-08-04|
|   success|2025-08-01|2025-08-02|
|   success|2025-08-13|2025-08-13|
+----------+----------+----------+
