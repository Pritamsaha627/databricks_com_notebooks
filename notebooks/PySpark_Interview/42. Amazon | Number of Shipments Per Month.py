# Databricks notebook source
'''Write a query that will calculate the number of shipments per month. The unique key for one shipment is a combination of shipment_id and sub_id. Output the year_month in format YYYY-MM and the number of shipments in that month.'''

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/shipment_data_xlsx.csv',header = True)
df = df.withColumn('shipment_date',to_date('shipment_date','dd-MM-yyyy'))

df.show()


# COMMAND ----------

# Output:

# COMMAND ----------

new_df = df.withColumn('unique_key',concat('shipment_id','sub_id')).withColumn('year_month',date_format('shipment_date','yyyy-MM'))
new_df.show()

# COMMAND ----------

final_df = new_df.groupBy('year_month').agg(count('unique_key').alias('count'))
final_df.show()
