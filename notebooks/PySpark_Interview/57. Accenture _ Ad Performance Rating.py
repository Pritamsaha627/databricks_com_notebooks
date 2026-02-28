# Databricks notebook source
'''Following a recent advertising campaign, the marketing department wishes to classify its efforts based on the total number of units sold for each product.


You have been tasked with calculating the total number of units sold for each product and categorizing ad performance based on the following criteria for items sold:


Outstanding: 30+
Satisfactory: 20 - 29
Unsatisfactory: 10 - 19
Poor: 1 - 9


Your output should contain the product ID, total units sold in descending order, and its categorized ad performance.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('/Volumes/test_catalog/default/test_volume/pyspark_interview_source/Accenture_marketing_campaign.csv',header = True)
df = df.withColumn('created_at',to_date('created_at','yyyy-MM-dd'))
df.display()

# COMMAND ----------

new_df = df.groupBy('product_id').agg(sum('quantity').alias('total_units_sold'))
display(new_df)

# COMMAND ----------

final_df = new_df.withColumn('ad_performance',when(col('total_units_sold') >= 30,'Outstanding').when(col('total_units_sold') >= 20,'Satisfactory').when(col('total_units_sold') >= 10,  'UnSatisfactory').otherwise('Poor'))
final_df.orderBy(desc("total_units_sold")).display()
