# Databricks notebook source

'''You have the marketing_campaign table, which records in-app purchases by users. Users making their first in-app purchase enter a marketing campaign, where they see call-to-actions for more purchases. Find how many users made additional purchases due to the campaign's success.

The campaign starts one day after the first purchase. Users with only one or multiple purchases on the first day do not count, nor do users who later buy only the same products from their first day.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/marketing_campaign.csv',header = True)
df = df.withColumn('created_at',to_date('created_at','dd-MM-yyyy'))
df.display()


# COMMAND ----------

new_df = df.withColumn('campaign_rnk',dense_rank().over(Window.partitionBy('user_id').orderBy('created_at'))).withColumn('product_rnk',dense_rank().over(Window.partitionBy('user_id','product_id').orderBy('created_at')))
new_df.display()

# COMMAND ----------

final_df = new_df.filter((col('campaign_rnk') > 1) & (col('product_rnk') < 2)).select(countDistinct('user_id').alias('user_count'))
final_df.display()

# COMMAND ----------

# Output :

+----------+
|user_count|
+----------+
|        23|
+----------+

