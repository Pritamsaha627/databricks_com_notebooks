# Databricks notebook source
'''Find the best-selling item for each month (no need to separate months by year). The best-selling item is determined by the highest total sales amount, calculated as: total_paid = unitprice * quantity. Output the month, description of the item, and the total amount paid.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/Ebay_online_retail.csv',header = True)
df = df.withColumn('invoicedate',to_date(col('invoicedate'),'dd-MM-yyyy'))
df.display()

# COMMAND ----------

# Output : 

# COMMAND ----------

new_df = df.withColumn('total_paid',col('quantity')*col('unitprice')).withColumn('month',month('invoicedate'))
new_df.display()

# COMMAND ----------

new_df = new_df.groupBy('month','description').agg(sum('total_paid').alias('total_paid'))
new_df.display()

# COMMAND ----------

final_df = new_df.withColumn('rnk',dense_rank().over(Window.partitionBy('month').orderBy(desc('total_paid')))).filter(col('rnk') == 1).drop('rnk')
final_df.display()
