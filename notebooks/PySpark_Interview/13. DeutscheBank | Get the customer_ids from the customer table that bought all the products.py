# Databricks notebook source
'''
Write a query to get the customer_ids from the customer table that bought all the products in the product table.
'''

# COMMAND ----------

cust_data = [(1,5),(2,6),(3,5),(3,6),(1,6),(3,5),(2,6)]
cust_schema = ['customer_id','product_key']
df_cust = spark.createDataFrame(cust_data,cust_schema)
df_cust.show()

# COMMAND ----------

product_data = [(5,),(6,)]
product_schema = ['product_key']
df_product = spark.createDataFrame(product_data,product_schema)
df_product.show()

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct

df_cust = df_cust.groupBy(col("customer_id")).agg(countDistinct(col("product_key")).alias("product_count"))
df_product = df_product.agg(countDistinct(col("product_key")).alias("total_product_count"))
df_cust.show()
df_product.show()

# COMMAND ----------

df_cust.join(df_product,df_cust.product_count == df_product.total_product_count).select(col('customer_id')).show()

# COMMAND ----------


