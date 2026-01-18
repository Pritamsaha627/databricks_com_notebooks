# Databricks notebook source
# MAGIC %md
# MAGIC TechMahindra Interview Question in PySpark

# COMMAND ----------

"""
you have two tables. One is for customer details and another is for order details. Find all the customers without a single order.
"""

# COMMAND ----------

cust_data = [(1,'Virat','Delhi'),(2,'Rohit','Mumbai'),(3,'Shami','Kolkata'),(4,'Bumrah','Gujrat'),(5,'Bhuvi','Pune')]
cust_schema = ['cust_id','name','city']
cust_df = spark.createDataFrame(cust_data,cust_schema)
display(cust_df)

# COMMAND ----------

order_data = [(205,2),(218,3),(314,1),(165,2)]
order_schema = ['order_id','cust_id']
order_df = spark.createDataFrame(order_data,order_schema)
display(order_df)

# COMMAND ----------

df = cust_df.join(order_df,['cust_id'],'left')
display(df)

# COMMAND ----------

df.filter(df.order_id.isNull()).select(df.name).show()

# COMMAND ----------

cust_data = [(1,'Virat'),(2,'Rohit'),(3,'Shami'),(4,'Bumrah'),(5,'Bhuvi')]
cust_schema = ['emp_id','name']
cust_df = spark.createDataFrame(cust_data,cust_schema)
display(cust_df)

# COMMAND ----------

from pyspark.sql.functions import *
cust_df = cust_df.withColumn('timestamp_UTC',lit(current_timestamp()))
display(cust_df)
