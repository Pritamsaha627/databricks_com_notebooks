# Databricks notebook source
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

print(globals().items())

# COMMAND ----------

from pyspark.sql.functions import DataFrame
for key,val in globals().items():
    if type(val)==DataFrame:
        print(key)

# COMMAND ----------

from pyspark.sql.functions import DataFrame
for key,val in globals().items():
    if isinstance(val,DataFrame):
        print(key)
        val.show()

# COMMAND ----------


