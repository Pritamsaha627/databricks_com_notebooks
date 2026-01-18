# Databricks notebook source
'''
Write a PySpark query to find out the duplicate emails.
'''

# COMMAND ----------

data = [(1,'virat@abc.com'),(2,'rohit@abc'),(3,'shami@abc.com'),(4,'virat@abc.com')]
schema = ['id','email']
df = spark.createDataFrame(data,schema)
df.show()
df.createOrReplaceTempView('tp')

# COMMAND ----------

from pyspark.sql.functions import col

df.groupBy('email').count().filter(col('count')>1).select(col('email')).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select email from
# MAGIC (select count(*),email from tp 
# MAGIC group by email 
# MAGIC having count(*)>1)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH count_view AS (
# MAGIC   select count(*),email from tp 
# MAGIC group by email 
# MAGIC having count(*)>1
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT email FROM count_view
