# Databricks notebook source
'''
Count rows in each column where Nulls are present.
'''

# COMMAND ----------

data = [(1,'Virat',90),(2,'Anil','Null'),(3,'Sachin','Null'),(4,'Sourav',60),(5,'Null','Null'),(6,'Null',60)]

schema = ['id', 'name', 'age']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, when,count

df_final = df.select([count(when(col(i)=='Null',1)).alias(i) for i in df.columns])
df_final.show()

# COMMAND ----------

df.createOrReplaceTempView('tp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC SUM(CASE WHEN id == 'Null' THEN 1 ELSE 0 END) AS id,
# MAGIC SUM(CASE WHEN name == 'Null' THEN 1 ELSE 0 END) AS name,
# MAGIC SUM(CASE WHEN age == 'Null' THEN 1 ELSE 0 END) AS age
# MAGIC FROM tp
