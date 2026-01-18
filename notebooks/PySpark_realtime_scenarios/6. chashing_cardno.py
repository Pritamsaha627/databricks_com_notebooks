# Databricks notebook source
data = [(1,'Rahul',1234567891234567),(2,'Raj',1234567892345678),(3,'Priya',1234567893456789),(3,'Murti',1234567890123456)]
schema = "id int, name string, card_no long"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

def create_hash(num):
    return '************' + str(num)[-4:]

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.functions import StringType

createHash = udf(lambda x : create_hash(x), StringType())

df.withColumn('num_hash',createHash(df.card_no)).show()


# COMMAND ----------

# MAGIC %fs
