# Databricks notebook source
'''
Data validation between source and target table
'''

# COMMAND ----------

source_data = [(1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E')]
source_schema = ['id','name']
source_df = spark.createDataFrame(source_data,source_schema)
source_df.show()

target_data = [(1,'A'),(2,'B'),(3,'X'),(4,'F'),(6,'G')]
target_schema = ['id','name']
target_df = spark.createDataFrame(target_data,target_schema)
target_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, coalesce
df = source_df.alias("t1").join(target_df.alias("t2"),on = (col("t1.id") == col("t2.id")),how='full')
df.show()

# COMMAND ----------

df = df.select(col("t1.id").alias("source_id"),col("t2.id").alias("target_id"),col("t1.name").alias("source_name"),col("t2.name").alias("target_name"))
df.show()

# COMMAND ----------

df= df.withColumn("comment",when((col("source_id") == col("target_id")) & (col("source_name") != col("target_name")),"Mismatched")\
    .when(col("target_id").isNull(),"new in source").when(col("source_id").isNull(),"new in target"))
df.show()


# COMMAND ----------

df = df.filter(col("comment").isNotNull())
df.show()

# COMMAND ----------

final_df = df.withColumn("id",coalesce(col("source_id"),col("target_id"))).select("id","comment")
final_df.show()
