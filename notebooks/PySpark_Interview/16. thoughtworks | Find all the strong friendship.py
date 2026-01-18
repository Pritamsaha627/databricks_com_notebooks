# Databricks notebook source
'''
Each row of this table indicates that the users user1_id and user2_id are friends. Note that user1_id < user2_id.
A friendship between a pair of friends x and y is strong if x and y have at least three common friends.
Write a query to find all the strong friendship.
Note that the result table should not contain duplicates with user1_id < user2_id(To have the friendship user1_id<user2_id)
'''

# COMMAND ----------

data = [('1','2'),('1','3'),('2','3'),('1','4'),('2','4'),('1','5'),('2','5'),('1','7'),('3','7'),('1','6'),('3','6'),('2','6')]
schema = ['user1_id','user2_id']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col


df1 = df.select(col("user2_id").alias("user1_id"),col("user1_id").alias("user2_id"))
df1.show()

# COMMAND ----------

df_union = df.union(df1)
display(df_union)

# COMMAND ----------

df_union1 = df_union.select(col('user1_id').alias('user1'),col('user2_id').alias('user2'))
df_final = df_union.join(df_union1).filter((df_union.user1_id<df_union1.user1) & (df_union.user2_id == df_union1.user2))
display(df_final)

# COMMAND ----------

df_final = df_final.groupBy('user1_id','user1').count()
display(df_final)

# COMMAND ----------

df_final = df_final.filter(col("count") > 2)
df_final.show()
