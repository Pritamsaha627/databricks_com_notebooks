# Databricks notebook source
'''
Query to find out total match and win count for each team.
'''

# COMMAND ----------

data = [('IND','PAK','IND'),('IND','SL','IND'),('SL','PAK','SL')]

schema = ['team_A','team_B','win']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

Output :
+----+------+---+
|team|played|win|
+----+------+---+
| IND|     2|  2|
|  SL|     2|  1|
| PAK|     2|  0|
+----+------+---+

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

df1 = df.select(col("team_B").alias("team_A"),col("team_A").alias("team_B"),col("win"))
df1.show()

# COMMAND ----------

new_df = df.union(df1)
new_df.show()

# COMMAND ----------

final_df1 = new_df.groupBy("team_A").agg(count("*").alias("played"))
final_df1.show()

# COMMAND ----------

final_df2 = new_df.groupBy("win").agg(count("*")/2)
final_df2.show()

# COMMAND ----------

final_df = final_df1.join(final_df2,col("team_A")==col("win"),"left").select(col("team_A").alias("team"),col("played"),col("(count(1) / 2)").cast("int").alias("win")).na.fill(0)
final_df.show()

# COMMAND ----------


