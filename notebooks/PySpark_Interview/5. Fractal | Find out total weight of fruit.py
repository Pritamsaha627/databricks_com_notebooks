# Databricks notebook source
data = [('Rahul','Tomato',3),('Virat','Apple',2),('Rahul','Tomato',1),('Rahul','Apple',3),('Priya','Orange',6),('Virat','Apple',1),('Priya','Orange',2)]
schema = "name string, item string, weight int"

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------



# COMMAND ----------

final_df = df.groupBy("name","item").sum("weight")
final_df = final_df.withColumnRenamed('sum(weight)','total_weight')
final_df.show()

# COMMAND ----------

from pyspark.sql.functions import collect_set,struct

df_final = final_df.groupBy("name").agg(collect_set(struct("item","total_weight")).alias("items"))
df_final.show(truncate=False)

# COMMAND ----------


