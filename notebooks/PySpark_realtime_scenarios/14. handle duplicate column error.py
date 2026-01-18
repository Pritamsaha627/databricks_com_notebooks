# Databricks notebook source
data1 = [(67,'Virat',1),(25,'Rohit',1),(56,'Shami',2),(66,'Bumrah',2)]
schema1 = ['player_id','name','dept_id']
df1 = spark.createDataFrame(data1,schema1)
display(df1)

# COMMAND ----------

data2 = [(1,'Batting'),(2,'Bowling')]
schema2 = ['dept_id','dept_name']
df2 = spark.createDataFrame(data2,schema2)
display(df2)

# COMMAND ----------

df = df1.join(df2,['dept_id'],'inner')
df.show()

# COMMAND ----------

df.write.format('delta').saveAsTable('player_detail')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM player_detail

# COMMAND ----------


