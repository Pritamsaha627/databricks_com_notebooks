# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Given an employee dataset with two column employee_id and team_id. Compute the team size for each employee.
# MAGIC

# COMMAND ----------

"""
Given an employee dataset with two column employee_id and team_id. Compute the team size for each employee.
"""

# COMMAND ----------

data = [(1,8),(2,8),(3,8),(4,7),(5,9),(6,9)]
schema = ['emp_id','team_id']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df1 = df.groupBy('team_id').agg(count('*').alias('team_size'))
df1.show()

# COMMAND ----------

final_df = df.join(df1,['team_id']).drop('team_id')
final_df.show()

# COMMAND ----------


