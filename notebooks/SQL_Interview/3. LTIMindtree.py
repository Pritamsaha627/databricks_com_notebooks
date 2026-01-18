# Databricks notebook source
data = [('Virat','IT',8000),('Rohit','IT',7500),('Pant','Finance',7000),('Rahul','Finance',6000),('Rinku','HR',4000),('Rinku','HR',4000),('Bumrah','Admin',7000),('Shami','Admin',6000)]

schema = ['name','dept','salary']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("tp")
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the highest salary employee details fro each dept
# MAGIC SELECT * FROM (SELECT *,dense_rank() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk FROM tp)
# MAGIC WHERE rnk = 1
# MAGIC
# MAGIC

# COMMAND ----------

# What is the output of bellow mentioned code snippet
%sql
SELECT * FROM tp 
WHERE 5=7 AND dept = 'IT'
ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the duplicate data from a table which doesn't have any primary key
# MAGIC WITH cte AS (
# MAGIC SELECT *, row_number() OVER (PARTITION BY salary,dept,name ORDER BY name) as rnk FROM tp)
# MAGIC DELETE FROM cte
# MAGIC       WHERE rnk >= 2
