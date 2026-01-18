# Databricks notebook source
'''
Write a query to report all the employees with their primary department. For employees who belong to one department, report their only department.

Employees can belong to multiple departments. When the employee joins other departments, they need to decide which department is their primary dept.
Note that when an employee belongs to only one dept, their primary_flag is 'N'
'''

# COMMAND ----------

data = [(1,1,'N'),(2,1,'Y'),(2,2,'N'),(3,3,'N'),(4,2,'N'),(4,3,'Y'),(4,4,'N')]

schema = ['emp_id','dept_id','primary_flag']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("tp")
df.show()

# COMMAND ----------

Output :
+------+-------+
|emp_id|dept_id|
+------+-------+
|     1|      1|
|     2|      1|
|     3|      3|
|     4|      3|
+------+-------+

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

df1 = df.groupBy('emp_id').agg(count('dept_id').alias('dept_count'))
df1.show()

# COMMAND ----------

final_df = df.join(df1,'emp_id','left').filter((col('dept_count') == 1) | ((col('primary_flag') == 'Y') & (col('dept_count') > 1))).select('emp_id','dept_id')
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (SELECT *, rank() OVER (partition by emp_id order by dept_id) as rank FROM tp),
# MAGIC cte1 AS (SELECT *, max(rank) OVER (partition by emp_id) as max_rank FROM cte)
# MAGIC select emp_id,dept_id from cte1
# MAGIC WHERE max_rank = 1 OR max_rank >1 AND primary_flag = 'Y'
