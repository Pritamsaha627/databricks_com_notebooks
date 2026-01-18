# Databricks notebook source
'''
Find the top 3 high eraner employees in each of the department.
'''

# COMMAND ----------

data = [(1,'Joe',85000,1),(2,'Henry',80000,2),(3,'Sam',60000,2),(4,'Max',90000,1),(5,'Janet',69000,1),(6,'Randy',85000,1),(7,'Will',70000,1)]

schema = ['emp_id','name','salary','departmentId']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("df")
df.show()

# COMMAND ----------

dept_data = [(1,'IT'),(2,'Sales')]

dept_schema = ['departmentId','department']
dept_df = spark.createDataFrame(dept_data,dept_schema)
dept_df.createOrReplaceTempView("dept_df")
dept_df.show()

# COMMAND ----------

new_df = df.join(dept_df,'departmentId','left')
new_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, dense_rank, desc
from pyspark.sql.window import Window

# COMMAND ----------

w_df = Window.partitionBy('department').orderBy(desc('salary'))
final_df = new_df.withColumn('rnk',dense_rank().over(w_df)).filter(col('rnk') <= 3)
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (SELECT *, dense_rank() OVER (PARTITION BY a.departmentId ORDER BY a.salary DESC) AS rnk FROM df a
# MAGIC JOIN dept_df b ON a.departmentId = b.departmentId)
# MAGIC SELECT * FROM cte 
# MAGIC WHERE rnk <= 3
# MAGIC
