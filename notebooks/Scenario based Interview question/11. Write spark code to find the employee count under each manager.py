# Databricks notebook source
'''
Write spark code to find the employee count under each manager.
'''

# COMMAND ----------

data = [(4529,'Nancy','Young','4125'),(4238,'John','Simon','4329'),(4329,'Martina','Candreva','4125'),(4009,'Klaus','Koch','4329'),(4125,'Mafalda','Ranieri','Null'),(4500,'Jakub','Hrabal','4529'),(4118,'Moira','Areas','4952'),(4012,'Jon','Nilssen','4952'),(4952,'Sandra','Rajkovic','4529'),(4444,'Seamus','Quinn','4329')]

schema = ['emp_id','first_name','last_name','manager_id']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("tp")
df.show()

# COMMAND ----------

Output:
+------+----------+-----+
|emp_id|first_name|count|
+------+----------+-----+
|  4529|     Nancy|    2|
|  4329|   Martina|    3|
|  4125|   Mafalda|    2|
|  4952|    Sandra|    2|
+------+----------+-----+

# COMMAND ----------

from pyspark.sql.functions import count, col

# COMMAND ----------

df1 = df.groupBy('manager_id').agg(count('emp_id').alias('emp_count'))
df1.show()

# COMMAND ----------

final_df = df.join(df1,df.emp_id == df1.manager_id,'left').filter(col('emp_count').isNotNull()).select('emp_id','first_name','emp_count')
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.emp_id, a.first_name,count(*) FROM tp a
# MAGIC LEFT JOIN tp b ON a.emp_id == b.manager_id
# MAGIC WHERE b.emp_id is not Null
# MAGIC GROUP BY a.emp_id, a.first_name
