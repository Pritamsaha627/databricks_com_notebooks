# Databricks notebook source
'''Find employees who are earning more than their managers. Output the employee's first name along with the corresponding salary.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

employee_Walmart = spark.read.csv('dbfs:/FileStore/employee_Walmart.csv',header = True)
employee_Walmart.display()


# COMMAND ----------

new_df = employee_Walmart.alias('t1').join(employee_Walmart.alias('t2'),col("t1.id") == col("t2.manager_id"),'left')
new_df.display()

# COMMAND ----------

final_df = new_df.filter(col("t1.salary") > col("t2.salary")).select('t1.first_name','t1.salary').distinct().orderBy(desc('t1.salary')).limit(1)
final_df.display()

# COMMAND ----------

# Output :
+----------+------+
|first_name|salary|
+----------+------+
|   Richerd|250000|
+----------+------+


# COMMAND ----------


