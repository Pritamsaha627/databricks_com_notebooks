# Databricks notebook source
"""Get the name of the highest and lowest salary employee from each dept. If the salary is same then return the employee name in lexicographical order"""

# COMMAND ----------

data = [(1,'Virat',30000),(2,'Rohit',40000),(1,'Shami',50000),(1,'Bumrah',30000),(2,'Bhuvi',20000)]
schema = ['dept_id','emp_name','salary']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, count, when, max, min

w_df = Window.partitionBy(col('dept_id')).orderBy(col('salary'),col('emp_name'))
df1 = df.withColumn('row_num',row_number().over(w_df))
display(df1)

# COMMAND ----------

df1 = df1.withColumn('count',count('*').over(Window.partitionBy('dept_id')))
display(df1)

# COMMAND ----------

final_df = df1.groupBy('dept_id').agg(max(when(col('row_num') == 1, col('emp_name'))).alias('min_salary'),
                                     min(when(col('row_num') == col('count'), col('emp_name'))).alias('max_salary') )
final_df.show()
