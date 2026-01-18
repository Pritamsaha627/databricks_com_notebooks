# Databricks notebook source
'''
Find the top employees from each shop who have made the most amount of sales in the 3rd quarter of 2024.
'''

# COMMAND ----------

data = [('Rahul','Electronics',1,'2024-07-05',5000),('Priya','Clothing',1,'2024-07-15',7000),('Amit','Electronics',1,'2024-08-10',3000),('Sunita','Clothing',1,'2024-08-20',10000),('Anjali','Electronics',2,'2024-09-01',2000),('Ravi','Clothing',2,'2024-09-10',8000),('Neha','Electronics',2,'2024-07-25',9000),('Vijay','Clothing',2,'2024-08-30',12000),('Raj','Electronics',3,'2024-07-22',11000),('Puja','Clothing',3,'2024-09-05',6000)]

schema = ['employee','department','shopid','date','sale_value']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

Output :
+--------+------+----------+
|employee|shopid|sale_value|
+--------+------+----------+
|  Sunita|     1|     10000|
|   Vijay|     2|     12000|
|     Raj|     3|     11000|
+--------+------+----------+

# COMMAND ----------

from pyspark.sql.functions import col, to_date, dense_rank
from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn('date',to_date('date','yyyy-MM-dd'))
df.show()

# COMMAND ----------

w_df = Window.partitionBy('shopid').orderBy(col('sale_value').desc())
new_df = df.withColumn('rnk',dense_rank().over(w_df))
new_df.show()

# COMMAND ----------

final_df = new_df.filter((col('rnk') == 1) & (col('date').between('2024-07-01','2024-09-30'))).select('employee','shopid','sale_value')
final_df.show()

# COMMAND ----------


