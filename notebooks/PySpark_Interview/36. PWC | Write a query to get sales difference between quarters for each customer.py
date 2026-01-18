# Databricks notebook source
'''
Write a query to get sales difference between quarters for each customer.
'''

# COMMAND ----------

from pyspark.sql.functions import col, lag, abs
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1,'Q1',22000),
        (2,'Q3',40000),
        (1,'Q4',30000),
        (1,'Q2',21000),
        (1,'Q3',15000),
        (2,'Q1',20000),
        (2,'Q2',30000),
        (2,'Q4',7800),
				]
		
schema = ['cust_id', 'quarter', 'sales']
	

df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('tp')
df.show()




# COMMAND ----------

df_new = df.withColumn('pre_sales',lag('sales').over(Window.partitionBy('cust_id').orderBy('quarter'))).na.fill(0)
df_new.show()

# COMMAND ----------

df_final = df_new.withColumn('diff',abs(col('sales') - col('pre_sales')))
df_final.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH cte AS (SELECT *, LAG(sales) OVER (PARTITION BY(cust_id) ORDER BY(quarter)) AS prev_sales FROM tp)
# MAGIC
# MAGIC SELECT *, ABS(sales - NVL(prev_sales,0)) AS diff FROM cte
