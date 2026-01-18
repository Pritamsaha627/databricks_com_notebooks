# Databricks notebook source
'''
Write a query to find out third highest salary.
'''

# COMMAND ----------

data = [(1,'Virat','70000'),(2,'Rohit','80000'),(1,'Virat','70000'),(3,'Rahul','90000'),(4,'Gill','95000'),(5,'Sanju','98000')]

schema = ['id','name','salary']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('tp')
df.show(truncate = False)

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select *, row_number() over (partition by id order by salary) as row_num from tp),
# MAGIC cte1 as (select *, dense_rank() over(order by salary desc) as rnk from cte
# MAGIC where row_num = 1)
# MAGIC select id,name,salary from cte1
# MAGIC where rnk = 3
