# Databricks notebook source
'''
Find the highest salaried employee from each department who is not manager. 
'''

# COMMAND ----------

data = [(1,'Virat','IT','Null',8000),(2,'Rohit','IT','Null',8000),(3,'Pant','Finance','1',7000),(4,'Rahul','Finance','2',6000),(5,'Sanju','HR','1',5000),(6,'Rinku','HR','3',4000),(7,'Bumrah','Admin','Null',7000),(8,'Shami','Admin','7',6000),(9,'Siraj','BPO','3',5000),(10,'Axar','BPO','1',5500)]

schema = ['emp_id','name','department','manager_id','salary']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("tp")
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (select x.emp_id from tp x
# MAGIC left join tp y 
# MAGIC on x.emp_id = y.manager_id
# MAGIC where y.manager_id is Null),
# MAGIC
# MAGIC cte1 AS (select *,DENSE_RANK() over(partition by department order by salary desc) as rnk from tp)
# MAGIC select b.emp_id,b.name,b.department,b.manager_id,b.salary from cte a 
# MAGIC left join cte1 b on a.emp_id = b.emp_id 
# MAGIC where rnk = 1
