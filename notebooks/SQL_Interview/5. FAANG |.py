# Databricks notebook source
# Companies often perform salary analyses to ensure fair compensation practices.
# One useful analysis is to check if there are any employees earning more than their direct managers.

# As a HR Analyst, you're asked to identify all employees who earn more than their direct managers.
# The result should include the employee's ID and name. 

# COMMAND ----------

data = [
    (1,'Emma',3800,1,6),
    (2,'Daniel',2230,1,7),
    (3,'Olivia',7000,1,8),
    (4,'Noah',6800,2,9),
    (5,'Sophia',1750,1,11),
    (6,'Liam',13000,3,NULL),
    (7,'Ava',12500,3,NULL),
    (8,'William',6800,2,NULL)
]

schema = ['employee_id','name','salary','department_id','manager_id']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s.employee_id,s.name FROM demo.dbo.employee s
# MAGIC LEFT JOIN demo.dbo.employee t ON s.manager_id = t.employee_id
# MAGIC WHERE s.salary > t.salary
# MAGIC
