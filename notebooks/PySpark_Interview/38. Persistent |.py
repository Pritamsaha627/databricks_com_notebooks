# Databricks notebook source
'''
i. Convert the salaries value in rupee.
ii. Create new column as 'emp_level' and mark the employees among junior(<=3),mid_level(<7),senior(>=7)
'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1,'Pritam',200,2),
        (2,'Siva',300,3),
        (3,'Mani',100,5),
        (4,'Fred',600,8),
				]
		
schema = ['emp_id', 'name', 'sal_usd','experience']
	

df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('tp')
df.show()




# COMMAND ----------

Output :
  
+------+------+----------+-------+---------+
|emp_id|  name|experience|sal_ind|    level|
+------+------+----------+-------+---------+
|     1|Pritam|         2|  16400|   Junior|
|     2|  Siva|         3|  24600|   Junior|
|     3|  Mani|         5|   8200|Mid_level|
|     4|  Fred|         8|  49200|   Senior|
+------+------+----------+-------+---------+

# COMMAND ----------

new_df = df.withColumn('sal_rupee',col('sal_usd')*87.26).drop('sal_usd')
new_df.show()

# COMMAND ----------

final_df = new_df.withColumn('level',when(col('experience')<=3,'Junior').when(col('experience')<7,'Mid_level').otherwise('Senior'))
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (SELECT *,(sal_usd * 87.26) AS sal_rupee FROM tp)
# MAGIC SELECT *,
# MAGIC CASE
# MAGIC WHEN experience <= 3 THEN 'Junior'
# MAGIC WHEN experience <7 THEN 'Mid_level'
# MAGIC ELSE 'Senior'
# MAGIC END
# MAGIC AS level FROM cte

# COMMAND ----------


