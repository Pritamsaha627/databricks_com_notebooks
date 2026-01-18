# Databricks notebook source

'''Write a pyspark query to find the zero value marks and replace it with the average marks value of that subject'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [('X','Math',80),('X','English',50),('Y','Math',0),('Y','English',60),('Z','Math',60)]
schema = ['name','subject','marks']
df = spark.createDataFrame(data,schema)
df.display()
df.createOrReplaceTempView("tp")

# COMMAND ----------

# Output :
  
+-------+----+-----+
|subject|name|marks|
+-------+----+-----+
|   Math|   X| 80.0|
|English|   X| 50.0|
|   Math|   Y| 70.0|
|English|   Y| 60.0|
|   Math|   Z| 60.0|
+-------+----+-----+


# COMMAND ----------

avg_df = df.filter(col("marks") != 0).groupBy("subject").agg(avg("marks").alias("avg_marks"))
avg_df.display()


# COMMAND ----------

join_df = df.join(avg_df,['subject'],'left')
join_df.display()

# COMMAND ----------

final_df = join_df.withColumn("marks", when(col("marks") == 0, col("avg_marks")).otherwise(col("marks"))).drop("avg_marks")
final_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select subject,avg(marks) as avg_marks from tp
# MAGIC where marks != 0
# MAGIC group by subject)
# MAGIC
# MAGIC select tp.name,tp.subject,(case when tp.marks = 0 then cte.avg_marks else tp.marks end) as marks from tp left join cte on tp.subject = cte.subject
