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
# MAGIC -- Implement your solution here
# MAGIC WITH Expanded AS(
# MAGIC     SELECT name,price FROM products
# MAGIC     JOIN generate_series(1,quantity) AS qty ON true
# MAGIC ),
# MAGIC Ranked AS(
# MAGIC     SELECT *,ROW_NUMBER()OVER(ORDER BY price ASC) AS rn,SUM(price)OVER(ORDER BY price ASC ROWS UNBOUNDED PRECEDING) AS running_total FROM Expanded
# MAGIC     )
# MAGIC     SELECT COUNT(*) AS quantity FROM Ranked
# MAGIC     WHERE running_total <= 100
# MAGIC
