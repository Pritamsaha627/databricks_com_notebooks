# Databricks notebook source
'''
Write a query that'll identify returning active users. A returning active user is a user that has made a second purchase with in 7 days of any other of their purchases. Output a list of user_ids of these returning active_users.
'''

# COMMAND ----------

data = [(100,'bread','06-03-2024',410),(100,'banana','14-03-2024',175),(100,'banana','29-03-2024',599),(101,'milk','27-03-2024',449),(101,'milk','26-03-2024',740),(114,'banana','29-03-2024',200),(114,'cookies','26-03-2024',300)]

schema = ['user_id' ,'item', 'created_at','cost']

df = spark.createDataFrame(data,schema)

df.show()

# COMMAND ----------

from pyspark.sql.functions import to_date, col

df = df.select("*",to_date(col("created_at"),"dd-MM-yyyy").alias("created_date")).drop("created_at")
df.createOrReplaceTempView('tp1')
df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, rank, current_date, datediff

w_df = Window.partitionBy("user_id").orderBy("created_date")

win_df = df.withColumn("last_created_date",lag('created_date').over(w_df)).withColumn("date_diff",datediff(col('created_date'),col('last_created_date')))
win_df.show()

# COMMAND ----------

final_df = win_df.select("user_id").filter(col("date_diff")<=7)
final_df.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with cte as (select user_id,created_date,
# MAGIC lag(created_date) over (partition by user_id order by created_date) as last_created_date
# MAGIC from tp1
# MAGIC ), cte1 as (select *,date_diff(created_date, last_created_date) as date_diff
# MAGIC from cte
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select distinct user_id
# MAGIC from cte1
# MAGIC where date_diff <= 7

# COMMAND ----------


