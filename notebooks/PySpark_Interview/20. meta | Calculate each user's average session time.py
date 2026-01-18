# Databricks notebook source
'''
Calculate each user's average session time. A session is defined as the time difference between a page_load and page_exit. For simplicity, assume a user has only 1 session per day and if there are multiple of the same events on that day, consider only the latest page_load and earliest page_exit, with an obvious restriction that load time event should happen before exit time event . Output the user_id and their average session time.
'''

# COMMAND ----------

data = [(0,'2019-04-25 13:30:15','page_load'),(0,'2019-04-25 13:30:18','page_load'),(0,'2019-04-25 13:30:40','scroll_down'),(0,'2019-04-25 13:30:45','scroll_up'),(0,'2019-04-25 13:31:40','page_exit'),(1,'2019-04-25 13:40:00','page_load'),(1,'2019-04-25 13:40:10','scroll_down'),(1,'2019-04-25 13:40:15','scroll_down'),(1,'2019-04-25 13:40:35','page_exit'),(2,'2019-04-25 13:41:21','page_load'),(2,'2019-04-25 13:41:30','scroll_down'),(2,'2019-04-25 13:41:35','scroll_down'),(2,'2019-04-25 13:41:40','scroll_up'),(1,'2019-04-26 11:15:00','page_load'),(1,'2019-04-26 11:15:10','scroll_down'),(1,'2019-04-26 11:15:20','scroll_down'),(1,'2019-04-26 11:15:25','scroll_up'),(1,'2019-04-26 11:15:35','page_exit'),(0,'2019-04-28 14:30:15','page_load'),(0,'2019-04-28 14:30:10','page_load'),(0,'2019-04-28 13:30:40','scroll_down'),(0,'2019-04-28 15:31:40','page_exit')]

schema = ['user_id','timestamp','action']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

import pyspark.sql.functions as F
df = df.withColumn("timestamp",F.to_timestamp("timestamp","yyyy-MM-dd HH:mm:ss"))
df.createOrReplaceTempView("tp")
df.show()

# COMMAND ----------

load_df = df.filter(F.col("action")=="page_load").select("user_id","timestamp").alias("load")
exit_df = df.filter(F.col("action")=="page_exit").select("user_id","timestamp").alias("exit")
new_df = load_df.join(exit_df,"user_id","left")
final_df = new_df.filter(F.col("load.timestamp") < F.col("exit.timestamp")).withColumn("date_load", F.to_date(F.col("load.timestamp")))
new_df.show()
final_df.show()

# COMMAND ----------

final_df = final_df.groupBy("user_id", "date_load").agg( F.max("load.timestamp").alias("timestamp_load"), F.min("exit.timestamp").alias("timestamp_exit")).withColumn("duration", F.unix_timestamp("timestamp_exit") - F.unix_timestamp("timestamp_load"))

final_df.show()

# COMMAND ----------

final_df = final_df.groupBy("user_id").agg(F.mean("duration").alias("duration"))
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT user_id,
# MAGIC     DATE(timestamp) AS d,
# MAGIC     MAX(CASE WHEN action = 'page_load' THEN timestamp ELSE NULL END) AS last_page_load,
# MAGIC     MIN(CASE WHEN action = 'page_exit' THEN timestamp ELSE NULL END) AS first_page_exit
# MAGIC     FROM tp
# MAGIC     GROUP BY user_id, d
# MAGIC ),
# MAGIC
# MAGIC cte2 AS (
# MAGIC SELECT user_id, 
# MAGIC   d, unix_timestamp(first_page_exit) - unix_timestamp(last_page_load) AS session_time
# MAGIC   FROM cte
# MAGIC )
# MAGIC
# MAGIC SELECT user_id, AVG(session_time) AS avg
# MAGIC FROM cte2
# MAGIC GROUP BY user_id
# MAGIC HAVING avg IS NOT NULL
# MAGIC
# MAGIC
