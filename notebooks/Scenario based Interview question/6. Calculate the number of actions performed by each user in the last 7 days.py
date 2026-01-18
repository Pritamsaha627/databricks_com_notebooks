# Databricks notebook source
'''
Calculate the number of actions performed by each user in the last 7 days.
'''

# COMMAND ----------

data = [(1,101,'login','2023-09-05 08:30:00'),(9,101,'login','2024-03-16 08:30:00'),(10,101,'login','2024-03-14 08:30:00'),(2,102,'click','2023-09-06 12:45:00'),(3,101,'click','2024-03-10 14:15:00'),(4,103,'login','2023-09-08 09:00:00'),(5,102,'logout','2024-03-10 17:30:00'),(11,102,'login','2024-02-21 08:30:00'),(6,101,'click','2024-03-14 11:20:00'),(7,103,'click','2024-03-15 10:15:00'),(13,103,'click','2024-03-10 10:15:00'),(12,102,'login','2024-03-12 08:30:00'),(8,102,'click','2023-03-13 13:10:00'),]

schema = ['log_id','user_id', 'action', 'timestamp']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import datediff,current_date

df = df.select("*").filter(datediff(current_date(),"timestamp")<=7)
df.show()

# COMMAND ----------

df = df.groupBy('user_id').count()
df.show()
