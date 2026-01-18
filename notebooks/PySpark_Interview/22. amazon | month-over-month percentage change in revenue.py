# Databricks notebook source
'''
Calculate the month-over-month percentage change in revenue. The output should include the year-month date(YYYY-MM) and percentage change, rounded to the two decimal places and sort from the begining of the year to the end of the year.
'''

# COMMAND ----------

data = [(1,'01-01-2019',172692,43),(2,'05-01-2019',177194,36),(3,'06-02-2019',116948,56),(4,'10-02-2019',162515,29),(5,'14-02-2019',120741,30),(6,'22-03-2019',151688,34),(7,'26-03-2019',1002327,44)]

schema = ['id','created_at','value','purchase_id']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, date_format, sum, lag, round
from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("created_at",to_date(col("created_at"),"dd-MM-yyyy"))
df.show()

# COMMAND ----------

new_df = df.withColumn('year_month',date_format("created_at","yyyy-MM"))
new_df.show()

# COMMAND ----------

new_df = new_df.groupBy("year_month").agg(sum("value").alias("revenue"))
new_df.show()

# COMMAND ----------

final_df = new_df.withColumn('pre_revenue',lag("revenue").over(Window.orderBy('year_month'))).withColumn("percentage",round(((col("revenue")-col("pre_revenue"))/col("pre_revenue")*100),2)).select("year_month","percentage")
final_df.show()

# COMMAND ----------


