# Databricks notebook source

"""
Write an pyspark code to find the ctr of each Ad.
Round ctr to 2 decimal points. 
Order the result table by ctr in descending order and by ad_id in ascending order in case of a tie.
[Ctr=Clicked/(Clicked+Viewed)]
"""

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType 
from pyspark.sql.functions import sum, when, col, round, rank
from pyspark.sql.window import Window

data = [
(1, 1, 'Clicked'),
(2, 2, 'Clicked'),
(3, 3, 'Viewed'),
(5, 5, 'Ignored'),
(1, 7, 'Ignored'),
(2, 7, 'Viewed'),
(3, 5, 'Clicked'),
(1, 4, 'Viewed'),
(1, 2, 'Clicked')
]

schema=StructType([
StructField('AD_ID',IntegerType(),True),
StructField('USER_ID',IntegerType(),True),
StructField('ACTION',StringType(),True)
])


df=spark.createDataFrame(data,schema) 
df.show()

# COMMAND ----------

ctr_df = (df.groupBy("ad_id") .agg(sum(when(df["action"] == "Clicked", 1).otherwise(0)).alias("click_count"),sum(when(df["action"] == "Viewed", 1).otherwise(0)).alias("view_count")) .withColumn("ctr", round(col("click_count") / (col("click_count") + col("view_count")), 2)) )
ctr_df.show()



# COMMAND ----------

window_spec = Window.orderBy(col("ctr").desc(), col("ad_id").asc()) 
result_df = ctr_df.withColumn("rank", rank().over(window_spec))
result_df.show()

# COMMAND ----------

result_df.select('ad_id','ctr').show()
