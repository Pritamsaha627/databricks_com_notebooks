# Databricks notebook source
# MAGIC %md
# MAGIC Find out the origin and destination of each customer. There can be more than one stop for the same customer journey.

# COMMAND ----------

data = [(1,'flight1','Delhi','Hydrabad'),(1,'flight2','Hydrabad','Kochi'),(1,'flight3','Kochi','Bangalore'),(2,'flight1','Mumbai','Ayodhya'),(2,'flight2','Ayodhya','Kolkata')]

schema = ['cust_id' ,'flight_id', 'origin','destination']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import row_number, when, col, min, max
from pyspark.sql.window import Window

win = Window.partitionBy('cust_id').orderBy('flight_id')

w_df = df.withColumn('row_num',row_number().over(win))
w_df.show()
        

# COMMAND ----------

filter_df = w_df.groupBy(col('cust_id')).agg(min(col('row_num')).alias('start'),
                                             max(col('row_num')).alias('end'))
display(filter_df)

# COMMAND ----------

final_df = w_df.join(filter_df,['cust_id'])
final_df.show()

# COMMAND ----------

final_df = final_df.groupBy(col('cust_id')).agg(max(when(col('row_num') == col('start'), col('origin'))).alias('origin'),
                                            min(when(col('row_num') == col('end'), col('destination'))).alias('destination'))
final_df.show()

# COMMAND ----------

from pyspark.sql.functions import first, last

final_df2 = df.groupBy(col('cust_id')).agg(first('origin').alias('origin'),last('destination').alias('destination'))
final_df2.show()
