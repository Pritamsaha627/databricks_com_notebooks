# Databricks notebook source
'''Query for most expensive model both in SQL and PYSPARK.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

vehicle_data = [('Pulsar','Rear View Mirror',100,2),
        ('Discover','Rear View Mirror',50,2),
        ('Pulsar','Headlight',500,1),
        ('Discover','Headlight',200,1),
				]
		
vehicle_schema = ['Bike_Model', 'Spare_Part', 'Per_Unit_Spare_Part_cost','Quantity']
	

vehicle_df = spark.createDataFrame(vehicle_data,vehicle_schema)
vehicle_df.createOrReplaceTempView('tp')

vehicle_df.show()




# COMMAND ----------

final_vehicle_df = vehicle_df.withColumn('parts_total_cost',col('Per_Unit_Spare_Part_cost') * col('Quantity'))
final_vehicle_df = final_vehicle_df.groupBy('Bike_Model').agg(sum('parts_total_cost').alias('total_cost'))
final_vehicle_df = final_vehicle_df.withColumn('rnk',dense_rank().over(Window.orderBy(desc('total_cost')))).filter(col('rnk') == 1).select('Bike_Model')
final_vehicle_df.show()

# COMMAND ----------

'''Write a query to get the sales person who did the max sales For each month in PYSPARK.'''

# COMMAND ----------

emp_data = [(100,1000,'Jan'),(110,200,'Jan'),(100,3500,'Feb'),(100,350,'Feb'),(100,400,'Feb')]

emp_schema = ['EMPID', 'Sales', 'Month']

emp_df = spark.createDataFrame(emp_data,emp_schema)

emp_df.show()

# COMMAND ----------

final_emp_df = emp_df.withColumn('rnk',dense_rank().over(Window.partitionBy('Month').orderBy(desc('Sales')))).filter(col('rnk') == 1)
final_emp_df.show()

# COMMAND ----------

'''Write a query to separate number and letter.'''

# COMMAND ----------

data = [('1',),('2',),('3',),('A',),('B',)]
schema = ['data']
df = spark.createDataFrame(data,schema)

df.show()

# COMMAND ----------

final_df = df.withColumn('number_data',when(col('data').isin('1','2','3','4','5','6','7','8','9','0'),col('data'))).withColumn('letter_data',when(~col('data').isin('1','2','3','4','5','6','7','8','9','0'),col('data')))
final_df.show()

# COMMAND ----------

# from pyspark.sql.functions import udf, regexp_replace
# from pyspark.sql.types import StringType

# def split_number_letter(input_string):
#     if input_string is None:
#         return None
#     numbers = ''.join(filter(str.isdigit, input_string))
#     letters = ''.join(filter(str.isalpha, input_string))
#     return numbers, letters

# split_udf = udf(split_number_letter, StringType())

# df = df.withColumn("number", split_udf(df["data"]).getItem(0))\
#        .withColumn("letter", split_udf(df["data"]).getItem(1))

# df.show()
