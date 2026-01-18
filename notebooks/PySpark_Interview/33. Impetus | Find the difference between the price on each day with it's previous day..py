# Databricks notebook source
'''
Find the difference between the price on each day with it's previous day.
'''

# COMMAND ----------

from pyspark.sql.functions import col, to_date, count, lag, when, abs
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType

# COMMAND ----------

data = [(1,'Indigo',7000,'2023-01-01'),
        (1,'Indigo',7500,'2023-01-05'),
        (1,'Indigo',7100,'2023-02-10'),
        (1,'Indigo',8200,'2023-02-15'),
        (1,'Indigo',8500,'2023-03-04'),
        (2,'Vistara',8000,'2023-01-02'),
        (2,'Vistara',8500,'2023-01-06'),
        (2,'Vistara',8200,'2023-02-12'),
        (2,'Vistara',9200,'2023-02-18'),
        (2,'Vistara',9500,'2023-03-06'),
        ]

schema = ['flight_id','name','price','price_date']
df = spark.createDataFrame(data,schema)
df = df.withColumn('price_date',to_date('price_date','yyyy-MM-dd'))
df.show()

# COMMAND ----------

new_df = df.withColumn('prev_price',lag('price').over(Window.partitionBy('flight_id').orderBy('price_date')))
new_df.show()

# COMMAND ----------

final_df = new_df.withColumn('price_diff',when(col('prev_price').isNull(),col('price')).otherwise(abs(col('price') - col('prev_price')))).drop('prev_price')
final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write a query to count the number of occurance of 'Hello World' in the text.
# MAGIC 	This will be case sensitive; 
# MAGIC 	Hello and World should be counted when they are placed together.

# COMMAND ----------

# DBTITLE 1,emp
data1 = [(1,"Hi Hello Hello World hi hello dear Hello hi World Hello  Worldhi world HelloWorld")]
schema1 = ['id','value']
value_df = spark.createDataFrame(data1,schema1)
value_df.createOrReplaceTempView('tp')



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT value from tp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT(len(value) - len(REGEXP_REPLACE(value,"Hello World","")))/len("Hello World") as count_of_Hello_World FROM tp
# MAGIC
