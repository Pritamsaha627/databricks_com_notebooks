# Databricks notebook source
'''Query to get cumulative percentage of amount using PYSPARK.
<=80, A
<=95, B
C'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [('A001','S05',200,'2024-09-22'),
        ('A002','S15',400,'2025-02-14'),
        ('A003','S14',300,'2025-01-09'),
        ('A002','S05',600,'2025-01-24'),
        ('A001','S12',600,'2025-02-16'),
        ('A001','S14',800,'2025-02-18'),
        ('A003','S16',900,'2025-02-26')
        ]
		
schema = ['factory_code', 'sku_code', 'amount','date']
	

df = spark.createDataFrame(data,schema)
df = df.withColumn('date',to_date('date','yyyy-MM-dd'))
df = df.select('date','factory_code', 'sku_code', 'amount')

df.show()


# COMMAND ----------

new_df = df.withColumn('factory_sku_code',concat(col('factory_code'),col('sku_code')))
new_df = new_df.withColumn('percentage_amount',col('amount')/sum('amount').over(Window.partitionBy('factory_code'))*100)
new_df = new_df.withColumn('cumulative_percentage',sum('percentage_amount').over(Window.partitionBy('factory_code').orderBy('amount')))
new_df = new_df.withColumn('grade',when(col('cumulative_percentage')<=80,'A').when(col('cumulative_percentage')<=95,'B').otherwise('C'))
new_df.show()
