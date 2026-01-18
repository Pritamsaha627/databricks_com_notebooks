# Databricks notebook source
'''
Find the price at start of the month and the difference in the price from previous month for each sku_id
'''

# COMMAND ----------

data = [(1,"2023-01-01",10),(1,"2023-01-27",12),(1,"2023-02-01",14),(1,"2023-02-15",15),(1,"2023-03-03",18),(1,"2023-03-27",15),(1,"2023-04-06",20),(2,"2023-01-01",50),(2,"2023-01-29",100),(2,"2023-02-01",150)]

schema = ['sku_id' ,'price_date', 'price']

df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("tp")
df.show()

# COMMAND ----------

from pyspark.sql.functions import col,to_date,day, date_format, add_months, row_number,desc,when

# COMMAND ----------

new_df = df.withColumn('price_date', to_date(col('price_date'),'yyyy-MM-dd'))
new_df.show()

# COMMAND ----------

from pyspark.sql.window import Window

w_df = Window.partitionBy('sku_id').orderBy('price_date')
rank_df = new_df.withColumn('row_num',row_number().over(w_df))

rank_df.show()

# COMMAND ----------

new_df = new_df.withColumn('price_month', when(day(col('price_date'))>1,add_months('price_date',1)).otherwise(col('price_date')))
new_df.show()

# COMMAND ----------

uni_df = new_df.unionAll(rank_df)
uni_df.show()
