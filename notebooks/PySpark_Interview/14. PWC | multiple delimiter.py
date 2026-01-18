# Databricks notebook source
dbutils.fs.put("FileStore/pwc_multiple_delimeter.csv","""Sl_no|Name|Quarterly_Sales
1|virat|20,30,40
2|Sachin|42,72,64
3|Sourav|95,84,77
4|Aswin|75,88,92""")

# COMMAND ----------

df =  spark.read.csv('dbfs:/FileStore/pwc_multiple_delimeter.csv',header=True,sep='|')
df.show()

# COMMAND ----------

from pyspark.sql.functions import split,col 

df_final = df.withColumn('1st_Q',split(col('Quarterly_Sales'),',')[0])\
            .withColumn('2nd_Q',split(col('Quarterly_Sales'),',')[1])\
            .withColumn('3rd_Q',split(col('Quarterly_Sales'),',')[2]).drop('Quarterly_Sales')
df_final.show()

# COMMAND ----------


