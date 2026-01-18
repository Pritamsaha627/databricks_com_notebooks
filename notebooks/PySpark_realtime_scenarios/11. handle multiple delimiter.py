# Databricks notebook source
dbutils.fs.rm("FileStore/multiple_delimeter.csv",True)

# COMMAND ----------

dbutils.fs.put("FileStore/multiple_delimeter.csv","""Sl_no|Name|Quarterly_Sales
1|Rahul|30;50;70;75
2|Ishan|40;25;80;90
3|Sanju|90;50;60;95""")

# COMMAND ----------

# MAGIC %fs
# MAGIC head FileStore/multiple_delimeter.csv

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/multiple_delimeter.csv',header=True,sep="|")
display(df)

# COMMAND ----------

from pyspark.sql.functions import split
df = df.withColumn("Quarterly_Sales_split",split(df.Quarterly_Sales,";"))
display(df)

# COMMAND ----------

df1 = df.withColumn("Q1",col("Quarterly_Sales_split")[0])\
        .withColumn("Q2",col("Quarterly_Sales_split")[1])\
        .withColumn("Q3",col("Quarterly_Sales_split")[2])\
        .withColumn("Q4",col("Quarterly_Sales_split")[3]).drop(df.Quarterly_Sales_split)
display(df1)

# COMMAND ----------


