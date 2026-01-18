# Databricks notebook source
dbutils.fs.rm('FileStore/duplicate_data.csv',True)

# COMMAND ----------

dbutils.fs.put("FileStore/duplicate_data.csv","""Sl_no, Name, update_date
1,'Rahul',2023-01-14
2,'Ishan',2023-02-24
3,'Sanju',2023-01-26
1,'Virat',2023-01-15
2,'Subhman',2023-02-25
3,'Rohit',2023-01-27""")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
schema = StructType([StructField('Sl_no',IntegerType()),\
                        StructField('Name',StringType()),\
                        StructField('update_date',TimestampType())])
df = spark.read.csv("dbfs:/FileStore/duplicate_data.csv",header=True,schema=schema)
display(df)

# COMMAND ----------

display(df.distinct())

# COMMAND ----------

from pyspark.sql.functions import col
display(df.orderBy(col('update_date').desc()).dropDuplicates(['Sl_no']))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df = df.withColumn('rowid',row_number().over(Window.partitionBy('Sl_no').orderBy(col('update_date').desc())))
display(df)

# COMMAND ----------

display(df.filter(df.rowid == 1))

# COMMAND ----------

display(df.filter(df.rowid > 1))

# COMMAND ----------


