# Databricks notebook source
# MAGIC %fs 
# MAGIC head dbfs:/FileStore/source_file/emp_nullvalues.csv

# COMMAND ----------

df = spark.read.option("nullValue","null").csv('dbfs:/FileStore/source_file/emp_nullvalues.csv',header=True,inferSchema=True)
display(df)

# COMMAND ----------

df.filter(df.EMPNO.isNull()).count()

# COMMAND ----------

from pyspark.sql.functions import count,col,when
df_final = df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
display(df_final)

# COMMAND ----------

df.columns

# COMMAND ----------


