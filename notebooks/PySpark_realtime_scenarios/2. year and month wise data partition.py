# Databricks notebook source
df = spark.read.csv('dbfs:/FileStore/source_file/empDetail_new.csv',header=True,inferSchema=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import to_date, month, year

df = df.withColumn('HIREDATE', to_date('HIREDATE','dd-MM-yyyy')).fillna({"HIREDATE":"1998-09-24"})
display(df)

# COMMAND ----------

df = df.withColumn('MONTH',month('HIREDATE')).withColumn('YEAR',year('HIREDATE'))
display(df)

# COMMAND ----------

df.write.format("delta").partitionBy("YEAR","MONTH").mode("overwrite").saveAsTable("emp_data")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/emp_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM emp_data WHERE YEAR = 1987
