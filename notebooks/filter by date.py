# Databricks notebook source
# dbutils.widgets.text('date','2009-04-05')

# COMMAND ----------

date = dbutils.widgets.get('date')
print(date)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/source_file/races.csv',header=True)
df.limit(20).display()

# COMMAND ----------

df = df.filter(col('date') == date)
df.show()

# COMMAND ----------

dbutils.notebook.exit('done')
