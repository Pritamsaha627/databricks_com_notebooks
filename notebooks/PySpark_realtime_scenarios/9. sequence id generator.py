# Databricks notebook source
df = spark.read.option("nullValue","null").csv('dbfs:/FileStore/source_file/races.csv',inferSchema=True,header=True).dropna(how="all").dropDuplicates(['raceId'])
display(df)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
df = df.withColumn("id",monotonically_increasing_id()+1)
display(df)

# COMMAND ----------

from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
df = df.withColumn('row_num',row_number().over(Window.partitionBy(lit('')).orderBy(lit(''))))
display(df)

# COMMAND ----------

from pyspark.sql.functions import crc32, col
df = df.withColumn('crc32',crc32(col("raceId").cast("string")))
display(df)

# COMMAND ----------

from pyspark.sql.functions import md5
df = df.withColumn('md5',md5(col("raceId").cast("string")))
display(df)

# COMMAND ----------

from pyspark.sql.functions import sha2
df = df.withColumn('sha2_value',sha2(col("raceId").cast("string"),512))
display(df)

# COMMAND ----------


