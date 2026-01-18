# Databricks notebook source
dbutils.fs.put("/sample_data/dynamic_data.csv","""1,Nabin
2,Virat,35
4,Rohit,37,Mumbai""")

# COMMAND ----------

df = spark.read.text("/sample_data/dynamic_data.csv")
display(df)

# COMMAND ----------

from pyspark.sql.functions import split,size
df = df.withColumn("split_col",split("value",",")).drop("value")
df = df.withColumn("size_col",size('split_col'))
display(df)

# COMMAND ----------

from pyspark.sql.functions import max
for i in range (df.select(max(size('split_col'))).collect()[0][0]):
    df = df.withColumn("col"+str(i),df['split_col'][i])
df = df.drop('split_col','size_col')
display(df)

# COMMAND ----------

df1= df.withColumnRenamed('col0','slno').withColumnRenamed('col1','name').withColumnRenamed('col2','age').withColumnRenamed('col3','city')
display(df1)
