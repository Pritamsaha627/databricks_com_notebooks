# Databricks notebook source
from pyspark.sql.functions import *

data = [(101,'2025-02-12',100),
        (102,'2025-02-12',200),
        (103,'2025-02-12',300),
        (101,'2025-04-12',400),
        (102,'2025-04-12',500),
        (103,'2025-04-12',600),
        (101,'2025-06-12',700),
        (102,'2025-06-12',800),
        (103,'2025-06-12',900),
        (101,'2025-08-12',1000),
        (102,'2025-08-12',1100),
        (103,'2025-08-12',1200),
        (101,'2025-10-12',1300),
        (102,'2025-10-12',1400),
        (103,'2025-10-12',1500),
        (101,'2025-12-12',1600),
        (102,'2025-12-12',1700),
        (103,'2025-12-12',1800)]

schema = ["shop_id","dt","price"]

data1 = [(101,'Delhi'),(102,'Mumbai'),(103,'Chennai')]
schema1 = ["shop_id","city"]

df = spark.createDataFrame(data, schema)
df = df.withColumn('dt',df.dt.cast("date"))

df1 = spark.createDataFrame(data1, schema1)

df1.createOrReplaceTempView("shops")
df.createOrReplaceTempView("sales")

df.display()
df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT b.city, SUM(CASE WHEN EXTRACT(QUARTER FROM dt) == 1 THEN price ELSE 0 END) AS Q1_sales,
# MAGIC        SUM(CASE WHEN EXTRACT(QUARTER FROM dt) == 2 THEN price ELSE 0 END) AS Q2_sales,
# MAGIC        SUM(CASE WHEN EXTRACT(QUARTER FROM dt) == 3 THEN price ELSE 0 END) AS Q3_sales,
# MAGIC        SUM(CASE WHEN EXTRACT(QUARTER FROM dt) == 4 THEN price ELSE 0 END) AS Q4_sales
# MAGIC FROM sales a JOIN shops b ON a.shop_id = b.shop_id
# MAGIC GROUP BY b.city
# MAGIC
