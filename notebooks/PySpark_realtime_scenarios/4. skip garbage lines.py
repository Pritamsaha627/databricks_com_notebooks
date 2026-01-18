# Databricks notebook source
dbutils.fs.put("/sample_data/garbage_row.csv","""l1
l2
l3
slno,name,roll,city
1,Nabin,38,Kolkata
2,Virat,35,Chennai
3,Rohit,37,Mumbai""")

# COMMAND ----------

rdd = sc.textFile("/sample_data/garbage_row.csv")
rdd = rdd.zipWithIndex().filter(lambda x : x[1]>2).map(lambda x:x[0].split(","))

# COMMAND ----------

header = rdd.collect()[0]

# COMMAND ----------

df = rdd.filter(lambda x:x!=header).toDF(header).show()
