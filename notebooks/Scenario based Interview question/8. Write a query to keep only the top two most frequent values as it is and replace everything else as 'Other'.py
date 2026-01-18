# Databricks notebook source
'''
Write a query to keep only the top two most frequent values of jobs as it is and replace everything else as 'Other'. 
'''

# COMMAND ----------

data = [
("John","Engineer"),
("Ron","Engineer"),
("Mary","Scientist"),
("Bob","Engineer"),
("Bean","Engineer"),
("Root","Scientist"),
("Sam","Doctor"),
]

schema = ["name", "job"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

Output:
+----+---------+
|name|      job|
+----+---------+
|John| Engineer|
| Ron| Engineer|
|Mary|Scientist|
| Bob| Engineer|
|Bean| Engineer|
|Root|Scientist|
| Sam|   Others|
+----+---------+


# COMMAND ----------

new_df = df.groupBy("job").count().orderBy("count",ascending = False).limit(2).rdd.flatMap(lambda x:x).collect()
print(new_df)

# COMMAND ----------

from pyspark.sql.functions import col, when
final_df = df.withColumn("job",when(col("job").isin(new_df),col("job")).otherwise("Others"))
final_df.show()
