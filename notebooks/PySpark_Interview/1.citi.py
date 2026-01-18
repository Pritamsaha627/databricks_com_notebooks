# Databricks notebook source
df = spark.read.csv('dbfs:/FileStore/source_file/jobs.csv',header = True)
# df = df.dropDuplicates(['job'])
display(df)

# COMMAND ----------

df_rdd = df.rdd.collect()
print(df_rdd)

# COMMAND ----------

d = {}
job_list = []
for i in df_rdd:
    job_list.append(i[1])
for i in job_list:
    if i not in d:
        d[i] = 1
    else:
        d[i] = d[i] + 1
print(d)

# COMMAND ----------

df = d.toDF(schema=['ob_description','count'])
df.show()

# COMMAND ----------

help(pyspark.RDD.reduceByKey)

# COMMAND ----------

from pyspark.context import sparkContext

data = (("Project","A", 1),
    ("Gutenberg’s", "X",3),
    ("Alice’s", "C",5),
    ("Adventures","B", 1)
)
rdd=spark.sparkContext.parallelize(data)
rdd.foreach(println)
# rdd2=rdd.map(f=>{(f._2, (f._1,f._2,f._3))})
# rdd2.foreach(println)
# rdd3= rdd2.sortByKey()
# rdd2.foreach(println)

# COMMAND ----------


