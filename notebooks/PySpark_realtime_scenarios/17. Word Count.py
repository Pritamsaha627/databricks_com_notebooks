# Databricks notebook source
rdd = sc.textFile("/dbfs/sample_data/word_count.txt")

# COMMAND ----------

rdd_word = rdd.flatMap(lambda x : x.lower().split(" "))
rdd_word = rdd_word.filter(lambda x : x!='')
rdd_word = rdd_word.map(lambda x:(x,1))
rdd_count = rdd_word.reduceByKey(lambda x,y:x+y)
rdd_count = rdd_count.map(lambda x:(x[1],x[0])).sortByKey()
rdd_count.collect()

# COMMAND ----------

count_df = rdd_count.toDF(['word','word_count']).show()

# COMMAND ----------


