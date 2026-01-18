# Databricks notebook source
"""Find out the head count of employee in each job"""

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/source_file/jobs.csv',header = True)
display(df)

# COMMAND ----------



# COMMAND ----------

df_rdd = df.rdd.collect()
print(df_rdd)

# COMMAND ----------

dict1 = {}
list1 = []

for i in df_rdd:
    list1.append(i[1])
# print(list1)

for i in list1:
    if i not in dict1:
        dict1[i] = 1
    else:
        dict1[i] = dict1[i] + 1
print(dict1) 

# COMMAND ----------

dict1_list = []
dict1_list.append(dict1)
dict1_df = spark.read.json(sc.parallelize(dict1_list))
dict1_df.show()

# COMMAND ----------

final_df = df.groupBy('job').count()
final_df.show()

# COMMAND ----------


