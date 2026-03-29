# Databricks notebook source
''' Remove k string from sales column and cast the data in int format and perform the pivot operation at the end concate the k string.
'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [('MUM','60k'),('HYD','40k'),('RAJ','70k')]
schema = ('city','sales')

df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

new_df = df.withColumn('sales',regexp_replace('sales','k','')).select(col('city'),col('sales').cast('int'))
new_df.display()

# COMMAND ----------

new_df = new_df.withColumn('dummy',lit(1))
new_df = new_df.groupBy('dummy').pivot("city",['MUM','HYD','RAJ']).agg(first('sales'))
new_df = new_df.drop('dummy')
new_df.display()


# COMMAND ----------

final_df = new_df.select([concat(col(i),lit('k')).alias(i) for i in new_df.columns])
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Problem

# COMMAND ----------

lis = [1,2,3,4,5] 
target_value = 5
# output : [(1,4),(3,2)]

from itertools import combinations

result = [(x,y) for x,y in combinations(lis,2) if x+y == target_value]
print(result)




# COMMAND ----------

lis = [1,2,3,4,5] 
target_value = 5

res = []

for i in range(len(lis)):
    for j in range(i+1,len(lis)):
        if lis[i]+lis[j] == target_value:
            res.append((lis[i],lis[j]))
print(res)

