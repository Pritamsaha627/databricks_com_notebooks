# Databricks notebook source
'''
Find the popularity percentage for each user on Meta/Facebook. The populrity percentage is defined as the no of friends the user has divided by the total no of users in the platform multiply with 100. Output each user with popular percentage. Order records in ascending order by user id.
'''

# COMMAND ----------

data = [(1,5),(1,3),(1,6),(2,1),(2,6),(3,9),(4,1),(7,2),(8,3)]

schema = ['user1','user2']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, round

# COMMAND ----------

df1 = df.select(col('user2').alias('user1'),col('user1').alias('user2'))
union_df = df.union(df1)
union_df.show()

# COMMAND ----------

friends_df = union_df.groupBy('user1').agg(count('user1').alias('total_no_of_friends'))
friends_df.show()

# COMMAND ----------

user_count = union_df.select(countDistinct('user1')).collect()[0][0]
print(user_df)

# COMMAND ----------

final_df = friends_df.select(col('user1').alias('user'),round(col('total_no_of_friends')*100/user_count,2).alias('popular_percentage'))
final_df.show()
