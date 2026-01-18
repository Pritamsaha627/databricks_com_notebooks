# Databricks notebook source
'''Provided a table with user id and the dates they visited the platform, find the top 3 users with the longest continuous streak of visiting the platform as of August 10, 2022. Output the user ID and the length of the streak.

 
In case of a tie, display all users with the top three longest streaks.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/LinkedIn_user_streakes.csv',header = True)
df = df.withColumn('date_visited',to_date(col('date_visited'),'dd-MM-yyyy'))
df.display()

# COMMAND ----------

# output :

# COMMAND ----------

new_df = df.distinct().filter(col('date_visited') <= lit('2022-08-10')).withColumn('row_number',row_number().over(Window.partitionBy('user_id').orderBy('date_visited'))).withColumn('datesub',date_sub('date_visited','row_number'))
new_df.display()

# COMMAND ----------

final_df = new_df.groupBy('user_id','datesub').agg(count('datesub').alias('streak_length')).withColumn('rnk',dense_rank().over(Window.orderBy(desc('streak_length')))).filter('rnk <= 3').select('user_id','streak_length')
final_df.display()
