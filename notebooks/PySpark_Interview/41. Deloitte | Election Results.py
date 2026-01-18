# Databricks notebook source
'''The election is conducted in a city and everyone can vote for one or more candidates, or choose not to vote at all. Each person has 1 vote so if they vote for multiple candidates, their vote gets equally split across these candidates. For example, if a person votes for 2 candidates, these candidates receive an equivalent of 0.5 vote each. Some voters have chosen not to vote, which explains the blank entries in the dataset.


Find out who got the most votes and won the election. Output the name of the candidate or multiple names in case of a tie.
To avoid issues with a floating-point error you can round the number of votes received by a candidate to 3 decimal places.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/vote_poll.csv',header = True)

df.show()


# COMMAND ----------

# Output : 

# COMMAND ----------

new_df = df.na.drop()
new_df.show()

# COMMAND ----------

new_df = new_df.withColumn('vote',round(1/count('voter').over(Window.partitionBy('voter').orderBy('voter')),3))
new_df.show()

# COMMAND ----------

final_df = new_df.groupBy('candidate').agg(sum('vote').alias('total_vote')).orderBy(desc('total_vote')).select('candidate').limit(1)
final_df.show()
