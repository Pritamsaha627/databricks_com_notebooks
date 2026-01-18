# Databricks notebook source
'''Compare the total number of comments made by users in each country between December 2019 and January 2020. For each month, rank countries by total comments using dense ranking (i.e., avoid gaps between ranks) in descending order. Then, return the names of the countries whose rank improved from December to January.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df1 = spark.read.csv('dbfs:/FileStore/fb_comments_count.csv',header = True)
df1 = df1.withColumn('created_at',to_date(col('created_at'),'dd-MM-yyyy'))
df1.display()

# COMMAND ----------

df2 = spark.read.csv('dbfs:/FileStore/fb_active_users.csv',header = True)
df2.display()

# COMMAND ----------

# Output :
  
+-------+
|country|
+-------+
|   Mali|
+-------+

# COMMAND ----------

df = df1.join(df2,['user_id'],'inner').filter(col('created_at').between('2019-12-01','2020-01-31')).withColumn('year_month',date_format('created_at','yyyyMM'))
display(df)

# COMMAND ----------

new_df = df.groupBy('year_month','country').agg(sum('number_of_comments').alias('total_comment')).withColumn('rnk',dense_rank().over(Window.partitionBy('year_month').orderBy(desc('total_comment'))))
new_df.display()

# COMMAND ----------

final_df = new_df.groupBy('country').pivot('year_month').agg(first('rnk')).where(col('201912')>col('202001')).select('country')
final_df.display()
