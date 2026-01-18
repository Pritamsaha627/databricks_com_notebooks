# Databricks notebook source
'''
Write a query to find out all employees who have continuously absent in last 2 or more days.
'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1,'A','2024-04-25'),
        (2,'A','2024-04-25'),
        (3,'P','2024-04-25'),
        (1,'P','2024-04-26'),
        (2,'A','2024-04-26'),
        (3,'P','2024-04-26'),
        (1,'A','2024-04-27'),
        (2,'A','2024-04-27'),
        (3,'P','2024-04-27')
				]
		
schema = ['emp_id', 'attendance', 'date']
	

df = spark.createDataFrame(data,schema)
df = df.withColumn('date',to_date(col('date'),'yyyy-MM-dd'))
# df.createOrReplaceTempView('tp')
df.show()




# COMMAND ----------

df = df.withColumn('longDate', unix_timestamp(df['date']))
absent_df = df.filter(col('attendance') == 'A')
absent_df.show()

# COMMAND ----------

window_spec = Window.partitionBy(col('emp_id')).orderBy(col('date'))
absent_df = absent_df.withColumn('prev_date', lag('date', 1).over(window_spec))
absent_df = absent_df.withColumn('longPrevdate', unix_timestamp(absent_df['prev_date']))
absent_df.show()

# COMMAND ----------

absent_df = absent_df.withColumn('consecutive',(col('longDate') - col('longPrevdate') == 86400) | (col('prev_date').isNull()))
absent_df.show()

# COMMAND ----------

absent_df = absent_df.withColumn('streak_id', sum(when(~col('consecutive'), 1).otherwise(0)).over(window_spec))
absent_df.show()

# COMMAND ----------

streaks_df = absent_df.groupBy('emp_id', 'streak_id').count()
streaks_df.show()

# COMMAND ----------

long_streaks_df = streaks_df.filter(col('count') >= 2)
long_streaks_df.show()
