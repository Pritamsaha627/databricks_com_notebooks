# Databricks notebook source
'''
Write a pyspark code to calculate the percentage difference between total sales of Q1 and Q2 of year 2010.
'''

# COMMAND ----------

data = [('2010-01-02',500),('2010-02-03',1000),('2010-03-04',1000),('2010-04-05',1000),('2010-05-06',1500),('2010-06-07',1000),('2010-07-08',1000),('2010-08-09',1000),('2010-10-10',1000)]

schema = ['date','sales']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import to_date, col, quarter, sum


# COMMAND ----------

new_df = df.withColumn('date',to_date('date','yyyy-MM-dd'))
new_df.createOrReplaceTempView('tp')
new_df.show()

# COMMAND ----------

new_df = new_df.withColumn('quarter',quarter('date'))
new_df.show()

# COMMAND ----------

new_df = new_df.groupBy('quarter').agg(sum('sales').alias('total_sales'))
new_df.show()

# COMMAND ----------

q1_sales = new_df.filter(col('quarter')==1).select('total_sales').collect()[0][0]
q2_sales = new_df.filter(col('quarter')==2).select('total_sales').collect()[0][0]

percentage_difference = ((q2_sales - q1_sales)/q1_sales) * 100
print(percentage_difference)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (SELECT *, quarter(date) as quarter from tp),
# MAGIC
# MAGIC cte1 AS (SELECT quarter, sum(sales) as sales FROM cte
# MAGIC GROUP BY quarter
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC  ABS(
# MAGIC  SUM(CASE WHEN quarter == 2 THEN sales ELSE 0 END) 
# MAGIC  - SUM(CASE WHEN quarter == 1 THEN sales ELSE 0 END)
# MAGIC  ) 
# MAGIC  / SUM(CASE WHEN quarter == 1 THEN sales ELSE 0 END) * 100 AS perentage_diff
# MAGIC FROM 
# MAGIC  cte1
# MAGIC
