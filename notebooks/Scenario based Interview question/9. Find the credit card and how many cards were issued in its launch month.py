# Databricks notebook source
'''
Write a query that outputs the name of credit card and how many cards were issued  in its launch month. The launch month is the earliest record in the monthly_cards_issued table for a given card. Order the results starting from the biggest issued amount.
'''

# COMMAND ----------

data = [(1,2021,'Chase Sapphire Reserve',170000),(2,2021,'Chase Sapphire Reserve',175000),(3,2021,'Chase Sapphire Reserve',180000),(3,2021,'Chase Freedom Flex',65000),(4,2021,'Chase Freedom Flex',70000)]

schema = ['issue_month','issue_year','card_name','issue_amount']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('tp')
df.show(truncate = False)

# COMMAND ----------

Output :
+--------------------+------------+
|           card_name|issue_amount|
+--------------------+------------+
|Chase Sapphire Re...|      170000|
|  Chase Freedom Flex|       65000|
+--------------------+------------+

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

final_df = df.withColumn('rnk',row_number().over(Window.partitionBy('card_name').orderBy('issue_month'))).where(col('rnk') == 1).select('card_name','issue_amount').orderBy('issue_amount',ascending = False)
final_df.show(truncate = False)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC   SELECT *, row_number() OVER (PARTITION BY card_name ORDER BY issue_month) as rnk FROM tp
# MAGIC )
# MAGIC SELECT card_name, issue_amount FROM cte
# MAGIC WHERE rnk == 1
# MAGIC ORDER BY issue_amount DESC
