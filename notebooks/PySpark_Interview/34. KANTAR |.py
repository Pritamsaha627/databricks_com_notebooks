# Databricks notebook source
'''
Question: Suppose you have a PySpark DataFrame containing customer data with columns id, name, purchase_amount, and purchase_date. Write a PySpark script to filter customers who made purchases over $1000 in the last year and display their id and total_purchase_amount.
'''

# COMMAND ----------

from pyspark.sql.functions import col, to_date, year,current_date,sum
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1, "Alice", 1500, "2023-05-15"),
		(2, "Bob", 500, "2023-02-20"),
		(2, "Bob", 700, "2023-04-22"),
		(3, "Charlie", 1200, "2022-12-10"),
  	(4, "Donald", 1200, "2024-12-10"),
    (5, "Tom", 800, "2023-12-08")]
		
schema = ['id', 'name', 'purchase_amount', 'purchase_date']
	

df = spark.createDataFrame(data,schema)
df = df.withColumn('purchase_date',to_date('purchase_date','yyyy-MM-dd'))
df.createOrReplaceTempView('tp')
df.show()




# COMMAND ----------

new_df = df.filter(year('purchase_date') == year(current_date())-1)
new_df.show()

# COMMAND ----------

final_df = new_df.groupBy('id').agg(sum('purchase_amount').alias('total_purchase_amount')).filter(col('total_purchase_amount') > 1000)
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (SELECT id,sum(purchase_amount) AS total_purchase_amount FROM tp
# MAGIC WHERE year(purchase_date) == year(current_date())-1
# MAGIC GROUP BY id)
# MAGIC SELECT * FROM cte
# MAGIC WHERE total_purchase_amount > 1000
# MAGIC
