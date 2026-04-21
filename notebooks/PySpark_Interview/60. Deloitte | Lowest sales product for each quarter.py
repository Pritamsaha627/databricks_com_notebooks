# Databricks notebook source
'''
Get the non matching countries from both table.

Input :
+-------+
|country|
+-------+
|    IND|
|     US|
| CANADA|
+-------+

+-------+
|country|
+-------+
|    IND|
|   FRAN|
|    GER|
+-------+

Output :
+-------+
|country|
+-------+
|     US|
| CANADA|
|   FRAN|
|    GER|
+-------+


'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data1 = ['IND','US','CANADA']

data2 = ['IND','FRAN','GER']

schema = ['country']

df1 = spark.createDataFrame(data1, schema)
df2 = spark.createDataFrame(data2, schema)

df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

df1.show()
df2.show()

# COMMAND ----------

df_final = df1.subtract(df2).union(df2.subtract(df1))
df_final.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS(SELECT * FROM df1
# MAGIC EXCEPT
# MAGIC SELECT * FROM df2),
# MAGIC cte1 AS(SELECT * FROM df2
# MAGIC EXCEPT
# MAGIC SELECT * FROM df1)
# MAGIC SELECT * FROM cte
# MAGIC UNION ALL
# MAGIC SELECT * FROM cte1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Get the lowest sales product for each quarter 

# COMMAND ----------

# Sample data for product, price, and sales tables
product_data = [
    (1, "Bread", "Whole wheat bread"),
    (2, "Cake", "Chocolate cake"),
    (3, "Cookie", "Oatmeal cookie")
]
product_schema = ["prod_id", "prod_name", "description"]

price_data = [
    (1, 2.5, "w1,026"),
    (2, 5.0, "w1,026"),
    (3, 1.5, "w1,026")
]
price_schema = ["prod_id", "price", "work_week"]

sales_data = [
    (1, "2025-01-15", 120),
    (2, "2025-01-20", 60),
    (3, "2025-02-10", 180),
    (1, "2025-03-05", 90),
    (1, "2025-06-19", 100),
    (2, "2025-07-19", 50),
    (3, "2025-08-19", 200),
    (2, "2025-04-19", 50),
    (3, "2025-04-19", 200),
    (1, "2025-05-22", 110),
    (2, "2025-06-13", 70),
    (3, "2025-07-08", 160),
    (1, "2025-08-17", 130),
    (2, "2025-09-25", 80),
    (3, "2025-10-30", 210)
] 
sales_schema = ["prod_id", "sales_date", "sales_qnt"]

# Create DataFrames
product_df = spark.createDataFrame(product_data, product_schema)
price_df = spark.createDataFrame(price_data, price_schema)
sales_df = spark.createDataFrame(sales_data, sales_schema).withColumn("sales_date", to_date(col("sales_date"), "yyyy-MM-dd"))

display(product_df)
display(price_df)
display(sales_df)

# COMMAND ----------

new_sales_df = sales_df.join(price_df, "prod_id",'left')\
                        .withColumn("Quarter",quarter(col("sales_date")))\
                        .withColumn("total_sales",col("sales_qnt")*col("price"))
new_sales_df = new_sales_df.groupBy('prod_id','Quarter').agg(sum('total_sales').alias('total_sales'))

new_sales_df.display()

# COMMAND ----------

final_sales_df = new_sales_df.withColumn('rnk',dense_rank().over(Window.partitionBy('Quarter').orderBy('total_sales')))\
                              .filter(col('rnk') == 1)\
                              .drop('rnk')
final_sales_df = final_sales_df.join(product_df,["prod_id"])
final_sales_df.display()
