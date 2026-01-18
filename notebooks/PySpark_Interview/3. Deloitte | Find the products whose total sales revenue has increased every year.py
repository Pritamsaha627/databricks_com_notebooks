# Databricks notebook source
""" Find the products whose total sales revenue has increased every year. Include product_id, product_name and category in the result. """

# COMMAND ----------

product_data = [(1,'Laptop','Electronics'),(2,'Jeans','Clothing'),(3,'Chairs','Home Appliances')]
product_schema = ['product_id','product_name','catagory']
product_df = spark.createDataFrame(product_data,product_schema)
display(product_df)

# COMMAND ----------

sales_data = [(1,2019,1000.00),(1,2020,1200.00),(1,2021,1100.00),(2,2019,500.00),(2,2020,600.00),(2,2021,900.00),(3,2019,300.00),(3,2020,450.00),(3,2021,400.00),]
sales_schema = ['product_id','year','total_sales_revenue']
sales_df = spark.createDataFrame(sales_data,sales_schema)
display(sales_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, lag

w_df = Window.partitionBy(sales_df.product_id).orderBy(sales_df.year)
new_df = sales_df.withColumn('previous_year_revenue',lag(sales_df.total_sales_revenue).over(w_df))
new_df = new_df.withColumn('diff',new_df.total_sales_revenue - new_df.previous_year_revenue)

display(new_df)

# COMMAND ----------

new_df1 = new_df.groupby(col('product_id')).agg(min(col('diff')).alias('min_diff')).filter(col('min_diff')>0)
display(new_df1)

# COMMAND ----------

df = product_df.join(new_df1,['product_id'],'inner').drop(col('min_diff'))
df.show()
