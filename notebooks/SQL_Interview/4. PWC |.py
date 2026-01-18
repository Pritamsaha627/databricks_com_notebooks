# Databricks notebook source
    # 1. Identify companies whose revenue consistently increases every year without any dips. 

	# This means that if a company's revenue increases for several consecutive years but then experiences a dip in revenue, that company should not be included in the.

	# 2.Result should not contain companies like 'Green Solutions' and 'Retail Giants' as they have dip in their revenue
	# 3. Create similar dataframe in spark environment and solve the problem.


# COMMAND ----------

data = [
    ('Tech Innovations', 2018, 5000000.00),
    ('Tech Innovations', 2019, 6000000.00),
    ('Tech Innovations', 2020, 6500000.00),
    ('Tech Innovations', 2021, 7000000.00),
    ('Tech Innovations', 2022, 7500000.00),
    ('Green Solutions', 2018, 3000000.00),
    ('Green Solutions', 2019, 3500000.00),
    ('Green Solutions', 2020, 4000000.00),
    ('Green Solutions', 2021, 4500000.00),
    ('Green Solutions', 2022, 4400000.00), 
    ('Finance Corp', 2018, 8000000.00),
    ('Finance Corp', 2019, 9000000.00),
    ('Finance Corp', 2020, 9500000.00),
    ('Finance Corp', 2021, 10000000.00),
    ('Finance Corp', 2022, 10500000.00),
    ('Health Services', 2018, 2000000.00),
    ('Health Services', 2019, 2500000.00),
    ('Health Services', 2020, 2600000.00),
    ('Health Services', 2021, 2700000.00),
    ('Health Services', 2022, 2800000.00),
    ('EduTech', 2018, 1500000.00),
    ('EduTech', 2019, 1600000.00),
    ('EduTech', 2020, 1700000.00),
    ('EduTech', 2021, 1800000.00),
    ('EduTech', 2022, 1900000.00),
    ('Retail Giants', 2018, 9000000.00),
    ('Retail Giants', 2019, 8500000.00), 
    ('Retail Giants', 2020, 9000000.00),
    ('Retail Giants', 2021, 9500000.00),
    ('Retail Giants', 2022, 10000000.00)
]

schema = ['company','year','revenue']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window


# COMMAND ----------

new_df = df.withColumn('pre_revenue',lag('revenue').over(Window.partitionBy('company').orderBy('year'))).filter((col('revenue')-col('pre_revenue'))<0)
dip_company_list = new_df.select('company').rdd.flatMap(lambda x: x).collect()
print(dip_company_list)
final_df = df.select('company').where(~col('company').isin(dip_company_list)).distinct()
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (SELECT *, lag(revenue) over(partition by company order by year) as pre_revenue FROM company_revenue),
# MAGIC cte1 AS (SELECT *,(CASE WHEN revenue < pre_revenue THEN 0 ELSE 1 END) AS rnk FROM cte),
# MAGIC cte2 AS (SELECT company FROM cte1
# MAGIC WHERE rnk = 0)
# MAGIC SELECT DISTINCT company 
# MAGIC FROM company_revenue
# MAGIC WHERE company NOT IN (SELECT * FROM cte2)
# MAGIC
