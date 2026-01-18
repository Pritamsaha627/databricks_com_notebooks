# Databricks notebook source
data = [("Iphone","BestBuy",100),("Samsung","Walmart",85),("Iphone","Walmart",90),("Samsung","BestBuy",90),("Iphone","Amazon",95),("Samsung","Amazon",80)]

schema = ['product' ,'store', 'price']

df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("tp")
df.show()

# COMMAND ----------

Output :
+-------+-------------+-------------+------------+-----------------+
|product|Walmart_price|BestBuy_price|Amazon_price|competitive_price|
+-------+-------------+-------------+------------+-----------------+
|Samsung|           85|           90|          80|                Y|
| Iphone|           90|          100|          95|                N|
+-------+-------------+-------------+------------+-----------------+

# COMMAND ----------

from pyspark.sql.functions import col, min, when

# COMMAND ----------

new_df = df.groupBy("product").pivot("store",["Walmart","BestBuy","Amazon"]).max("price")
new_df = new_df.selectExpr('product','Walmart as Walmart_price','BestBuy as BestBuy_price','Amazon as Amazon_price')
new_df.show()

# COMMAND ----------

min_df = df.groupBy("product").agg(min("price").alias("min_price"))
min_df.show()

# COMMAND ----------

final_df = new_df.join(min_df,["product"],"inner").withColumn("competitive_price", when(col("Amazon_price")==col("min_price"),"Y").otherwise("N")).drop("min_price").orderBy("product")
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_pivot as (
# MAGIC   select * from tp pivot(
# MAGIC     max(price) for store in ("Walmart" as Walmart_pice,"BestBuy" as BestBuy_price,"Amazon" as Amazon_price)
# MAGIC )),
# MAGIC cte_minprice as (select min(price) as min_price, product from tp group by product)
# MAGIC
# MAGIC select m1.*,
# MAGIC case when m1.Amazon_price = m2.min_price
# MAGIC then 'Y'
# MAGIC else 'N'
# MAGIC end as competitive_price
# MAGIC from cte_pivot m1 inner join cte_minprice m2
# MAGIC on m1.product = m2.product
# MAGIC order by m1.product
# MAGIC
