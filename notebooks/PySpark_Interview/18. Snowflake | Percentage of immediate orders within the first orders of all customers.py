# Databricks notebook source
'''
Write a solution to find the percentage of immediate orders within the first orders of all customers,rounded to 2 decimal places.
Note: Immediate delivery is <1 day delivery
'''

# COMMAND ----------

data = [(1,1,'2019-08-01',"2019-08-02"),(2,2,'2019-08-02',"2019-08-02"),(3,1,'2019-08-11',"2019-08-12"),(5,3,'2019-08-24',"2019-08-24"),(4,3,'2019-08-21',"2019-08-22"),(6,2,'2019-08-11',"2019-08-13"),(7,4,'2019-08-09',"2019-08-09")]

schema = ['delivery_id' ,'customer_id', 'order_date','customer_pref_delivery_date']

df = spark.createDataFrame(data,schema)

df.show()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, min, datediff
from pyspark.sql.window import Window


# COMMAND ----------


df = df.withColumn("order_date",to_date(col("order_date"),'yyyy-MM-dd'))\
        .withColumn("customer_pref_delivery_date",to_date(col("customer_pref_delivery_date"),'yyyy-MM-dd')).orderBy("customer_id","order_date")

df.show()

# COMMAND ----------

w_df = Window.partitionBy("customer_id").orderBy("order_date")
df_new = df.withColumn("first_order_date",min("order_date").over(w_df))\
        .withColumn("date_diff",datediff("customer_pref_delivery_date","first_order_date"))\
        .withColumn("first_delivery_id",min("delivery_id").over(w_df))
df_new.show()

# COMMAND ----------

df_total_first_order = df_new.where(col("delivery_id") == col("first_delivery_id"))
df_total_immediate_order = df_new.where((col("delivery_id") == col("first_delivery_id")) & (col("date_diff") < 1))
df_total_first_order.show()
df_total_immediate_order.show()

# COMMAND ----------

df_total_first_order_count = df_total_first_order.count()
df_total_immediate_order_count = df_total_immediate_order.count()
final_result = (round((df_total_immediate_order_count/df_total_first_order_count)*100,2))
print(final_result)
