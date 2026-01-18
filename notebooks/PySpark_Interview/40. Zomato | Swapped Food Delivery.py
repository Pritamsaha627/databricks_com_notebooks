# Databricks notebook source
'''Zomato encountered an issue with their delivery system. Due to an error in the delivery driver instructions, each item's order was swapped with the item in the subsequent row. As a data analyst, you're asked to correct this swapping error and return the proper pairing of order ID and item.

If the last item has an odd order ID, it should remain as the last item in the corrected data. For example, if the last item is Order ID 7 Tandoori Chicken, then it should remain as Order ID 7 in the corrected data.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1,'Chow Mein'),
        (2,'Pizza'),
        (3,'Pad Thai'),
        (4,'Butter Chicken'),
        (5,'Eggrolls'),
        (6,'Burger'),
        (7,'Tandoori Chicken'),
        ]
		
schema = ['order_id', 'item']

df = spark.createDataFrame(data,schema)

df.show()


# COMMAND ----------

# output : 

# COMMAND ----------

total_order = df.count()
print(total_order)

# COMMAND ----------

new_df = df.withColumn('total_order_count',lit(total_order))
new_df.show()

# COMMAND ----------

new_df = new_df.withColumn('order_id',when((col('order_id')%2 != 0) & (col('order_id') != col('total_order_count')),col('order_id')+1)
                       .when((col('order_id')%2 != 0) & (col('order_id') == col('total_order_count')),col('order_id'))
                       .otherwise(col('order_id')-1)).drop('total_order_count')



display(new_df)
