# Databricks notebook source
''' Write a Pyspark code to get running total sales for each product in each quarter.
'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    ('Pen', '2024-01-03', 100),
    ('Notebook', '2024-01-14', 600),
    ('Pen', '2024-02-05', 120),
    ('Notebook', '2024-02-18', 650),
    ('Pen', '2024-03-10', 110),
    ('Notebook', '2024-03-22', 700),
    ('Pen', '2024-04-07', 130),
    ('Notebook', '2024-04-15', 720),
    ('Pen', '2024-05-12', 140),
    ('Notebook', '2024-05-20', 750),
    ('Pen', '2024-06-09', 150),
    ('Notebook', '2024-06-25', 800),
    ('Pen', '2024-07-04', 160),
    ('Notebook', '2024-07-18', 820),
    ('Pen', '2024-08-11', 170),
    ('Notebook', '2024-08-23', 850),
    ('Pen', '2024-09-06', 180),
    ('Notebook', '2024-09-19', 900),
    ('Pen', '2024-10-13', 190),
    ('Notebook', '2024-10-27', 950),
    ('Pen', '2024-11-08', 200),
    ('Notebook', '2024-11-21', 1000),
    ('Pen', '2024-12-15', 210),
    ('Notebook', '2024-12-29', 1050)
]
schema = ('product', 'date', 'sales')

df = spark.createDataFrame(data, schema)
df = df.withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))

df.createOrReplaceTempView('tp')

display(df)

# COMMAND ----------

new_df = df.withColumn('quarter', quarter(col('date'))).withColumn('running_total', sum(col('sales')).over(Window.partitionBy('product', 'quarter').orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)))
new_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Solution

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS(SELECT *, quarter(date) AS Quarter FROM tp)
# MAGIC SELECT *, sum(sales) OVER (PARTITION BY product,Quarter ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum FROM cte
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # JSON Question 

# COMMAND ----------

import json

data = [
  {
    "order_id": 1,
    "customer": {
      "name": "John",
      "address": {
        "city": "Delhi",
        "zip": "110001"
      }
    },
    "items": [
      {"product": "Pen", "price": 10},
      {"product": "Notebook", "price": 50},
      {"product": "Pencil", "price": 5}
    ]
  },
  {
    "order_id": 2,
    "customer": {
      "name": "Alice",
      "address": {
        "city": "Mumbai",
        "zip": "400001"
      }
    },
    "items": [
      {"product": "Marker", "price": 20},
      {"product": "Notebook", "price": 60}
    ]
  },
  {
    "order_id": 3,
    "customer": {
      "name": "Bob",
      "address": {
        "city": "Bangalore",
        "zip": "560001"
      }
    },
    "items": [
      {"product": "Pen", "price": 15},
      {"product": "Eraser", "price": 8},
      {"product": "Sharpener", "price": 12}
    ]
  },
  {
    "order_id": 4,
    "customer": {
      "name": "Charlie",
      "address": {
        "city": "Kolkata",
        "zip": "700001"
      }
    },
    "items": [
      {"product": "Notebook", "price": 70},
      {"product": "Pen", "price": 20}
    ]
  },
  {
    "order_id": 5,
    "customer": {
      "name": "Diana",
      "address": {
        "city": "Chennai",
        "zip": "600001"
      }
    },
    "items": [
      {"product": "Pencil", "price": 6},
      {"product": "Marker", "price": 25},
      {"product": "Notebook", "price": 80}
    ]
  }
]

# Write to JSON file
with open("/Volumes/test_catalog/default/test_volume/orders.json", "w") as f:
    json.dump(data, f, indent=2)

print("JSON file created successfully!")

# COMMAND ----------

json_df = spark.read.option("multiLine", "true").json("/Volumes/test_catalog/default/test_volume/orders.json")
display(json_df)


# COMMAND ----------

new_json_df = json_df.withColumn('items',explode(col('items')))\
                      .withColumn('name',(col('customer.name')))\
                      .withColumn('city',(col('customer.address.city')))\
                      .withColumn('zip',(col('customer.address.zip')))\
                      .withColumn('product', col('items.product'))\
                      .withColumn('price', col('items.price'))\
                      .drop('customer', 'items')

new_json_df.display()
