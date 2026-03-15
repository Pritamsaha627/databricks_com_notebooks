# Databricks notebook source
  +------------+-------+----------+
  | name       | price | quantity |
  +------------+-------+----------+
  | Pencil     | 3     | 30       |
  | Eraser     | 5     | 3        |
  | Notebook   | 5     | 3        |
  | Pen        | 6     | 20       |
  +------------+-------+----------+

# COMMAND ----------

'''name represents the name of the product, price is the price of the product, and quantity is the number of units available inshop. With 100 rupees, what is the maximum number of items you can buy?'''

# COMMAND ----------

data = [("Pencil", 3, 30), ("Eraser", 5, 3), ("Notebook", 5, 3), ("Pen", 6, 20)]
df = spark.createDataFrame(data, ["name", "price", "quantity"])
display(df)

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create
# MAGIC CREATE TABLE products (
# MAGIC   name varchar,
# MAGIC   price integer,
# MAGIC   quantity integer
# MAGIC );
# MAGIC
# MAGIC -- insert
# MAGIC INSERT INTO products VALUES ('Pencil',3,30);
# MAGIC INSERT INTO products VALUES ('Eraser', 5,3);
# MAGIC INSERT INTO products VALUES ('Notebook', 5,3);
# MAGIC INSERT INTO products VALUES ('Pen',6,20);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Implement your solution here
# MAGIC WITH Expanded AS(
# MAGIC     SELECT name,price FROM products
# MAGIC     JOIN generate_series(1,quantity) AS qty ON true
# MAGIC ),
# MAGIC Ranked AS(
# MAGIC     SELECT *,ROW_NUMBER()OVER(ORDER BY price ASC) AS rn,SUM(price)OVER(ORDER BY price ASC ROWS UNBOUNDED PRECEDING) AS running_total FROM Expanded
# MAGIC     )
# MAGIC     SELECT COUNT(*) AS quantity FROM Ranked
# MAGIC     WHERE running_total <= 100
# MAGIC
