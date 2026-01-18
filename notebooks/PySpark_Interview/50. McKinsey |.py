# Databricks notebook source
'''Given a list of pizza toppings, consider all the possible 3-topping pizzas, and print the total cost of that. Sort the result with cost in ascending order. Break ties by listing the ingredients in alphabetical order, starting from the first ingredient, followed by the second and third.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [('Pepperoni',5),('Sausage',7),('Chicken',55),('Extra Cheese',4)]
schema = ['topping_name','ingredient_cost']

df = spark.createDataFrame(data,schema).orderBy(asc('topping_name'))
df.show()

# COMMAND ----------

# Output : 
+------------------------------+----+
|pizza                         |cost|
+------------------------------+----+
|Chicken,Pepperoni,Sausage     |67  |
|Chicken,Extra Cheese,Pepperoni|64  |
|Chicken,Extra Cheese,Sausage  |66  |
|Extra Cheese,Pepperoni,Sausage|16  |
+------------------------------+----+

# COMMAND ----------

new_df = df.alias('t1').join(df.alias('t2'),col('t1.topping_name') < col('t2.topping_name'),'inner')\
                        .join(df.alias('t3'),col('t2.topping_name') < col('t3.topping_name'),'inner')\
                        .select(col('t1.topping_name').alias('t1_topping_name'),col('t1.ingredient_cost').alias('t1_ingredient_cost'),col('t2.topping_name').alias('t2_topping_name'),col('t2.ingredient_cost').alias('t2_ingredient_cost'),col('t3.topping_name').alias('t3_topping_name'),col('t3.ingredient_cost').alias('t3_ingredient_cost'))
new_df.display()

# COMMAND ----------

final_df = new_df.withColumn('pizza',concat_ws(',','t1_topping_name','t2_topping_name','t3_topping_name'))\
                  .withColumn('cost',col('t1_ingredient_cost') + col('t2_ingredient_cost') + col('t3_ingredient_cost'))\
                  .select('pizza','cost').orderBy(desc('cost'))

final_df.show(truncate= 0)
