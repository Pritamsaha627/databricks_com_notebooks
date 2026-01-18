# Databricks notebook source
# MAGIC %md
# MAGIC Transform the below dataframe to a new dataframe where it will contain columns 'product','month','sales'

# COMMAND ----------

data = [('A',150,200,100),('B',200,50,300),('C',300,400,100),('D',250,150,500)]

schema = ['product' ,'Jan_Sales', 'Feb_Sales','Mar_Sales']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_final = df.unpivot("product",['Jan_Sales','Feb_Sales','Mar_Sales'],'Month','Sales')
df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Show the count of activity_type for each product_category

# COMMAND ----------

data1 = [(1,'Electronics','view','2022-01-01'),(2,'Clothing','purchase','2022-01-02'),(1,'Electronics','purchase','2022-01-03'),(3,'Books','view','2022-01-04'),(2,'Clothing','view','2022-01-05')]

schema1 = ['user_id' ,'product_category', 'activity_type','timestamp']

df1 = spark.createDataFrame(data1,schema1)
df1.show()

# COMMAND ----------

df1_final = df1.groupBy('product_category').pivot('activity_type').count().na.fill(0)
df1_final.show()
