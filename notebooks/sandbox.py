# Databricks notebook source
# %pip install openpyxl

# COMMAND ----------

# import pandas as pd


# df = pd.read_excel('dbfs:/FileStore/source_file/hashbro_demo.xlsx',engine = 'openpyxl')
# display(df)


# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col, lit, row_number
from pyspark.sql.window import Window
import pandas as pd

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/hashbro_demo_final.csv')
display(df)

# COMMAND ----------

df1 = df.select(df.columns[0:8])\
        .withColumnRenamed('_c0','Vendor')\
        .withColumnRenamed('_c1','Hasbro')\
        .withColumnRenamed('_c2','Item')\
        .withColumnRenamed('_c3','Item_details')\
        .withColumnRenamed('_c4','Mfg. Part #')\
        .withColumnRenamed('_c5','Brand')\
        .withColumnRenamed('_c6','Last Cost')\
        .withColumnRenamed('_c7','Current retail')\
        .withColumn('index', monotonically_increasing_id())\

df1 = df1.filter(~df1.index.isin([0,1])).drop('index')
df1 = df1.withColumn('id', row_number().over(Window.orderBy(lit(''))))

# df1.show()

# COMMAND ----------

schema = ['Tot OH U', 'Sls U', 'Str OH U', 'Sls $', 'Tot OO U']
a = 9
b = 14

for i in range(len(df.collect()[0][9::5])):
    data = []
    for row in df.collect()[2:]:
        data.append(row[a:b])
    data_df = spark.createDataFrame(data,schema)
    data_df = data_df.withColumn('id',row_number().over(Window.orderBy(lit(''))))
    # data_df.display()
    a = a+5
    b = b+5

    if i == 0:
        new_df = df1.withColumn('week',lit(df.collect()[0][9::5][i]))
        final_df = new_df.join(data_df,'id','inner')
        # final_df.display()
        
    else:
        new_df = df1.withColumn('week',lit(df.collect()[0][9::5][i]))
        new_df = new_df.join(data_df,'id','inner')
        # new_df.display()
        final_df = final_df.union(new_df)
final_df.drop('id').display()

# COMMAND ----------

# for i in range(len(df.collect()[0][9::5])):
#     data = []
    
#     if i == 0:
    
#         final_df = df1.withColumn('week',lit(df.collect()[0][9::5][i]))
#     else:
#         new_df = df1.withColumn('week',lit(df.collect()[0][9::5][i]))
#         final_df = final_df.union(new_df)
# display(final_df)

# COMMAND ----------

# for i in range(len(df.collect()[0][9::5])):
#     data = []
#     for row in df.collect()[2:]:
#         data.append(row[a:b])
#     data_df =spark.createDataFrame(data,schema)
#     a = a+5
#     b = b+5
#     display(data_df)

# COMMAND ----------


# schema = ['Tot OH U', 'Sls U', 'Str OH U', 'Sls $', 'Tot OO U']
# a = 9
# b = 14
# for i in range(len(df.collect()[0][9::5])):
#     data = []
#     for row in df.collect()[2:]:
#         data.append(row[a:b])
#     window = Window.orderBy(lit(''))
#     data_df =spark.createDataFrame(data,schema).withColumn('id', row_number().over(window))
#     a = a+5
#     b = b+5
#     display(data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Date generator

# COMMAND ----------

# Sample sales data for a quarter (18 weeks), only for Friday, Saturday, Sunday, Monday
from datetime import datetime, timedelta

days = ['Friday', 'Saturday', 'Sunday', 'Monday']
start_date = datetime(2026, 1, 2)  
sales_data = []
for week in range(18):
    for i, day in enumerate(days):
      # print('i :',i)
      # print('day :',day)
      # print('timedelta :',timedelta(weeks=week, days=i))
      date = start_date + timedelta(weeks=week, days=i)
      # print('date :',date)
      # print('*'*10)
      sales = 1000 + week * 100 + i * 50  

      sales_data.append((date.strftime('%Y-%m-%d'), day, sales))
print(sales_data)
