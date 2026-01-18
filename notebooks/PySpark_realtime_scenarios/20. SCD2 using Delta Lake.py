# Databricks notebook source
# dbutils.fs.rm('dbfs:/FileStore/SCDcustomerData',True)

# COMMAND ----------

# customer_data = [

# (1,'Virat','Kolkata','india','N','2021-09-15','2022-09-24'),
# (2,'Rohit','Mumbai','india','Y','2022-08-12',None),
# (3,'Gill','Amritsar','india','Y','2022-09-10',None),
# (4,'Rahul','Delhi','india','Y','2022-06-10',None),
# (5,'Rinku','NY','USA','Y','2022-06-10',None),
# (1,'Virat','gurgaon','india','Y','2022-09-25',None),
# ]

# customer_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

# customer_df = spark.createDataFrame(customer_data,customer_schema)
# customer_df = customer_df.withColumn('effective_start_date',to_date('effective_start_date','yyyy-MM-dd')).withColumn('effective_end_date',to_date('effective_end_date','yyyy-MM-dd'))
# customer_df.show()

# customer_df.write.format('delta').save('dbfs:/FileStore/SCDcustomerData')

# COMMAND ----------

from pyspark.sql.functions import col, to_date,concat,lit
from delta.tables import *

# COMMAND ----------

sales_data = [

(1,1,'Virat','2023-01-16','gurgaon','india',380),
(77,1,'Virat','2023-03-11','bangalore','india',300),
(12,3,'Gill','2023-09-20','Amritsar','india',127),
(54,4,'Rahul','2023-08-10','jaipur','india',321),
(65,5,'Rinku','2023-09-07','mosco','russia',765),
(89,6,'SKY','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']


sales_df = spark.createDataFrame(sales_data,sales_schema)
sales_df = sales_df.withColumn('sales_date',to_date('sales_date','yyyy-MM-dd'))

# COMMAND ----------

sales_df.show()

# COMMAND ----------

deltatable = DeltaTable.forPath(spark,'dbfs:/FileStore/SCDcustomerData')
cust_df = deltatable.toDF()
cust_df.show()

# COMMAND ----------

join_df = sales_df.join(cust_df, (cust_df.id == sales_df.customer_id),'left')
join_df.display()

# COMMAND ----------

filter_df = join_df.filter((join_df.id.isNull()) | ((join_df.active == 'Y') & (join_df.food_delivery_address != join_df.city))).withColumn('comp_key',concat(join_df.customer_id,join_df.name))
filter_df.display()

# COMMAND ----------

change_df = filter_df.filter(filter_df.id.isNotNull()).withColumn('comp_key',lit(None))
change_df.display()

# COMMAND ----------

merge_df = filter_df.union(change_df)
merge_df.display()

# COMMAND ----------

deltatable.alias("target").merge(
    merge_df.alias("source"),
    "concat(target.id,target.name) == source.comp_key and target.active == 'Y'",
).whenMatchedUpdate(
    set={
        "target.active": "'N'",
        "target.effective_end_date": "source.sales_date",
    }
).whenNotMatchedInsert(
    values={
        "target.id": "source.customer_id",
        "target.name": "source.customer_name",
        "target.city": "source.food_delivery_address",
        "target.country": "source.food_delivery_country",
        "target.active": "'Y'",
        "target.effective_start_date": "source.sales_date",
        "target.effective_end_date": lit(None),
    }
).execute()

# COMMAND ----------

df = spark.read.format('delta').load('dbfs:/FileStore/SCDcustomerData')
df.display()
