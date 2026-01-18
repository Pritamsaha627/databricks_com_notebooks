# Databricks notebook source
'''
✅i. write PySpark code to get the desired output. 2NDDAY_SALE is the sale after the repeat sale (ex- 1st sell's 2NDDAY_SALE is the 3rd sale).
Output result:

+-------+----------+------+----------------+--------------------+
|product| sale_date|amount|NEXT_2NDDAY_SALE|PREVIOUS_2NDDAY_SALE|
+-------+----------+------+----------------+--------------------+
|     TV|2016-11-27|   800|             500|                null|
|     TV|2016-11-30|   900|             400|                null|
|     TV|2016-12-29|   500|            null|                 800|
|     TV|2017-11-20|   400|            null|                 900|
| FRIDGE|2016-10-11|   760|             460|                null|
| FRIDGE|2016-10-13|   400|            null|                null|
| FRIDGE|2016-11-27|   460|            null|                 760|
+-------+----------+------+----------------+--------------------+

✅ii. Identify the Sale_date which has made 3rd highest sale for each product.
'''

# COMMAND ----------

data = [('TV', '2016-11-27', 800),('TV', '2016-11-30', 900),('TV', '2016-12-29', 500),('TV', '2017-11-20', 400),('FRIDGE', '2016-10-11', 760),('FRIDGE', '2016-10-13', 400),('FRIDGE', '2016-11-27',460)]

schema = ['product','sale_date','amount']
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, row_number, when, lead, lag,desc,asc, dense_rank, split, explode, collect_list,unix_timestamp,count, round
from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn('sale_date',to_date(col('sale_date'),'yyyy-MM-dd'))
new_df = df.withColumn('NEXT_2NDDAY_SALE',lead('amount',2).over(Window.partitionBy('product').orderBy('sale_date'))).withColumn('PREVIOUS_2NDDAY_SALE',lag('amount',2).over(Window.partitionBy('product').orderBy('sale_date')))
new_df.orderBy(col('product').desc()).show()

# COMMAND ----------

final_df = df.withColumn('rnk',dense_rank().over(Window.partitionBy('product').orderBy(col('amount').desc()))).filter('rnk == 3').select('product','sale_date')
final_df.show()

# COMMAND ----------

'''
✅Given below dataframe, (df [uniqueid: string, status_value(status:code:codetype): array<string>])

uniqueid           status_value(status:code:codetype)

20d75c97-5fee-11e8-92c7-67fe1c388607 ['A:X:M', 'B:Y:N', 'C:Z:O', 'D:W:P', 'E:V:Q','A:W:P']
20d75c98-5fee-11e8-92c7-5f0316c1a74f ['A:X:M', 'B:W:N', 'C:L:O'] 
20d75c99-5fee-11e8-92c7-d9bfa897a151 ['A:X:M', 'F:Y:N', 'H:Z:O','A:W:P']

For status in A, B, C output should be as follows. Write code in PySpark.

uniqueid     status_value(status:code:codetype)

20d75c97-5fee-11e8-92c7-67fe1c388607 ['A:X:M','B:Y:N', 'C:Z:O','A:W:P']
20d75c98-5fee-11e8-92c7-5f0316c1a74f ['A:X:M', 'B:W:N', 'C:L:O']
20d75c99-5fee-11e8-92c7-d9bfa897a151 ['A:X:M','A:W:P']
'''

# COMMAND ----------

data1 = [('20d75c97-5fee-11e8-92c7-67fe1c388607',['A:X:M', 'B:Y:N', 'C:Z:O', 'D:W:P', 'E:V:Q','A:W:P']),('20d75c98-5fee-11e8-92c7-5f0316c1a74f',['A:X:M', 'B:W:N', 'C:L:O']),('20d75c99-5fee-11e8-92c7-d9bfa897a151',['A:X:M', 'F:Y:N', 'H:Z:O','A:W:P'])]
schema1 = ['uniqueid','status_value']
df1 = spark.createDataFrame(data1,schema1)
df1.display()

# COMMAND ----------

df1_final = df1.select('uniqueid',explode('status_value').alias('status_value')).filter(col('status_value').contains('A') | col('status_value').contains('B') | col('status_value').contains('C'))
df1_final = df1_final.groupBy(col('uniqueid')).agg(collect_list('status_value'))
df1_final.display()

# COMMAND ----------

'''
✅ Assume you have an events table on app analytics. Write a SQL query/pyspark query to get the app’s click-through rate (CTR %) in 2022. Output the results in percentages rounded to 2 decimal places.

Notes:

Percentage of click-through rate = 100.0 * Number of clicks / Number of impressions
To avoid integer division, you should multiply the click-through rate by 100.0, not 100.

events 

Table:
Column Name Type
app_id integer
event_type string
timestamp datetime

Example Input:
app_id event_type timestamp
123 impression 07/18/2022 11:36:12
123 impression 07/18/2022 11:37:12
123 click 07/18/2022 11:37:42
234 impression 07/18/2022 14:15:12
234 click 07/18/2022 14:16:12

Example Output:
app_id ctr
123 50.00
234 100.00
'''

# COMMAND ----------

data2 = [(123, 'impression', '07/18/2022 11:36:12'),(123, 'impression', '07/18/2022 11:37:12'),(123, 'click', '07/18/2022 11:37:42'),(234, 'impression', '07/18/2022 14:15:12'),(234, 'click', '07/18/2022 14:16:12')]
schema2 = ['app_id','event_type','timestamp']
df2 = spark.createDataFrame(data2,schema2)
df2.display()

# COMMAND ----------

df2_final = df2.withColumn('timestamp',unix_timestamp('timestamp','MM/dd/yyyy HH:mm:ss').cast('timestamp'))
df2_final = df2_final.groupBy('app_id').agg(count(when(col('event_type') == 'impression',1)).alias('impression_count'),count(when(col('event_type') == 'click',1)).alias('click_count'))
df2_final = df2_final.withColumn('ctr',round(((100.0 * col('click_count'))/col('impression_count')),2)).select('app_id','ctr')
df2_final.show()

# COMMAND ----------


