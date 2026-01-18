# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [('MI',),('CSK',),('RCB',),('KKR',),('RR',),('DC',)]
schema = ['team_name']

df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# Output : 

+-----+-----+----------+
|team1|team2|   matches|
+-----+-----+----------+
|   MI|  RCB| MI vs RCB|
|   MI|   RR|  MI vs RR|
|  CSK|   MI| CSK vs MI|
|  CSK|  RCB|CSK vs RCB|
|  CSK|  KKR|CSK vs KKR|
|  CSK|   RR| CSK vs RR|
|  CSK|   DC| CSK vs DC|
|  RCB|   RR| RCB vs RR|
|  KKR|   MI| KKR vs MI|
|  KKR|  RCB|KKR vs RCB|
|  KKR|   RR| KKR vs RR|
|   DC|   MI|  DC vs MI|
|   DC|  RCB| DC vs RCB|
|   DC|  KKR| DC vs KKR|
|   DC|   RR|  DC vs RR|
+-----+-----+----------+


# COMMAND ----------

new_df = df.alias('t1').join(df.alias('t2'),col('t1.team_name') < col('t2.team_name'), 'inner' ).select(col('t1.team_name').alias('team1'),col('t2.team_name').alias('team2'))

new_df.display()

# COMMAND ----------

final_df = new_df.withColumn('matches',concat_ws(' vs ',col('team1'),col('team2')))
final_df.display()
