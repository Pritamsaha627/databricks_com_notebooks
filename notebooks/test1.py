# Databricks notebook source
from pyspark.sql.functions import col, to_date, month, year, count

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test1").master("local[5]").getOrCreate()

# COMMAND ----------

demo_df = spark.read.csv('FileStore/demo12q4.txt',sep="$",header=True,inferSchema=True)

# COMMAND ----------

reac_df = spark.read.csv('FileStore/reac12q4.txt',sep="$",header=True,inferSchema=True)

# COMMAND ----------

drug_df = spark.read.csv('FileStore/drug12q4.txt',sep="$",header=True,inferSchema=True)

# COMMAND ----------

new_df = drug_df.alias('a').join(reac_df.alias('b'),'caseid','inner')
new_df = new_df.groupBy('caseid','drugname','pt').agg(count('pt').alias('pt_count'))
new_df = new_df.join(demo_df.alias('c'),'caseid','inner').selectExpr('drugname as drug','pt','c.mfr_dt','pt_count')   
df = new_df.withColumn('mfr_dt',to_date('mfr_dt','yyyyMMdd'))
final_df = df.withColumn('year',year('mfr_dt')).withColumn('month',month('mfr_dt')).drop('mfr_dt')


# COMMAND ----------

final_df.write.csv('FileStore/sink_file/result_data',header=True,mode='overwrite')   
