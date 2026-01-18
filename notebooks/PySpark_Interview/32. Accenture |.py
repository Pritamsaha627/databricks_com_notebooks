# Databricks notebook source
'''
You have a dataset containing information about insurance policies and claims for an insurance company. Your goal is to perform a series of complex data transformations to derive insights from the data.
Task:
1. Calculate the total payout for each claim, considering the claim_amount. Create a new column named total_payout in the DataFrame.
2. Calculate the cumulative payout for each policyholder. Create a new column named cumulative_payout for each policyholder, where cumulative payout is the sum of the total_payout for all claims made by the policyholder.
3. Identify the most frequently claimed type of coverage for each month. Create a new column named top_coverage_monthly that contains the coverage type with the highest total number of claims in each month.
4. Calculate the average claim amount for each policyholder.
'''

# COMMAND ----------

from pyspark.sql.functions import col, to_date, count, sum, year, month, rank,desc, avg
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1,1,'2023-01-01','Coverage_A',5000),
        (2,2,'2023-01-05','Coverage_B',7000),
        (1,3,'2023-02-10','Coverage_A',3000),
        (3,4,'2023-02-15','Coverage_C',4500),
        (2,5,'2023-03-03','Coverage_B',6000),
        (1,6,'2023-03-20','Coverage_A',8000),
        (1,7,'2023-04-02','Coverage_A',5500),
        (3,8,'2023-04-10','Coverage_C',7000),
        (2,9,'2023-05-05','Coverage_B',3500),
        (1,10,'2023-05-15','Coverage_A',9000),
        (3,11,'2023-06-01','Coverage_C',4200),
        (2,12,'2023-06-10','Coverage_B',5800),
        (1,13,'2023-07-05','Coverage_A',7500),
        (2,14,'2023-07-20','Coverage_B',6200),
        (3,15,'2023-08-02','Coverage_C',4800),
        (1,16,'2023-08-10','Coverage_A',5500),
        (2,17,'2023-09-01','Coverage_B',7000),
        (1,18,'2023-09-15','Coverage_A',8000), 
        (3,19,'2023-10-02','Coverage_C',4000),
        (2,20,'2023-10-10','Coverage_B',6500)]

schema = ['policyholder_id','claim_id','claim_date','coverage_type','claim_amount']
df = spark.createDataFrame(data,schema)
df = df.withColumn('claim_date',to_date('claim_date','yyyy-MM-dd'))
df.show()

# COMMAND ----------

df1 = df.withColumn('total_payout',col('claim_amount'))
df1.show()

# COMMAND ----------

df2 = df1.groupBy('policyholder_id').agg(sum('total_payout').alias('cumulative_payout'))
df2.show()

# COMMAND ----------

df3 = df.groupBy(year('claim_date').alias('year'), month('claim_date').alias('month'), 'coverage_type').agg(count('claim_id').alias('claim_count'))

df3 = df3.withColumn('rnk',rank().over(Window.partitionBy('year','month').orderBy(desc('claim_count')))).filter(col('rnk') == 1)
df3 = df3.select('year','month',col('coverage_type').alias('top_coverage_monthly'))

df3.show()                    

# COMMAND ----------

df4 = df1.groupBy('policyholder_id').agg(avg('total_payout').alias('average_claim_amount'))
df4.show()
