# Databricks notebook source
'''
Calculate the % of marks for each student. Each subject is worth 100 marks. Create a result column by following the below condition.

1. % marks >= 70 then 'Distinction'
2. % marks between 60-69 then 'First Class'
3. % marks between 50-59 then 'Second Class'
4. % marks between 40-49 then 'Third Class'
5. % marks <= 39 then 'Fail'
'''

# COMMAND ----------

student_data = [(1,'Amit'),(2,'Virat'),(3,'Priya')]

student_schema = ['student_id', 'student_name']

student_df = spark.createDataFrame(student_data,student_schema)
student_df.show()

# COMMAND ----------

marks_data = [(1,'PySpark',90),(2,'SQL',70),(3,'PySpark',20),(2,'PySpark',60),(1,'SQL',95),(3,'SQL',60)]

marks_schema = ['student_id', 'subject', 'marks']

marks_df = spark.createDataFrame(marks_data,marks_schema)
marks_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count,when

df = marks_df.groupBy(col('student_id')).agg(sum(col('marks')).alias('total_marks'),count(col('subject')).alias('total_sub'))
df = df.withColumn('percentage',col('total_marks')/col('total_sub'))
df.show()

# COMMAND ----------

df = df.withColumn('result',when(col('percentage')>=70,"Distinction")\
    .when(col('percentage').between(60,69),"First Class")\
    .when(col('percentage').between(50,59),"Second Class")\
    .when(col('percentage').between(40,49),"Third Class")\
    .otherwise("Fail")).join(student_df,['student_id']).drop('total_marks','total_sub')
df.show()

# COMMAND ----------


