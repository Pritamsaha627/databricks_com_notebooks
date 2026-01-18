# Databricks notebook source
'''
Question: Given a table of candidates and their skills, you're tasked with finding the candidates best suited for an open Data Science job. You want to find candidates who are proficient in Python, Tableau, and PostgreSQL.

Write a query to list the candidates who possess all of the required skills for the job. Sort the output by candidate ID in ascending order.
'''

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

data = [(123,'Python'),
				(123,'Tableau'),
				(123,'PostgreSQL'),
				(234,'R'),
				(234,'PowerBI'),
				(234,'SQL Server'),
				(345,'Python'),
				(345,'Tableau')]
		
schema = ['candidate_id', 'skill']
	

df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('tp')
df.show()




# COMMAND ----------

new_df = df.filter(col('skill').isin('Python', 'Tableau', 'PostgreSQL')).groupBy('candidate_id').agg(count('skill').alias('skill_count'))
new_df.show()

# COMMAND ----------

final_df = new_df.select('candidate_id').where('skill_count == 3').orderBy('candidate_id')
final_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (SELECT candidate_id,COUNT(skill) as skill_count FROM tp
# MAGIC WHERE skill IN ('Python','Tableau','PostgreSQL')
# MAGIC GROUP BY candidate_id)
# MAGIC
# MAGIC SELECT candidate_id FROM CTE
# MAGIC WHERE skill_count = 3
# MAGIC ORDER BY candidate_id
