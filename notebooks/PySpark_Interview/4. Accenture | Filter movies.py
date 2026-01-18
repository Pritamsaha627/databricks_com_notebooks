# Databricks notebook source
"""Write a PySpark query to report the movies with an odd-numbered ID and a description that is not "boring". Return the result table in descending order by rating"""

# COMMAND ----------

movie_data = [(1,'War','great 3D',8.9),(2,'Science','fiction',8.5),(3,'Irish','boring',6.2),(4,'Ice song','Fantacy',8.6),(5,'House card','Interesting',9.1)]
movie_schema = ['id','movie','description','rating']
movie_df = spark.createDataFrame(movie_data,movie_schema)
display(movie_df)

# COMMAND ----------

from pyspark.sql.functions import col

final_df = movie_df.select("*").filter((col("id") %2 != 0) & (movie_df.description != "boring")).orderBy(col("rating"),ascending = False)
final_df.show()

# COMMAND ----------

# MAGIC %md CASE STATEMENT SOLUTION

# COMMAND ----------

from pyspark.sql.functions import when

result_df = movie_df.select("*").where(when(col("id")%2 != 0, True).otherwise(False) & when(movie_df.description != "boring",True).otherwise(False)).orderBy(col("rating").desc())
result_df.show()

# COMMAND ----------


