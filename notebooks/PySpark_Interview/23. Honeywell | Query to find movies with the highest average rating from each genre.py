# Databricks notebook source
'''
Write a query to find movies with the highest average rating in each genre and then print the average rating by stars(nearest whole number)
'''

# COMMAND ----------

data1 = [(1,'Action','The Dark Knight'),(2,'Action','Avengers'),(3,'Action','Gladiator'),(4,'Action','Die Hard'),(5,'Action','Mad Max'),(6,'Drama','The Shawshank Redemption'),(7,'Drama','Forrest Gump'),(8,'Drama','The Godfather'),(9,'Drama',"Schindler's List"),(10,'Drama','Fight Club'),(11,'Comedy','The Hangover'),(12,'Comedy','Superbad'),(13,'Comedy','Dump and Dumber'),(14,'Comedy','Bridesmaids'),(15,'Comedy','Anchorman')]

schema1 = ['id','genre','title']
movie_df = spark.createDataFrame(data1,schema1)
movie_df.show()

# COMMAND ----------

data2 = [(1,4.5),(1,4.0),(1,5.0),(2,4.2),(2,4.8),(2,3.9),(3,4.6),(3,3.8),(3,4.3),(4,4.1),(4,3.7),(4,4.4),(5,3.9),(5,4.5),(5,4.2),(6,4.8),(6,4.7),(6,4.9),(7,4.6),(7,4.9),(7,4.3),(8,4.9),(8,5.0),(8,4.8),(9,4.7),(9,4.9),(9,4.5),(10,4.6),(10,4.3),(10,4.7),(11,3.9),(11,4.0),(11,3.5),(12,3.7),(12,3.8),(12,4.2),(13,3.2),(13,3.5),(13,3.8),(14,3.8),(14,4.0),(14,4.2),(15,3.9),(15,4.0),(15,4.1)]

schema2 = ['movie_id','rating']
rating_df = spark.createDataFrame(data2,schema2)
rating_df.show()

# COMMAND ----------

Output :
+------+---------------+----------+------+
| genre|          title|avg_rating|rating|
+------+---------------+----------+------+
|Action|The Dark Knight|       4.5| *****|
|Comedy|    Bridesmaids|       4.0|  ****|
| Drama|  The Godfather|       4.9| *****|
+------+---------------+----------+------+

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

rating_df = rating_df.groupBy("movie_id").agg(F.round(F.avg("rating"),1).alias("avg_rating"))
rating_df.show()

# COMMAND ----------

df = movie_df.join(rating_df,movie_df.id == rating_df.movie_id,"inner").orderBy("movie_id")
df.show()

# COMMAND ----------

w_df = Window.partitionBy("genre").orderBy(F.col("avg_rating").desc())
new_df = df.withColumn("row_num",F.row_number().over(w_df)).filter(F.col("row_num") == 1).select("genre","title","avg_rating")
new_df.show()

# COMMAND ----------

final_df = new_df.withColumn("rating",F.expr("repeat('*', round(avg_rating))"))
final_df.show()
