# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW GROUPS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GROUPS WITH USER `pritamsaha627@gmail.com`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS workspace.default.age_musking

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.default.ind_employees ALTER COLUMN Age DROP MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.default.age_musking(Age STRING)
# MAGIC -- RETURNS INT
# MAGIC RETURN CASE 
# MAGIC     WHEN is_account_group_member('HR') THEN Age 
# MAGIC     ELSE "****"
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.default.ind_employees ALTER COLUMN Age SET MASK workspace.default.age_musking

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.ind_employees

# COMMAND ----------

df = spark.table('workspace.default.ind_employees')
# df.display()
df.write.mode('overwrite').saveAsTable('sdp_catalog.default.ind_employees')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sdp_catalog.default.ind_employees

# COMMAND ----------


'''Calculate the percentage of spam posts in all viewed posts by day. A post is considered a spam if a string "spam" is inside keywords of the post. Note that the facebook_posts table stores all posts posted by users. The facebook_post_views table is an action table denoting if a user has viewed a post.'''

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

meta_facebook_posts = spark.read.csv('/Volumes/test_catalog/default/test_volume/pyspark_interview_source/meta_facebook_posts.csv',header = True)
meta_facebook_posts = meta_facebook_posts.withColumn('post_date',to_date('post_date','dd-MM-yyyy'))
meta_facebook_posts.display()

# COMMAND ----------

meta_facebook_post_views = spark.read.csv('/Volumes/test_catalog/default/test_volume/pyspark_interview_source/meta_facebook_post_views.csv',header = True)
meta_facebook_post_views.display()


# COMMAND ----------

# Output :

+----------+----------+
| post_date|spam_share|
+----------+----------+
|2019-01-02|      50.0|
|2019-01-01|     100.0|
+----------+----------+



# COMMAND ----------

df = meta_facebook_posts.join(meta_facebook_post_views,['post_id'],'inner')
df.display()

# COMMAND ----------

final_df = df.groupBy('post_date').agg((count(when(col('post_keywords').like("%spam%"),1 ))/count('viewer_id') * 100).alias('spam_share'))
final_df.display()
