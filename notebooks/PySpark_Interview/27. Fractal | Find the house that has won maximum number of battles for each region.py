# Databricks notebook source
'''
Below data is given from "Game of Thrones".
Find the house that has won maximum number of battles for each region.
'''

# COMMAND ----------

king_data = [(1,'Robb Stark','House Stark'),(2,'Joffrey Baratheon','House Lannister'),(3,'Stannis Baratheon','House Baratheon'),(4,'Balon Greyjoy','House Greyjoy'),(5,'Mace Tyrell','House Tyrell'),(6,'Doran Martell','House Martell')]

king_schema = ['k_no','king','house']
king_df = spark.createDataFrame(king_data,king_schema)
king_df.show()

# COMMAND ----------

battle_data = [(1,'Battle of Oxcross',1,2,1,'The North'),(2,'Battle of Blackwater',3,4,0,'The North'),(3,'Battle of the Fords',1,5,1,'The Reach'),(4,'Battle of the Green Fork',2,6,0,'The Reach'),(5,'Battle of the Ruby Ford',1,3,1,'The Riverlands'),(6,'Battle of the Golden Tooth',2,1,0,'The North'),(7,'Battle of Riverrun',3,4,1,'The Riverlands'),(8,'Battle of Riverrun',1,3,0,'The Riverlands')]

battle_schema = ['battle_no','name','attacker_king','defender_king','attacker_outcome','region']
battle_df = spark.createDataFrame(battle_data,battle_schema)
battle_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, dense_rank, desc
from pyspark.sql.window import Window

# COMMAND ----------

df = battle_df.withColumn('win',when(col('attacker_outcome') == 1, col('attacker_king')).otherwise(col('defender_king'))).select('region','win').join(king_df,col('win') == col('k_no'),'inner')
df.show()

# COMMAND ----------

new_df = df.groupBy('region','house').count()
new_df.show()

# COMMAND ----------

w_df = Window.partitionBy('region').orderBy(desc('count'))
final_df = new_df.withColumn('rnk',dense_rank().over(w_df)).filter(col('rnk') == 1).drop('count','rnk')
final_df.show()
