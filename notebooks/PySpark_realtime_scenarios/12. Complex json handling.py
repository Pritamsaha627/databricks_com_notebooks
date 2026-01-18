# Databricks notebook source
dbutils.fs.put("FileStore/complex_json.json","""{ 
  "accounting" : [   
                     { "firstName" : "John",  
                       "lastName"  : "Doe",
                       "age"       : 23 },

                     { "firstName" : "Mary",  
                       "lastName"  : "Smith",
                        "age"      : 32 }
                 ],                            
  "sales"      : [ 
                     { "firstName" : "Sally", 
                       "lastName"  : "Green",
                        "age"      : 27 },

                     { "firstName" : "Jim",   
                       "lastName"  : "Galley",
                       "age"       : 41 }
                 ] 
} """)

# COMMAND ----------

# MAGIC %fs
# MAGIC head FileStore/complex_json.json

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/complex_json.json",multiLine=True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import explode, col, lit
df1 = df.select(explode("accounting").alias("account_explode"))
df2 = df.select(explode(df.sales).alias("sales_explode"))
display(df1)
display(df2)

# COMMAND ----------

df1 = df1.withColumn("age",col("account_explode.age"))\
        .withColumn("firstName",col("account_explode.firstName"))\
        .withColumn("lastName",col("account_explode.lastName"))\
        .withColumn("dept",lit("account")).drop(df1.account_explode)
display(df1)

# COMMAND ----------

df2 = df2.withColumns({"age":col("sales_explode.age"),"firstName":col("sales_explode.firstName"),"lastName":col("sales_explode.lastName"),"dept":lit("sales")}).drop(df2.sales_explode)

display(df2)

# COMMAND ----------

df_final = df1.union(df2)
display(df_final)
