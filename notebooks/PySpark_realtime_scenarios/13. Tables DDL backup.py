# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test2

# COMMAND ----------

# MAGIC %sql
# MAGIC USE test2;
# MAGIC CREATE TABLE train(
# MAGIC   train_no STRING,
# MAGIC   train_fare INT
# MAGIC )
# MAGIC

# COMMAND ----------

spark.sql('SHOW DATABASES').collect()

# COMMAND ----------

def createDDL(db):
    alltables = spark.catalog.listTables(db)
    f = open("/ddl_backup/bkp_{}.sql".format(db),"w")
    for table in alltables:
        ddl = spark.sql("SHOW CREATE TABLE {}.{};".format(db,table.name))
        f.write(ddl.first()[0])
        f.write(";\n")
    f.close()

# COMMAND ----------

createDDL('test1')

# COMMAND ----------

# MAGIC %fs 
# MAGIC head file:/ddl_backup/bkp_test1.sql

# COMMAND ----------

alldb = [db.databaseName for db in spark.sql('SHOW DATABASES').collect()]
# print(alldb)
from multiprocessing.pool import ThreadPool
processes = ThreadPool(4)
processes.map(createDDL,[db for db in alldb])

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls file:/ddl_backup

# COMMAND ----------

# MAGIC %fs
# MAGIC head file:/ddl_backup/bkp_test1.sql
# MAGIC

# COMMAND ----------


