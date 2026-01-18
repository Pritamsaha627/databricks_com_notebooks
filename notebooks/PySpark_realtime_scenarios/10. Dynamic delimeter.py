# Databricks notebook source
dbutils.fs.put("FileStore/pipe_delimeter.csv","""Sl_no|Name|update_date
1|Rahul|2023-01-14
2|Ishan|2023-02-24
3|Sanju|2023-01-26""")

# COMMAND ----------

dbutils.fs.rm("FileStore/pipe_delimeter.csv",True)

# COMMAND ----------

# MAGIC %fs 
# MAGIC head dbfs:/FileStore/pipe_delimeter.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/FileStore/EmpDetails-1.csv

# COMMAND ----------

df =  spark.read.csv('dbfs:/FileStore/pipe_delimeter.csv',header=True,sep="|")
display(df)

# COMMAND ----------

import re
def get_del(file_path):
    try:
        header_list = sc.textFile(file_path).take(1)
        print(header_list)
        header_str = ''.join(header_list)
        print(header_str)
        result = re.search(",|;|\\|",str(header_list))
        print(result)
        return result.group()
    except Exception as error:
        print("Error occured: ",str(error))

# COMMAND ----------

print(get_del('dbfs:/FileStore/pipe_delimeter.csv'))

# COMMAND ----------

file_path = 'dbfs:/FileStore/EmpDetails-1.csv'
delimeter = get_del(file_path)
df =  spark.read.csv(file_path,header=True,sep=f"{delimeter}")
display(df)
