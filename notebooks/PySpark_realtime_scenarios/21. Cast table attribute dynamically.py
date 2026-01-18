# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

sales_data = [
('1','Virat','gurgaon'),
('2','Gill','Amritsar'),
('3','Rahul','jaipur'),
('4','Rinku','mosco'),
('5','SKY','jaipur')
]
sales_schema = ['customer_id','customer_name','city']
sales_df = spark.createDataFrame(sales_data,sales_schema)
sales_df.show()

# COMMAND ----------

student_data = [
('101','Ram','23'),
('102','Lisa','34'),
('103','Edward','28'),
('104','Monty','23'),
('105','Sourav','26')
]
student_schema = ['student_id','student_name','age']
student_df = spark.createDataFrame(student_data,student_schema)
student_df.show()

# COMMAND ----------

schema_dict = {'sales_df':{'customer_id':'INT','customer_name':'STRING','city':'STRING'},'student_df':{'student_id':'INT','student_name':'STRING','age':'INT'}}
type(schema_dict)

# COMMAND ----------

df = student_df
df_name = 'student_df'

for key_df,val in schema_dict.items():
    if df_name == key_df:
        print(key_df)
        for col_name,dtype in schema_dict[key_df].items():
            if col_name in df.columns:
                try:
                    df = df.withColumn(col_name,col(col_name).cast(dtype))
                except Exception as e:
                    print(f"Error in df :'{key_df}',col_name : '{col_name}',datatype : '{dtype}','{e}'")
        df.show() 


# COMMAND ----------

def table_typecasting(df,df_name,schema_dict):
    for key_df,val in schema_dict.items():
        if df_name == key_df:
            print(key_df)
            for col_name,dtype in schema_dict[key_df].items():
                if col_name in df.columns:
                    try:
                        df = df.withColumn(col_name,col(col_name).cast(dtype))
                    except Exception as e:
                        print(f"Error in df :'{key_df}',col_name : '{col_name}',datatype : '{dtype}','{e}'")
            return df

# COMMAND ----------

df = sales_df
df_name = 'sales_df'

df = table_typecasting(df,df_name,schema_dict)
df.show()
