from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

class transformation:

    def dedup(self,df:DataFrame,dedup_cols:List[str],cdc_col:str):

        df = df.withColumn("dedupkey",concat(*dedup_cols))
        df = df.withColumn("rank",row_number().over(Window.partitionBy("dedupkey").orderBy(col(cdc_col).desc())))
        df = df.filter(col("rank")==1).drop("rank","dedupkey")
        return df
    
    def process_timestamp(self,df):
        df = df.withColumn("process_timestamp",current_timestamp())
        return df
    
    def upsert(self,df,key_cols,table,cdc):
        dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trgt").merge(df.alias("src"), " AND ".join([f"trgt.{k} = src.{k}" for k in key_cols])) \
            .whenMatchedUpdateAll(condition=f"src.{cdc} >= trgt.{cdc}")\
            .whenNotMatchedInsertAll()\
            .execute()
        return 1

