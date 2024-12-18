# Databricks notebook source
# MAGIC %md
# MAGIC ## Multiprocessing 
# MAGIC Sometimes you want to run code in parallel to speed it up. For instance test cases that need to be run for every table. You don't want to run them sequentially but rather you would want to run them in parallel and then later on combine them in one dataframe and then run 1 write action. 

# COMMAND ----------

def count_table(database_name,schema_name,table_name):
  print(f"start for {database_name}.{schema_name}.{table_name}")
  df = spark.sql(f"""
                SELECT 
                  "{database_name}" AS DATABASE_NAME
                , "{schema_name}" AS SCHEMA_NAME
                ,  "{table_name}" AS TABLE_NAME
                ,  COUNT(*) AS COUNT 
                FROM {database_name}.{schema_name}.{table_name}
                """)
  if debug:
    display(df)
  print(f"end for {database_name}.{schema_name}.{table_name}")
  return df



# COMMAND ----------

# Run sole testcase(s) sequential
debug = True
count_table('hive_metastore','00_system','adgroupsusers')
count_table('hive_metastore','demo','product_aantallen')
debug = False

# COMMAND ----------

# run them parallel

from multiprocessing.pool import ThreadPool
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# define dict with jobs that need to be run
tables_to_count = [
    ("hive_metastore","00_system","adgroupsusers")
    ,("hive_metastore","00_system","retentions")
    ,
]
# define an empty dataframe for results
schema = StructType([
    StructField("DATABASE_NAME",StringType(),True), \
    StructField("SCHEMA_NAME",StringType(),True), \
    StructField("TABLE_NAME",StringType(),True), \
    StructField("COUNT",IntegerType(),True)
])
df_results = spark.createDataFrame([], schema)

# define how many threads at once
with ThreadPool(processes=2) as pool:
    df_result = pool.starmap(count_table, tables_to_count)
for dataframe in df_result:
    df_results = df_results.union(dataframe) 
# closing and joining threadpool to make sure jobs are finished and flushed       
pool.close()
pool.join()

# show end results with everything in one dataframe
display(df_results)
