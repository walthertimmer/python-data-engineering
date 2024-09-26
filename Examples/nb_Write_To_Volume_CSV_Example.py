# Databricks notebook source
# MAGIC %md
# MAGIC Example how to write a csv to a volume to then download it locally

# COMMAND ----------

# input params 
catalog_name    = 'hive_metastore'
volume_name     = 'test_volume'
schema_name     = 'test_schema'
file_name       = 'test_file'
cleanup         = False

# COMMAND ----------

# Create a dummy dataframe
header = ['koloma','kolomb']
data = [
    ('rij1','a')
    ,('rij2','b')
        ]
df = spark.createDataFrame(data).toDF(*header)
# Show the dummy dataframe
display(df)

# COMMAND ----------

# create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# Create volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

# COMMAND ----------

# Write dataframe to volume
    # repartition to make sure it only creates a single file not multiple (per partition)
    # overwrite thus always overwriting existing files
df.repartition(1).write \
    .option("header",True) \
    .option("delimiter",",") \
    .mode("overwrite") \
    .csv(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}")

# COMMAND ----------

# remove the metadata (success,committed,started files in the folder)
for file in dbutils.fs.ls(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/"):
    remove_file_path = file[0]
    if "_SUCCESS" in remove_file_path:
        print(f"removing {remove_file_path}")
        dbutils.fs.rm(f"{remove_file_path}")
    if "_committed" in remove_file_path:
        print(f"removing {remove_file_path}")
        dbutils.fs.rm(f"{remove_file_path}")
    if "_started" in remove_file_path:
        print(f"removing {remove_file_path}")
        dbutils.fs.rm(f"{remove_file_path}")
    # final file is the actual file
    if "part" in remove_file_path:
        # copy it with correct name
        dbutils.fs.cp(f"{remove_file_path}",f"dbfs:/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/{file_name}.csv")
        # remove old/wrong file name
        dbutils.fs.rm(f"{remove_file_path}")

# COMMAND ----------

if cleanup:
    # cleanup the volume, schema using CASCADE (external tables files will still be there managed will be removed in 90 days)
    print("cleanup")
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE;")

# COMMAND ----------

# MAGIC %md
# MAGIC So above you can see how to do it.   
# MAGIC Next step would be to have it in 1 Python function so you can easily adjust in the future but also can simply import it and use it in multiple occassions without causing a lot of additional code rows. 
# MAGIC

# COMMAND ----------

def write_to_csv(df,catalog_name,volume_name,schema_name,file_name):
    """
    This function can be used to write a dataframe as a single CSV file with comma seperation and a nice name. It will create a schema and volume if they don't exist. 
    """
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name};")
        df.repartition(1).write.option("header",True).option("delimiter",",").mode("overwrite").csv(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}")
        for file in dbutils.fs.ls(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/"):
            remove_file_path = file[0]
            if "_SUCCESS" in remove_file_path or "_committed" in remove_file_path or "_started" in remove_file_path:
                dbutils.fs.rm(f"{remove_file_path}")
            if "part" in remove_file_path:
                dbutils.fs.cp(f"{remove_file_path}",f"dbfs:/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/{file_name}.csv")
                dbutils.fs.rm(f"{remove_file_path}")
    except Exception as e:
        print("error during function run\n")
        print(f"{e}")
    return print(f"written df to dbfs:/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/{file_name}.csv")

# COMMAND ----------

# call the function
write_to_csv(df,catalog_name,volume_name,schema_name,file_name)
