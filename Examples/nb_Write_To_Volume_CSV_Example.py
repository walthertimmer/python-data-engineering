# Databricks notebook source
# MAGIC %md
# MAGIC Example how to write a csv to a volume to then download it locally

# COMMAND ----------

# MAGIC %python
# MAGIC # input params 
# MAGIC catalog_name    = 'uc_verevening'
# MAGIC volume_name     = 'test_volume'
# MAGIC schema_name     = 'test'
# MAGIC file_name       = 'test'
# MAGIC cleanup         = False

# COMMAND ----------

# MAGIC %python
# MAGIC # Create a dummy dataframe
# MAGIC header = ['koloma','kolomb']
# MAGIC data = [
# MAGIC     ('rij1','a')
# MAGIC     ,('rij2','b')
# MAGIC         ]
# MAGIC df = spark.createDataFrame(data).toDF(*header)
# MAGIC # Show the dummy dataframe
# MAGIC display(df)

# COMMAND ----------

# MAGIC %python
# MAGIC # create schema
# MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %python
# MAGIC # Create volume
# MAGIC spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

# COMMAND ----------

# MAGIC %python
# MAGIC # Write dataframe to volume
# MAGIC     # repartition to make sure it only creates a single file not multiple (per partition)
# MAGIC     # overwrite thus always overwriting existing files
# MAGIC df.repartition(1).write \
# MAGIC     .option("header",True) \
# MAGIC     .option("delimiter",",") \
# MAGIC     .mode("overwrite") \
# MAGIC     .csv(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}")

# COMMAND ----------

# MAGIC %python
# MAGIC # remove the metadata (success,committed,started files in the folder)
# MAGIC for file in dbutils.fs.ls(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/"):
# MAGIC     remove_file_path = file[0]
# MAGIC     if "_SUCCESS" in remove_file_path:
# MAGIC         print(f"removing {remove_file_path}")
# MAGIC         dbutils.fs.rm(f"{remove_file_path}")
# MAGIC     if "_committed" in remove_file_path:
# MAGIC         print(f"removing {remove_file_path}")
# MAGIC         dbutils.fs.rm(f"{remove_file_path}")
# MAGIC     if "_started" in remove_file_path:
# MAGIC         print(f"removing {remove_file_path}")
# MAGIC         dbutils.fs.rm(f"{remove_file_path}")
# MAGIC     # final file is the actual file
# MAGIC     if "part" in remove_file_path:
# MAGIC         # copy it with correct name
# MAGIC         dbutils.fs.cp(f"{remove_file_path}",f"dbfs:/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/{file_name}.csv")
# MAGIC         # remove old/wrong file name
# MAGIC         dbutils.fs.rm(f"{remove_file_path}")
# MAGIC

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

# MAGIC %python
# MAGIC def write_to_csv(df,catalog_name,volume_name,schema_name,file_name):
# MAGIC     """
# MAGIC     This function can be used to write a dataframe as a single CSV file with comma seperation and a nice name. It will create a schema and volume if they don't exist. 
# MAGIC     """
# MAGIC     try:
# MAGIC         spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};")
# MAGIC         spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name};")
# MAGIC         df.repartition(1).write.option("header",True).option("delimiter",",").mode("overwrite").csv(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}")
# MAGIC         for file in dbutils.fs.ls(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/"):
# MAGIC             remove_file_path = file[0]
# MAGIC             if "_SUCCESS" in remove_file_path or "_committed" in remove_file_path or "_started" in remove_file_path:
# MAGIC                 dbutils.fs.rm(f"{remove_file_path}")
# MAGIC             if "part" in remove_file_path:
# MAGIC                 dbutils.fs.cp(f"{remove_file_path}",f"dbfs:/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/{file_name}.csv")
# MAGIC                 dbutils.fs.rm(f"{remove_file_path}")
# MAGIC     except Exception as e:
# MAGIC         print("error during function run\n")
# MAGIC         print(f"{e}")
# MAGIC     return print(f"written df to dbfs:/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_name}/{file_name}.csv")

# COMMAND ----------

# MAGIC %python
# MAGIC # call the function
# MAGIC write_to_csv(df,catalog_name,volume_name,schema_name,file_name)
