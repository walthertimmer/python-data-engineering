# Databricks notebook source
# MAGIC %md
# MAGIC #Datasets
# MAGIC This notebook shows all the available testsets that are available within the Databricks workspace

# COMMAND ----------

# MAGIC %md
# MAGIC #### Databricks Datasets
# MAGIC if there is no connectivity there will be a timeout on below command

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Databricks Unity Catalog Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC --both SCHEMAS or DATABASES can be used
# MAGIC SHOW SCHEMAS IN samples
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC # Get all the databases
# MAGIC df_databases = _sqldf.select('databaseName').collect()
# MAGIC database_list = [database.databaseName for database in df_databases]
# MAGIC print(database_list)
# MAGIC print(type(database_list))
# MAGIC print(len(database_list))

# COMMAND ----------

# drop the default database since it is not accessabel 
database_list.pop(0)
print(database_list)

# COMMAND ----------

# MAGIC %python
# MAGIC df_all_tables = None
# MAGIC for database_name in database_list:
# MAGIC     print(database_name)
# MAGIC     all_tables = spark.sql(
# MAGIC         f"""SHOW TABLES FROM samples.{database_name}"""
# MAGIC     )
# MAGIC     if df_all_tables is None:
# MAGIC         print("init")
# MAGIC         df_all_tables = all_tables
# MAGIC     else:
# MAGIC         print("extra databae")
# MAGIC         df_all_tables = df_all_tables.union(all_tables)

# COMMAND ----------

# MAGIC %python
# MAGIC display(df_all_tables)

# COMMAND ----------

def union_count_query(catalog_name,database_name,table_name):
    count_query = """ UNION SELECT COUNT(*) AS Aantal, '""" + catalog_name + "." + database_name + "." + table_name + """' AS Object FROM """ + catalog_name + """.""" + database_name + """.""" + table_name + "\n"
    #print(f"after: \n {count_query}")
    return count_query

# COMMAND ----------

init_count_query = "SELECT '0' AS Aantal, 'dummy' AS Object \n"
all_count_query = init_count_query

for row in df_all_tables.collect():
    print(f"adding to the super query: {row['database']} {row['tableName']} \n")
    all_count_query = all_count_query + union_count_query("samples",row['database'],row['tableName'])

print(all_count_query)

# COMMAND ----------

# MAGIC %python 
# MAGIC # Run the super count query
# MAGIC df_output_all_counts = spark.sql(all_count_query)
# MAGIC display(df_output_all_counts)
