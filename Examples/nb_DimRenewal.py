# Databricks notebook source
# MAGIC %md
# MAGIC dim renewal with correct surrkey test notebook

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- create a demo table
# MAGIC CREATE OR REPLACE TABLE hive_metastore.default.dim_demo AS
# MAGIC   SELECT
# MAGIC     1       AS SurrKey
# MAGIC   , 1       AS ID
# MAGIC   , 'First' AS Label
# MAGIC
# MAGIC   UNION 
# MAGIC   SELECT
# MAGIC     2       AS SurrKey
# MAGIC   , 5       AS ID
# MAGIC   , 'Second' AS Label
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.default.dim_demo_new AS
# MAGIC   SELECT 
# MAGIC     1       AS SurrKey
# MAGIC   , 5       AS ID
# MAGIC   , 'NewFirst' AS Label
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     2       AS SurrKey
# MAGIC   , 3       AS ID
# MAGIC   , 'TotallyNew' AS Label

# COMMAND ----------

existing_dim_df = spark.sql("select * /*, 'existing' AS Dim */ from hive_metastore.default.dim_demo")
existing_dim_df.registerTempTable("tmp_existing_dim")
new_dim_df = spark.sql("select * /*, 'new' AS Dim */ from hive_metastore.default.dim_demo_new")
new_dim_df.registerTempTable("tmp_new_dim")

# COMMAND ----------

# get highest existing surrkey
highest_existing_surrkey = spark.sql("Select Max(SurrKey) FROM tmp_existing_dim").head()[0]
print(highest_existing_surrkey)

# COMMAND ----------

# adjust new data to have new surrkeys that are higher
data_to_be_added_df = spark.sql(f"""
                                SELECT 
                                    ROW_NUMBER() OVER (ORDER BY new.ID ASC) + {highest_existing_surrkey} AS NewSurrKey
                                ,    new.*
                                --,   old.*
                                FROM tmp_new_dim AS new 
                                LEFT JOIN tmp_existing_dim AS old 
                                    ON 1=1 
                                    AND new.id = old.id
                                WHERE 1=1  
                                    AND old.ID IS NULL
                                    
                                """)


# then correct it to make it ready for insert/upsert?
# remove old surrkey columm
# rename new surrkey column

data_to_be_added_df = data_to_be_added_df.drop("SurrKey")
data_to_be_added_df = data_to_be_added_df.withColumnRenamed("NewSurrKey","SurrKey")
display(data_to_be_added_df)
data_to_be_added_df.registerTempTable("tmp_data_to_be_added")

# COMMAND ----------

# inleggen
spark.sql("INSERT INTO hive_metastore.default.dim_demo SELECT * FROM tmp_data_to_be_added")

# COMMAND ----------

# MAGIC %sql
# MAGIC --see results from insert of new data with correct SurrKey
# MAGIC SELECT * FROM hive_metastore.default.dim_demo;

# COMMAND ----------

# MAGIC %md  
# MAGIC ###situaties:  
# MAGIC 1-bestaande data krijgt update  
# MAGIC 2-er is nieuwe data  
# MAGIC    
# MAGIC #### Situation 2:  
# MAGIC 0- ophalen max surrkey van bestaande data  
# MAGIC 1- van nieuwe data bestaande afhalen (kan dit puur op ID?)  
# MAGIC 2- voor nieuwe data de surrkey toevoegen en berekenen vanaf de max van huidige data  
# MAGIC 3- inleggen nieuwe data in bestaande data   
# MAGIC
# MAGIC #### Situation 1:  
# MAGIC Make sure to not change the surrkey or id but only value fields. 
# MAGIC Must be dynamic to collect all columns from the dimensional table and substract the SurrKey column to make sure that one is never updated. 
# MAGIC   
# MAGIC   
# MAGIC ####Lessons learned  
# MAGIC - merge doesn't offer the possibility to include functions like row_number so dynamically adjust row_number 
# MAGIC - there is a possibility in the future that new source data has missing IDs that were there before. Simply adjusting the new source set SurrKey will thus not work since then you will have a miscount. Otherwise it would be possible to update and insert using one merge statement. 
# MAGIC - best to split both statements for clearity. One merge which can update existing rows (but not the surrkey column!) and one insert statement which will only insert rows with new IDs
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- new new update data
# MAGIC CREATE OR REPLACE VIEW hive_metastore.default.dim_demo_new_update AS
# MAGIC   SELECT 
# MAGIC     99       AS SurrKey
# MAGIC   , 5       AS ID
# MAGIC   , 'UpdatedData' AS Label
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     98       AS SurrKey
# MAGIC   , 3       AS ID
# MAGIC   , 'So updated' AS Label

# COMMAND ----------

# collect all columns names
headers = spark.sql("SELECT * FROM hive_metastore.default.dim_demo_new_update LIMIT 1").columns
print(headers)

# COMMAND ----------

## set on condition
# ON t.ID = s.ID, always the second column 
merge_on_condition = "t." + headers[1] + " = s." + headers[1]

print(merge_on_condition)

# COMMAND ----------

## set update statement without SurrKey field
# to do: wildcard SurrKey in name
merge_update_set_condition = ""
for column_name in headers:
    if column_name != "SurrKey":
        merge_update_set_condition = merge_update_set_condition + f" {column_name} = s.{column_name},"

# remove the last , since it is not needed
merge_update_set_condition = merge_update_set_condition[:-1]
print(merge_update_set_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- new data plus updates in one
# MAGIC CREATE OR REPLACE VIEW hive_metastore.default.dim_demo_new_update AS
# MAGIC   SELECT 
# MAGIC     55       AS SurrKey
# MAGIC   , 5       AS ID
# MAGIC   , 'ThirdData' AS Label
# MAGIC   UNION
# MAGIC   SELECT
# MAGIC     11       AS SurrKey
# MAGIC   , 11       AS ID
# MAGIC   , 'New' AS Label

# COMMAND ----------

# one merge for updating existing data

# can a merge be used to just only update data? yeah
# but if we need to write out all columns.. can't we just also do that for the insert with select surrkey?
spark.sql(f"""
MERGE INTO hive_metastore.default.dim_demo as t 
using hive_metastore.default.dim_demo_new_update as s
ON {merge_on_condition}
/* t.ID = s.ID */
WHEN MATCHED 
THEN UPDATE SET 
    {merge_update_set_condition}
    /* ID = s.ID, Label = s.Label */
""")

# COMMAND ----------

# one merge for inserting new data from prepped source
spark.sql(f"""
MERGE INTO hive_metastore.default.dim_demo as t 
using tmp_data_to_be_added as s
ON {merge_on_condition}
/* t.ID = s.ID */ 
WHEN NOT MATCHED 
THEN INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC --end results check
# MAGIC
# MAGIC SELECT * FROM hive_metastore.default.dim_demo;
