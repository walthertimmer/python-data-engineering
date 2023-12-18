# Databricks notebook source
# MAGIC %md
# MAGIC #### Excel

# COMMAND ----------

# basic params
catalog_name = "uc_verevening"
schema_name = "test"
volume_name = "test_volume"


# COMMAND ----------

# MAGIC %md ##### Get the basics in place

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}   ")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}   ")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read from excel
# MAGIC

# COMMAND ----------

excel_path = "/Volumes/uc_verevening/test/test_volume/test/Voorbeeld_Excel.xlsx"

#df_excel = spark.read.format("com.crealytics.spark.excel").option("header","true").option("inferSchema","true").load(excel_path)

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

import pandas as pd

# first read it with pandas instead of spark native because package is not installed issue.. then convert to spark dataframe for better distribution
pd_excel = pd.read_excel(excel_path)
df_excel = spark.createDataFrame(pd_excel)

# COMMAND ----------

display(df_excel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to excel
# MAGIC Note that writing a file directly to dbfs is not supported since you cannot keep a file open while writing. So you first need to create the file locally on cluster and then copy/write it to DBFS

# COMMAND ----------

# add some data
columns = ['a','b']
values = [
    (101,102)
]
df_NewData = spark.createDataFrame(values,columns)

# COMMAND ----------

# DBTITLE 1,combine some data
df_excel.union(df_NewData)

# COMMAND ----------

# write locally
pd_excel = df_excel.toPandas()

# multiple sheets
with pd.ExcelWriter('output_excel_file.xlsx') as writer:
    pd_excel.to_excel(writer, sheet_name='Output', header=True,index=False)
    pd_excel.to_excel(writer, sheet_name='OutputSheet2', header=True,index=False)

# COMMAND ----------

# MAGIC %sh
# MAGIC # list all files in current directory
# MAGIC ls -a
# MAGIC pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC # copy data from local cluster to dbfs
# MAGIC mv output_excel_file.xlsx /Volumes/uc_verevening/test/test_volume/test/Export_Excel.xlsx
