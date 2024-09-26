# Databricks notebook source
# MAGIC %md
# MAGIC Read asc file which can contain , in values which should remain there. 

# COMMAND ----------

asc_file_path = "dbfs:/mnt/import/test_file.ASC"

# COMMAND ----------

def read_asc_file(asc_file_path):
    df_asc = spark.read \
        .option("header", False) \
        .option("inferSchema",True) \
        .text(asc_file_path)
    df_split = df_asc.withColumn("value", regexp_replace(col("value"), "\\s+", " ")) # replace multiple spaces with one
    df_split = df_split.withColumn("columns", split(df_split["value"], " ")) # split based on space
    df_split = df_split.withColumn("columns", expr("filter(columns, x -> x != '')")) # remove empty column
    df_split = df_split.drop("value") # drop unneeded value column
    df_final = df_split.selectExpr("columns[0] as column1", "columns[1] as column2", "columns[2] as column3", "columns[3] as column4") # create seperate columns based on data
    return df_final

df_asc = read_asc_file(asc_file_path)
display(df_asc)

# COMMAND ----------

# MAGIC %md
# MAGIC Below everything as seperate steps for example

# COMMAND ----------

# read the file as text, not csv
df_asc = spark.read \
    .option("header", False) \
    .option("inferSchema",True) \
    .text(asc_file_path)

# COMMAND ----------

df_asc.printSchema()
display(df_asc,10)

# COMMAND ----------

# Find the row that contains a "," in the value column of df_asc to see if it is properly read
row_with_comma = df_asc.filter("value LIKE '%,%'").collect()
display(row_with_comma)

# COMMAND ----------

from pyspark.sql.functions import split, col,regexp_replace

# Replace multiple spaces in column values with a single space
df_split = df_asc.withColumn("value", regexp_replace(col("value"), "\\s+", " "))
display(df_split,100)

# COMMAND ----------

# Split the column into separate columns based on white spaces
df_split = df_split.withColumn("columns", split(df_split["value"], " "))
display(df_split,100)

# COMMAND ----------

from pyspark.sql.functions import expr

# drop all columns with empty values (empty spaces), in this case the last one
df_split = df_split.withColumn("columns", expr("filter(columns, x -> x != '')"))
display(df_split,100)

# COMMAND ----------

# drop no longer needed value column
df_split = df_split.drop("value")
display(df_split,100)

# COMMAND ----------

# create columns for all values
df_final = df_split.selectExpr("columns[0] as column1", "columns[1] as column2", "columns[2] as column3", "columns[3] as column4")
display(df_final,100)

# COMMAND ----------

# Find the row that contains a "," in the value column of df_asc to verify it has survived
final_with_comma = df_final.filter("column2 LIKE '%,%'").collect()
display(final_with_comma)
