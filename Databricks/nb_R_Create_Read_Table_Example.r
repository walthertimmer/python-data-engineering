# Databricks notebook source
# MAGIC %md
# MAGIC ## General info
# MAGIC This notebook serves the purpose of showing how to create a delta table from a (spark)R df

# COMMAND ----------

# MAGIC %md
# MAGIC create a dummy r dataset from nothing

# COMMAND ----------

# Load SparkR library
library(SparkR)

# Create a Spark session
spark <- sparkR.session()

# Create a dummy data frame with 3 columns and 10 rows
dummy_df <- data.frame(
  x = rnorm(10),
  y = base::sample(letters, 10),
  z = runif(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC convert the r dataframe to a spark dataframe

# COMMAND ----------

# Convert the data frame to a Spark data frame
spark_df <- createDataFrame(dummy_df)

# Print the schema and the first few rows of the Spark data frame
printSchema(spark_df)
head(spark_df)
display(spark_df)

# COMMAND ----------

## Another option is reading data from a CSV into a Spark data frame; example:
# df_csv <- read.df(path = "dbfs:/FileStore/tables/books.csv", source = "csv", header = "true")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a test database to write away data

# COMMAND ----------

# load package
library(sparklyr)

# Connect sparklyr to a cluster
sc <- spark_connect(method = "databricks")

# Create a database
sdf_sql(sc,"CREATE DATABASE IF NOT EXISTS test")

# change to this database 
tbl_change_db(sc, "test")

# COMMAND ----------

# MAGIC %md
# MAGIC write the dataset to a delta table

# COMMAND ----------

# Write the Spark data frame to a delta table
saveAsTable(spark_df, tableName="test.r_table",mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC collect the table in R

# COMMAND ----------

r_table <- spark_read_table(sc,name = "r_table")

# COMMAND ----------

# MAGIC %md
# MAGIC View the table in notebook

# COMMAND ----------

collect(r_table)

# COMMAND ----------

print(r_table, n=5)

# COMMAND ----------

head(r_table)

# COMMAND ----------

show(r_table)
