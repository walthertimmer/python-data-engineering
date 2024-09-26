# Databricks notebook source
# MAGIC %md
# MAGIC ## Visualizations in Databricks notebooks
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/visualizations/preview-chart-visualizations
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/visualizations/

# COMMAND ----------

table_name = "hive_metastore.test_schema.test_table"

# COMMAND ----------

sparkDF = spark.sql(f"SELECT * FROM {table_name}")

# COMMAND ----------

display(sparkDF)
