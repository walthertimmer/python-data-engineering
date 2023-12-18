# Databricks notebook source
# MAGIC %md
# MAGIC ## Visualizations in Databricks notebooks
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/visualizations/preview-chart-visualizations
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/visualizations/

# COMMAND ----------

sparkDF = spark.sql("SELECT * FROM ndp_consumable.25_silver_oracle_dm.obd_own_dwh_verzekerden")

# COMMAND ----------

display(sparkDF)

# COMMAND ----------

display(sparkDF)

# COMMAND ----------


