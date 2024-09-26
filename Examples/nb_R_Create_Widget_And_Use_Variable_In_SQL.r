# Databricks notebook source
# maken widget
dbutils.widgets.text("period", "123")

# COMMAND ----------

# ophalen variable in R
period <- dbutils.widgets.get("period")

print(period)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gebruiken variable in sql
# MAGIC SELECT '${period}' as test

# COMMAND ----------

# widget om extern te gebruiken in andere notebook 
dbutils.notebook.exit(r_variable)
