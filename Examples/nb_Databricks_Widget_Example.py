# Databricks notebook source
# MAGIC %md
# MAGIC ## Databricks widgets
# MAGIC To view the documentation for the widget API in Scala, Python, or R, use the following command: dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help()
dbutils.widgets.help("dropdown")
dbutils.widgets.help("text")

# COMMAND ----------

# MAGIC %md 
# MAGIC create the widgets
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.dropdown("state", "CA", ["CA", "IL", "MI", "NY", "OR", "VA"])
dbutils.widgets.text("a","")
dbutils.widgets.text("b","")

# COMMAND ----------

# MAGIC %md 
# MAGIC get the values

# COMMAND ----------

a = dbutils.widgets.get("a")
b = dbutils.widgets.get("b")


# COMMAND ----------

def Count(a,b):
    return a + b

# COMMAND ----------

print(Count(a,b))


# COMMAND ----------


