# Databricks notebook source
# MAGIC %md 
# MAGIC ## Some handy commands

# COMMAND ----------

columns = ["koloma","kolomb"]
values = [
    ("test","12")
    ,("blob","4")
    ]
df_dummy = spark.createDataFrame(values,columns)

# COMMAND ----------

df_dummy.schema

# COMMAND ----------

df_dummy.printSchema()

# COMMAND ----------

display(df_dummy)

# COMMAND ----------

df_dummy.show()

# COMMAND ----------

# count of rows
df_dummy.count()

# COMMAND ----------

df_dummy.describe()
display(df_dummy.summary())

# COMMAND ----------

# first x rows
df_dummy.head()
df_dummy.first()

# COMMAND ----------

# get everything in array
df_dummy.collect()

# COMMAND ----------

# get an array with first x rows
df_dummy.take(5)

# COMMAND ----------

df_dummy.createOrReplaceTempView("dummy")
df_dummy = (spark.sql("""
              SELECT * FROM dummy
              UNION
              SELECT 
              "drie"    AS koloma
              ,45       AS kolomb
              """)
)

# COMMAND ----------

display(df_dummy)
