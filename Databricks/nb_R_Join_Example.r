# Databricks notebook source
# MAGIC %md
# MAGIC ### R joins
# MAGIC
# MAGIC How to combine datasets? 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dummy datasets

# COMMAND ----------

df1 = data.frame(CustomerId = c(1:6), Product = c(rep("Toaster", 3), rep("Radio", 3)))
df2 = data.frame(CustomerId = c(2, 4, 6), State = c(rep("Alabama", 2), rep("Ohio", 1)))

# COMMAND ----------

df1

# COMMAND ----------

df2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join / combine them

# COMMAND ----------

# MAGIC %md
# MAGIC inner join

# COMMAND ----------

merge(df1, df2, by = "CustomerId")

# COMMAND ----------

# MAGIC %md
# MAGIC outer join

# COMMAND ----------

merge(x = df1, y = df2, by = "CustomerId", all = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC left outer

# COMMAND ----------

merge(x = df1, y = df2, by = "CustomerId", all.x = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC right outer

# COMMAND ----------

merge(x = df1, y = df2, by = NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC cross join

# COMMAND ----------

merge(x = df1, y = df2, by = NULL)
