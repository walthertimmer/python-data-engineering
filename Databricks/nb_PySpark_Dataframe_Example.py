# Databricks notebook source
# MAGIC %md
# MAGIC # Some basic examples with python

# COMMAND ----------

# MAGIC %python
# MAGIC # Create a DataFrame with Python
# MAGIC data = [[295, "South Bend", "Indiana", "IN", 101190, 112.9]]
# MAGIC columns = ["rank", "city", "state", "code", "population", "price"]
# MAGIC
# MAGIC df1 = spark.createDataFrame(data, schema="rank LONG, city STRING, state STRING, code STRING, population LONG, price DOUBLE")
# MAGIC display(df1)

# COMMAND ----------

# MAGIC %python
# MAGIC # set different timeout settings than normal because databricks files may be blocked
# MAGIC spark.conf.set("spark.sql.ipc.client.connect.timeout", 10)
# MAGIC spark.conf.set("spark.sql.ipc.client.connect.max.retries.on.timeouts", 2)
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC # Load data into a DataFrame from files
# MAGIC df2 = (spark.read
# MAGIC   .format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")
# MAGIC )
# MAGIC

# COMMAND ----------

display(df2.head(1))

# COMMAND ----------

# MAGIC %python
# MAGIC # Returns a DataFrame that combines the rows of df1 and df2
# MAGIC df = df1.union(df2)

# COMMAND ----------

# MAGIC %python
# MAGIC #view it
# MAGIC
# MAGIC try:
# MAGIC     display(df)
# MAGIC except AttributeError:
# MAGIC     pass
# MAGIC except NameError:
# MAGIC     df = df1 
# MAGIC     display(df)
# MAGIC     pass

# COMMAND ----------

# MAGIC %python
# MAGIC # get first row
# MAGIC display(df.head(1))

# COMMAND ----------

# MAGIC %python
# MAGIC # Print the data schema
# MAGIC df.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC # Filter rows in a DataFrame
# MAGIC filtered_df = df.filter(df["rank"] < 6)
# MAGIC display(filtered_df)
# MAGIC filtered_df = df.where(df["rank"] < 6)
# MAGIC display(filtered_df)

# COMMAND ----------

# MAGIC %python
# MAGIC # Filter rows in a DataFrame
# MAGIC filtered_df = df.filter(df["rank"] < 6)
# MAGIC display(filtered_df)
# MAGIC filtered_df = df.where(df["rank"] < 6)
# MAGIC display(filtered_df)

# COMMAND ----------

# MAGIC %python
# MAGIC # Select columns from a DataFrame
# MAGIC select_df = df.select("City", "State")
# MAGIC display(select_df)

# COMMAND ----------

# MAGIC %python
# MAGIC # Create a subset DataFrame
# MAGIC subset_df = df.filter(df["rank"] < 11).select("City")
# MAGIC display(subset_df)

# COMMAND ----------

# MAGIC %python 
# MAGIC #create a schema
# MAGIC spark.sql("CREATE SCHEMA uc_concurrentie.test")

# COMMAND ----------

# MAGIC %python
# MAGIC # Save the DataFrame to a table
# MAGIC df.write.saveAsTable("uc_concurrentie.test.city_price_data")

# COMMAND ----------

# MAGIC %python 
# MAGIC # create a volume
# MAGIC
# MAGIC spark.sql("CREATE VOLUME IF NOT EXISTS uc_concurrentie.test.test_files")

# COMMAND ----------

# MAGIC %python 
# MAGIC # Save the DataFrame to JSON files
# MAGIC json_location = "/Volumes/uc_concurrentie/test/test_files/json_data"
# MAGIC df.write.format("json") \
# MAGIC     .mode("overwrite") \
# MAGIC     .save(json_location)

# COMMAND ----------

# MAGIC %python
# MAGIC #Read the DataFrame from a JSON file
# MAGIC df3 = spark.read.format("json").json(json_location)
# MAGIC display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run SQL queries in PySpark

# COMMAND ----------

# MAGIC %python
# MAGIC # The selectExpr() method allows you to specify each column as an SQL query
# MAGIC display(df.selectExpr("`rank`", "upper(city) as big_name"))

# COMMAND ----------

# MAGIC %python
# MAGIC # You can import the expr() function from pyspark.sql.functions to use SQL syntax anywhere a column would be specified
# MAGIC from pyspark.sql.functions import expr
# MAGIC display(df.select("rank", expr("lower(city) as little_name")))

# COMMAND ----------

# MAGIC %python
# MAGIC # Run an arbitrary SQL query
# MAGIC query_df = spark.sql("SELECT * FROM uc_concurrentie.test.city_price_data")
# MAGIC display(query_df)

# COMMAND ----------

# MAGIC %python
# MAGIC # Parameterize SQL queries
# MAGIC catalog_name = 'uc_concurrentie'
# MAGIC schema_name = 'test'
# MAGIC table_name = "city_price_data"
# MAGIC
# MAGIC query_df = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")
# MAGIC display(query_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Apache Spark api reference:  
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/reference/spark  
# MAGIC Databricks PySpark API Reference:  
# MAGIC https://api-docs.databricks.com/python/pyspark/latest/index.html  

# COMMAND ----------

# MAGIC %md
# MAGIC More examples:  
# MAGIC https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html  
