# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Modelling POC.
# MAGIC
# MAGIC How do seperate data modelling tools look at tables and relations inside Databricks?
# MAGIC
# MAGIC https://medium.com/@kamaljp/untangling-databases-data-modeling-with-dbeaver-b2e53ca06ca9
# MAGIC https://www.databricks.com/blog/data-modeling-best-practices-implementation-modern-lakehouse
# MAGIC
# MAGIC Due note:
# MAGIC Table constraints are only supported in Unity Catalog

# COMMAND ----------

catalog_name = "uc_sandbox"
schema_name = "data_modelling_poc"

# COMMAND ----------

# clean start
display(spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE;"))

# COMMAND ----------

display(spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name};"))

# COMMAND ----------

# Create the country table with dummy records
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.country (
    country_key INT PRIMARY KEY,
    name STRING,
    region STRING
) USING DELTA;
""")

# COMMAND ----------

# Check if the country table is empty
country_count = spark.sql(f"SELECT COUNT(*) FROM {catalog_name}.{schema_name}.country").collect()[0][0]

# Insert dummy records into the country table if it is empty
if country_count == 0:
    spark.sql(f"""
    INSERT INTO {catalog_name}.{schema_name}.country VALUES
    (1, 'United States', 'North America'),
    (2, 'Canada', 'North America'),
    (3, 'Brazil', 'South America'),
    (4, 'United Kingdom', 'Europe'),
    (5, 'Germany', 'Europe'),
    (6, 'China', 'Asia'),
    (7, 'India', 'Asia'),
    (8, 'Australia', 'Oceania'),
    (9, 'South Africa', 'Africa'),
    (10, 'Egypt', 'Africa');
    """)

# COMMAND ----------

# Display the newly inserted records
display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.country"))

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.economy (
    economy_key INT PRIMARY KEY,
    country_key INT,
    year INT,
    gdp DECIMAL(10,2),
    FOREIGN KEY (country_key) REFERENCES {catalog_name}.{schema_name}.country(country_key)
) USING DELTA;
""")

# COMMAND ----------

from pyspark.sql.functions import explode, sequence, lit, col,monotonically_increasing_id
from pyspark.sql.types import DecimalType


# Get the distinct country keys from the country table
country_keys_df = spark.sql(f"SELECT DISTINCT country_key FROM {catalog_name}.{schema_name}.country")

# Generate 10 years of dummy data for each country
years_df = spark.range(2010, 2021).withColumnRenamed("id", "year")
dummy_data_df = country_keys_df.crossJoin(years_df).withColumn("gdp", lit(10000) * col("year"))

# Identify countries in the economy table to exclude those already having data
existing_countries_df = spark.sql(f"SELECT DISTINCT country_key FROM {catalog_name}.{schema_name}.economy")

# Exclude countries that already have data
countries_to_insert_df = dummy_data_df.join(existing_countries_df, "country_key", "left_anti")

# Add the economy_key column with unique values
countries_to_insert_df = countries_to_insert_df.withColumn("economy_key", monotonically_increasing_id())

# Cast the gdp column to DECIMAL(10,2)
countries_to_insert_df = countries_to_insert_df.withColumn("gdp", col("gdp").cast(DecimalType(10, 2)))

# Reorder the DataFrame columns to match the table structure
countries_to_insert_df = countries_to_insert_df.select("economy_key", "country_key", "year", "gdp")

# Insert the dummy data into the economy table
countries_to_insert_df.write.insertInto(f"{catalog_name}.{schema_name}.economy", overwrite=False)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.economy"))
