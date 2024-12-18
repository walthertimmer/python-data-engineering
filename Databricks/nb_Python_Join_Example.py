# Databricks notebook source
# MAGIC %md
# MAGIC ## Examples of joins setup within PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC #### create some demo data

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# Create the DataFrame
data = [
    (1, "John", 25),
    (2, "Jane", 30),
    (3, "Mike", 35)
]

person_df = spark.createDataFrame(data, schema)
display(person_df)

# COMMAND ----------

from pyspark.sql.functions import floor, ceil, concat, lit

# Generate a DataFrame with ages from 1 to 100
ages = [(i,) for i in range(1, 101)]
age_category_df = spark.createDataFrame(ages, ["Age"])

# Add an additional column for age categories
age_category_df = age_category_df \
    .withColumn("Age Floor", floor(age_category_df["Age"] / 10) * 10 ) \
    .withColumn("Age Ceil", ceil(age_category_df["Age"] / 10) * 10 ) 
    # .withColumn("Age Category", concat(df["Age Floor"],df["Age Ceil"]))
age_category_df = age_category_df \
    .withColumn("Age Category", concat(age_category_df["Age Floor"],lit('-'),age_category_df["Age Ceil"])) \
    .drop("Age Floor").drop("Age Ceil")

display(age_category_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### join time

# COMMAND ----------

# MAGIC %md
# MAGIC Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'.

# COMMAND ----------

# Join df1 with df on the 'Age' column
joined_df = person_df.join(age_category_df, person_df.Age == age_category_df.Age, "left")
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC join using sql logic

# COMMAND ----------

joined_df = spark.sql("""
                      SELECT * 
                      FROM {person_df} p 
                      LEFT JOIN {age_category_df} a
                      ON 1=1 
                      AND p.Age = a.Age
                      """
                      ,person_df=person_df
                      ,age_category_df=age_category_df
                      )
display(joined_df)
