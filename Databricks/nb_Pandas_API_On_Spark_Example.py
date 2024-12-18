# Databricks notebook source
# MAGIC %md Pandas API on Spark reference:  
# MAGIC https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html  
# MAGIC Databricks reference:  
# MAGIC https://api-docs.databricks.com/python/pyspark/latest/pyspark.pandas/index.html  

# COMMAND ----------

import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession



# COMMAND ----------

# MAGIC %md
# MAGIC Object Creation  
# MAGIC Creating a pandas-on-Spark Series by passing a list of values, letting pandas API on Spark create a default integer index:

# COMMAND ----------

s = ps.Series([1, 3, 5, np.nan, 6, 8])

# COMMAND ----------

s

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a pandas-on-Spark DataFrame by passing a dict of objects that can be converted to series-like.

# COMMAND ----------

psdf = ps.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])

# COMMAND ----------

psdf

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a pandas DataFrame by passing a numpy array, with a datetime index and labeled columns:

# COMMAND ----------

dates = pd.date_range('20130101', periods=6)

# COMMAND ----------

dates

# COMMAND ----------

pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
d = {'col1': [0, 1, 2, 3], 'col2': pd.Series([2, 3], index=[2, 3])}
pd.DataFrame(data=d, index=[0, 1, 2, 3])

# COMMAND ----------

pdf

# COMMAND ----------

# MAGIC %md
# MAGIC Now, this pandas DataFrame can be converted to a pandas-on-Spark DataFrame

# COMMAND ----------

psdf = ps.from_pandas(pdf)


# COMMAND ----------

type(psdf)

# COMMAND ----------

# MAGIC %md
# MAGIC It looks and behaves the same as a pandas DataFrame.

# COMMAND ----------

psdf

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a Spark DataFrame from pandas DataFrame

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
sdf = spark.createDataFrame(pdf)
sdf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating pandas-on-Spark DataFrame from Spark DataFrame.

# COMMAND ----------

psdf = sdf.pandas_api()
psdf

# COMMAND ----------

# MAGIC %md 
# MAGIC Having specific dtypes . Types that are common to both Spark and pandas are currently supported.

# COMMAND ----------

psdf.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Get top rows

# COMMAND ----------

psdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Displaying the index, columns, and the underlying numpy data.

# COMMAND ----------

psdf.index
psdf.columns
psdf.to_numpy()

# COMMAND ----------

# MAGIC %md 
# MAGIC Showing a quick statistic summary of your data

# COMMAND ----------

psdf.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC Transposing your data

# COMMAND ----------

psdf.T

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting by its index

# COMMAND ----------

psdf.sort_index(ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting by value

# COMMAND ----------

psdf.sort_values(by='B')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Missing data  
# MAGIC Pandas API on Spark primarily uses the value np.nan to represent missing data. It is by default not included in computations.

# COMMAND ----------

pdf1 = pdf.reindex(index=dates[0:4], columns=list(pdf.columns) + ['E'])

# COMMAND ----------

pdf1.loc[dates[0]:dates[1], 'E'] = 1

# COMMAND ----------

psdf1 = ps.from_pandas(pdf1)

# COMMAND ----------

psdf1

# COMMAND ----------

# MAGIC %md
# MAGIC To drop any rows that have missing data.

# COMMAND ----------

psdf1.dropna(how='any')

# COMMAND ----------

# MAGIC %md
# MAGIC Filling missing data.

# COMMAND ----------

psdf1.fillna(value=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operations

# COMMAND ----------

# Performing a descriptive statistic:
psdf.mean()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Spark Configurations
# MAGIC Various configurations in PySpark could be applied internally in pandas API on Spark. For example, you can enable Arrow optimization to hugely speed up internal pandas conversion. See also PySpark Usage Guide for Pandas with Apache Arrow in PySpark documentation.

# COMMAND ----------

prev = spark.conf.get("spark.sql.execution.arrow.pyspark.enabled")  # Keep its default value.
ps.set_option("compute.default_index_type", "distributed")  # Use default index prevent overhead.
import warnings
warnings.filterwarnings("ignore")  # Ignore warnings coming from Arrow optimizations.

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
%timeit ps.range(300000).to_pandas()

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", False)
%timeit ps.range(300000).to_pandas()

# COMMAND ----------

ps.reset_option("compute.default_index_type")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", prev)  # Set its default value back.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping
# MAGIC By “group by” we are referring to a process involving one or more of the following steps:
# MAGIC
# MAGIC     Splitting the data into groups based on some criteria
# MAGIC
# MAGIC     Applying a function to each group independently
# MAGIC
# MAGIC     Combining the results into a data structure
# MAGIC

# COMMAND ----------

psdf = ps.DataFrame({'A': ['foo', 'bar', 'foo', 'bar',
                          'foo', 'bar', 'foo', 'foo'],
                    'B': ['one', 'one', 'two', 'three',
                          'two', 'two', 'one', 'three'],
                    'C': np.random.randn(8),
                    'D': np.random.randn(8)})

psdf

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping and then applying the sum() function to the resulting groups.

# COMMAND ----------

psdf.groupby('A').sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping by multiple columns forms a hierarchical index, and again we can apply the sum function.

# COMMAND ----------

psdf.groupby(['A', 'B']).sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotting
# MAGIC

# COMMAND ----------

pser = pd.Series(np.random.randn(1000),
                 index=pd.date_range('1/1/2000', periods=1000))

# COMMAND ----------

psser = ps.Series(pser)

# COMMAND ----------

psser = psser.cummax()

# COMMAND ----------

psser.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC On a DataFrame, the plot() method is a convenience to plot all of the columns with labels:

# COMMAND ----------

pdf = pd.DataFrame(np.random.randn(1000, 4), index=pser.index,
                   columns=['A', 'B', 'C', 'D'])

# COMMAND ----------

psdf = ps.from_pandas(pdf)

# COMMAND ----------

psdf = psdf.cummax()

# COMMAND ----------

psdf.plot()

# COMMAND ----------

# MAGIC %md 
# MAGIC Plotting documentation:  
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#plotting  
