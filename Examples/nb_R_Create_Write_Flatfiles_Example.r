# Databricks notebook source
# MAGIC %md
# MAGIC ### R Flatfiles
# MAGIC This notebook demo's the usecase of creating and writing flatfiles in R.

# COMMAND ----------

# DBTITLE 1,params
# MAGIC %python
# MAGIC catalog_name = "hive_metastore"
# MAGIC schema_name = "test_schema"
# MAGIC volume_name = "test_volume"

# COMMAND ----------

# MAGIC %md
# MAGIC Create a volume to dump some example files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}; 
# MAGIC USE {catalog_name}.{schema_name};
# MAGIC CREATE VOLUME IF NOT EXISTS {schema_name}.{volume_name};

# COMMAND ----------

# MAGIC %md
# MAGIC ####CSV

# COMMAND ----------

# MAGIC %md
# MAGIC Create a dummy CSV file on the volume

# COMMAND ----------

# MAGIC %python 
# MAGIC import pandas as pd
# MAGIC import shutil
# MAGIC
# MAGIC # Create a dummy dataframe
# MAGIC data = {'Name': ['John', 'Jane', 'Alice', 'Bob'],
# MAGIC         'Age': [25, 30, 35, 40],
# MAGIC         'City': ['New York', 'Paris', 'London', 'Tokyo']}
# MAGIC df = pd.DataFrame(data)
# MAGIC
# MAGIC # Write the dataframe to a volume CSV file
# MAGIC df.to_csv(f'/Volumes/{catalog_name}/{schema_name}/{volume_name}/py_dummy_file.csv', index=False)
# MAGIC

# COMMAND ----------

library(tidyverse)

catalog_name <- "hive_metastore"
schema_name <- "test_schema"
volume_name <- "test_volume"

# Create a dummy data frame
data <- tibble(Name = c('John', 'Jane', 'Alice', 'Bob'),
               Age = c(25, 30, 35, 40),
               City = c('New York', 'Paris', 'London', 'Tokyo'))

# Construct the file path using variables
file_path <- paste0('/Volumes/', catalog_name, '/', schema_name, '/', volume_name, '/r_dummy_file.csv')

# Write the data frame to a CSV file using the constructed file path
write_csv(data, file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ####JSON

# COMMAND ----------

# Create a dummy JSON object
data <- list(
  Name = c('John', 'Jane', 'Alice', 'Bob'),
  Age = c(25, 30, 35, 40),
  City = c('New York', 'Paris', 'London', 'Tokyo')
)

# Convert the JSON object to a JSON string
json_string <- jsonlite::toJSON(data)

# Write the JSON string to a JSON file
file_path <- sprintf('/Volumes/%s/%s/%s/r_dummy_file.json', catalog_name, schema_name, volume_name)
writeLines(json_string, file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ####EXCEL

# COMMAND ----------

# install excel packages
install.packages("openxlsx")
install.packages("AzureStor")

# COMMAND ----------

library(openxlsx)
library(AzureStor)

# Generate some dummy data
dummy_data <- data.frame(
  Name = c("John", "Alice", "Bob"),
  Age = c(30, 25, 35),
  City = c("New York", "Los Angeles", "Chicago")
)

# Define the file path where the Excel file will be saved
excel_file_path <- paste0("/Volumes/", catalog_name, "/", schema_name, "/", volume_name, 'r_dummy_data.xlsx')

# Write the dummy data to an Excel file
write.xlsx(dummy_data, file = excel_file_path)

# Confirm that the file has been written successfully
if (file.exists(excel_file_path)) {
  cat("Excel file has been successfully written to the Databricks volume.")
} else {
  cat("Failed to write Excel file to the Databricks volume.")
}

# COMMAND ----------

# method 2
library(openxlsx)

# Create a dummy dataframe
df <- data.frame(
  Name = c("John", "Anna", "Peter", "Linda"),
  Age = c(23, 45, 34, 52),
  Occupation = c("Engineer", "Doctor", "Teacher", "Scientist")
)

# Create a new workbook
wb <- createWorkbook()

# Add the dataframe to the workbook
addWorksheet(wb, "Sheet 1")
writeData(wb, "Sheet 1", df)

excel_file_path <- paste0("/Volumes/", catalog_name, "/", schema_name, "/", volume_name, 'r_dummy_data2.xlsx')

# Save the workbook to an Excel file directly in DBFS
saveWorkbook(wb, excel_file_path, overwrite = TRUE)

# COMMAND ----------

# MAGIC %md
# MAGIC ####TXT

# COMMAND ----------


# Create dummy content
dummy_content <- "This is some dummy content for the text file."

txt_file_path <- paste0("/Volumes/", catalog_name, "/", schema_name, "/", volume_name, 'r_dummy.txt')

# Write the content to a text file
writeLines(dummy_content, txt_file_path)
