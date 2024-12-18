# Databricks notebook source
# MAGIC %md
# MAGIC ## Install/load R Packages
# MAGIC For R we want to use packages. There are multiple ways to install them.
# MAGIC
# MAGIC ### Install options:
# MAGIC 1 - Packages can be put on volume to easily load them without needing internet access.  
# MAGIC   1a - install from volume in source format   
# MAGIC   1b - install from volume in extracted package format  
# MAGIC 2 - install in notebook session  
# MAGIC   2a - using main install  
# MAGIC   2b - using devtools and specific internet repo  
# MAGIC   2c - using devtools and github  
# MAGIC 3 - install with init scripts of cluster meaning that it will be installed at every cluster start  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Init script setup:  
# MAGIC Type: Volume  
# MAGIC Location: /Volumes/ndp_consumable/00_system/init_scripts_r/copy_packages.sh  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load libs from volume

# COMMAND ----------

lib_path = "/Volumes/catalog_name/schema_name/volume_name/R/libs"

# COMMAND ----------

.libPaths(c(.libPaths(), lib_path))

# COMMAND ----------

library(plotly)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load the entire lib folder for all gz packages

# COMMAND ----------

# Specify the path to the folder containing the libraries
folder_path <- "/dbfs/Volumes/catalog_name/schema_name/volume_name/R/libs/"

# Get the list of files in the folder
library_files <- list.files(path = folder_path, pattern = "\\.tar.gz$", full.names = TRUE)

# Load each library using lapply
lapply(library_files, function(file) {
  install.packages(file, repos = NULL, type = "source")
  library(tools::file_path_sans_ext(basename(file)))
})

# COMMAND ----------

# MAGIC %md
# MAGIC ##### install main/popular R packages using easy install:

# COMMAND ----------

install.packages("janitor")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Manual install package from volume (if extracted):

# COMMAND ----------

# Specify the path to the package file on the volume
package_path <- "/Volumes/catalog_name/schema_name/volume_name/R/libs/janitor"

# Install the package from the specified path
install.packages(package_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Manual install packages from source:

# COMMAND ----------

# Specify the path to the package file on the volume
package_path <- "/Volumescatalog_name/schema_name/volume_name/R/libs/janitor_2.1.0.tar.gz"

# Install the package from the specified path
install.packages(package_path, repos = NULL, type = "source")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Install from specific internet repository (library will be available on drivers and workers)

# COMMAND ----------


require(devtools)

install_version(
  package = "janitor",
  repos   = "http://cran.us.r-project.org"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Install from github

# COMMAND ----------

require(devtools)
devtools::install_github("rstudio/rmarkdown")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Test installation of package

# COMMAND ----------

# remove.packages("vctrs")
install.packages("vctrs", version = "0.6.4")  # Update the vctrs package
install.packages("dplyr")
library(dplyr)
install.packages("janitor")
library(janitor)

# COMMAND ----------

# Create a data.frame with dirty names
test_df <- as.data.frame(matrix(ncol = 6))
names(test_df) <- c("firstName", "ábc@!*", "% successful (2009)",
                    "REPEAT VALUE", "REPEAT VALUE", "")

# COMMAND ----------

# Clean the variable names, returning a data.frame:
test_df %>%
  clean_names()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Erwin needed packages;   
# MAGIC janitor, plotly, sf, gt en gtExtras willen beschikken.

# COMMAND ----------

# remove.packages("vctrs")
install.packages("vctrs", version = "0.6.4")  # Update the vctrs package
install.packages("dplyr")
library(dplyr)

# COMMAND ----------

install.packages("janitor")

# COMMAND ----------

install.packages("plotly")

# COMMAND ----------

# Configuration failed to find the libv8 engine library. Try installing:
# * deb: libv8-dev or libnode-dev (Debian / Ubuntu)

# COMMAND ----------

# needed for gt
install.packages("V8")
install.packages("juicyjuice")


# COMMAND ----------

install.packages("gt") # werkt niet ivm missing deb

# COMMAND ----------

install.packages("gtExtras") # werkt niet ivm missing dep

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install -y libudunits2-dev

# COMMAND ----------

install.packages("units")

# COMMAND ----------

install.packages("sf") # werkt niet ivm missing dep

# COMMAND ----------

library(dplyr)
library(plotly)
library(janitor)
# library(sf)
# library(gt)
# library(gtExtras)
