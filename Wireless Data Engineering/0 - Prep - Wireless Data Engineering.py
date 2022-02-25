# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC ## About this application
# MAGIC This project is split up into several notebooks for reusability and orchestration purposes
# MAGIC 
# MAGIC ### Outline
# MAGIC * 0 - Prep: This notebook - used to set up the environment. This won't have to be orchestrated since it will be a run-once notebook
# MAGIC * 1 - Data Load: Read and parse the source files and store in the landing table
# MAGIC * 2 - Pre-processing: First-run analysis and munging where necessary. Output will be stored in clean tables
# MAGIC * 3 - Classification: Aggregations and truncation
# MAGIC * 4 - Predictions: Basic ML off the classifications
# MAGIC * 5 - Data Export: Pretty much what it sounds like

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's just have a quick look to see where our files are located. The original data can be found here: https://www.kaggle.com/bbutler/wireless-usa-dataset-b

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ademianczuk;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Make a directory to store our delta tables.

# COMMAND ----------

#dbutils.fs.mkdirs("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta")

# COMMAND ----------

# MAGIC %md
# MAGIC Set the flags to auto-optimize

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")
