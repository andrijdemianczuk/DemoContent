# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/06/MLflow-logo-pos-TM-1.png" width=200/>
# MAGIC 
# MAGIC # ML Flow Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction
# MAGIC 
# MAGIC Managed MLflow is built on top of MLflow, an open source platform developed by Databricks to help manage the complete machine learning lifecycle with enterprise reliability, security and scale.
# MAGIC 
# MAGIC Managed ML Flow is Databricks' implementation of open source ML Flow with a few key benefits including:
# MAGIC 1. Experiment tracking
# MAGIC 1. Model management
# MAGIC 1. Model deployment
# MAGIC 
# MAGIC ML Flow is one of the key components of ML Ops in tandem with Git Repositories and Delta Lake foundations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC The setup section covers everything we need to get up and running. Here we'll create the new catalog in the hive metastore, define a file location where we can reference and store files and always have a common location to work which is unique for every project.

# COMMAND ----------

#Imports
import subprocess
from datetime import datetime, timezone
import pytz

# COMMAND ----------

# Using GMT, set the local timezone for the notebook and default
localtz = pytz.timezone('Canada/Mountain')
dt = localtz.localize(datetime.now())

# COMMAND ----------

#Create some widgets to make parametrization a bit easier. This will make the notebook a bit more flexible.
dbutils.widgets.text("Database", "ademianczuk");
dbutils.widgets.dropdown("Timezone","Canada/Central", [str(tz) for tz in pytz.all_timezones])

# COMMAND ----------

#Set our database where we'll be building our Delta tables
database = dbutils.widgets.get("Database")

# COMMAND ----------

#Get the location of our source data
dbutils.fs.ls("FileStore/Users/andrij.demianczuk@databricks.com/")

# COMMAND ----------

# Construct the unique path to be used to store files on the local file system
local_data_path = "/dbfs/FileStore/Users/andrij.demianczuk@databricks.com/downloads" #This location should already exist. If not, it will need to be created.
print(f"Path to be used for Local Files: {local_data_path}")

# COMMAND ----------

#Create (if not already exists) and use the database defined in the widget
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------

#Let's do a quick check to make sure we can see the tables in the database context
display(spark.sql("SHOW TABLES;"))

# COMMAND ----------

#Double check that we have purged the location of the local data path. If this doesn't exist, it will return False. The function it will recurse (True is set).
dbutils.fs.rm(local_data_path[5:], True)

# COMMAND ----------

# Create local directories used in the workshop

process = subprocess.Popen(['mkdir', '-p', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)

stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# Download Initial CSV file used in the workshop

process = subprocess.Popen(['wget', '-P', local_data_path, 'https://www.dropbox.com/s/vuaq3vkbzv8fgml/sensor_readings_current_labeled_v4.csv'],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)

stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

#Confirm the file was successfully downloaded
dbutils.fs.ls(local_data_path[5:])

# COMMAND ----------

#This is a turning point - we'll start by creating an initial dataframe for any pyspark functionality we require. In this example, it will simply be transient to a materialized view.
df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(local_data_path[5:])

display(df)

# COMMAND ----------

#Next, let's create the materialization. This will allow us to use SQL functions on a dataframe-type object
df.createOrReplaceTempView("mlflow_input_vw")

# COMMAND ----------


