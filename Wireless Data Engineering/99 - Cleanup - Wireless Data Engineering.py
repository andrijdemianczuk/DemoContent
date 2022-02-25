# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ademianczuk.b_wireless_data

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta", True)
