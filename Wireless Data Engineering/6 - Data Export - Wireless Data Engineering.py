# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook we'll create some reporting metrics from our wireless data to be exported via adls to business users.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load the most recent data
# MAGIC Since we created the enriched delta tables in Notebook 3, we'll rely on the s_wireless_data_enriched data to build some reporting datasets for export.

# COMMAND ----------

df = spark.table("ademianczuk.s_wireless_data_enriched")

# COMMAND ----------

df.cache()
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the form widgets

# COMMAND ----------

# This will be used to populate the State dropdown. Widgets are limited to 1024 choices so we'll have to use some open text fields for matching
from pyspark.sql.functions import col
states = df.select(col("cState")).distinct().rdd.map(lambda row : row[0]).collect()
states.append("ALL")
states.sort()

# COMMAND ----------

dbutils.widgets.dropdown("State","ALL", [str(x) for x in states])
dbutils.widgets.text("City","")

# COMMAND ----------

self_State = dbutils.widgets.get("State")
self_City = dbutils.widgets.get("City")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build some simple reports

# COMMAND ----------

# MAGIC %md
# MAGIC We're going to build some simple reports off the loaded dataframe and the parameterized values

# COMMAND ----------

display(df.head(5))

# COMMAND ----------

df2 = df.groupBy("cState","city").count().orderBy(col("count").desc())
df2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write out to a data lake

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.adfieldengblog.dfs.core.windows.net", 
  dbutils.secrets.get(scope="fieldeng", key="ad_adls_storageblob"))

# COMMAND ----------

# Using GMT
from datetime import datetime, timezone
import pytz

localtz = pytz.timezone('Canada/Mountain')
dt = localtz.localize(datetime.now())

# COMMAND ----------

# Summary report
df2.coalesce(1).write.csv(f"abfss://wireless-services@adfieldengblob.dfs.core.windows.net/{dt.year}/{dt.month}/{dt.day}/{dt.hour}/{dt.minute}/{dt.second}/wireless_summary_by_city.csv", header='true')

# COMMAND ----------

from pyspark.sql import functions as fn
if self_State != "ALL":
  print("proceed")
  if self_City == "":
    print("no City")
    df2 = df.filter(col("cState") == self_State).groupBy("cState","city").count().orderBy(col("count").desc())
    display(df2)
    df2.coalesce(1).write.csv(f"abfss://wireless-services@adfieldengblob.dfs.core.windows.net/{dt.year}/{dt.month}/{dt.day}/{dt.hour}/{dt.minute}/{dt.second}/wireless_enrollment_{self_State}.csv", header='true')
  else:
    print(self_City)
    df2 = df.filter(col("cState") == self_State)
    df2 = df2.filter(col("city") == self_City).groupBy("cState","zip").count().orderBy(col("count").desc())
    display(df2)
    df2.coalesce(1).write.csv(f"abfss://wireless-services@adfieldengblob.dfs.core.windows.net/{dt.year}/{dt.month}/{dt.day}/{dt.hour}/{dt.minute}/{dt.second}/wireless_enrollment_{self_State}_{self_City}.csv", header='true')
else:
  print("halt")
