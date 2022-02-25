# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC # Enumeration
# MAGIC Enumeration is a good idea for two reasons!
# MAGIC 1. It allows us to process data at a much larger scale
# MAGIC 1. It gets data ready for ML & AI
# MAGIC 
# MAGIC This is important, and since we're not stepping on the toes of aggregation here, this is a good example of something that can be run tandem / independtly of the aggregations notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Load the latest copy of the data
# MAGIC This will be a different fork of the silver (enriched) data

# COMMAND ----------

df = spark.table("ademianczuk.s_wireless_data_enriched")

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prune the fields we won't be using

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as fn
from pyspark.sql.functions import col
df = df.select(col("zip"), col("phone"), col("carrier"), col("cState"), col("czip"), 
               col("lat"), col("lon"), col("city"), col("county_name"), col("population"), col("density"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enumerate!
# MAGIC Enumaration is the process of applying a numeric value to a string based on it's uniqueness. Think of it as re-coding anything unique about a field value as a number to make it easier and faster to process.

# COMMAND ----------

# MAGIC %md
# MAGIC This seems like a good start - next let's build a list of columns that are strings which we'll need to enumerate

# COMMAND ----------

# Let's build an array of the string column names. This will make it easier to iterate through
stringColList = [j[0] for j in df.dtypes if j[1] == 'string']
print(stringColList)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll have 5 fields we'll need to enumerate. Let's just quickly verify our candidacy for enumeration. We'll create a quick function that accepts a list of column names and the root dataframe and gets a distinct list of values for string columns only.

# COMMAND ----------

from pyspark.sql.functions import *

# Create a function that performs a countDistinct(colName)
def countDistinctCats(colNames, df):
  distinctList = []
  for colName in colNames:
    count = df.agg(countDistinct(colName)).collect()
    distinctList.append(count)
  return distinctList

# COMMAND ----------

print(countDistinctCats(stringColList, df))

# COMMAND ----------

# MAGIC %md
# MAGIC So this is good - maybe phone number isn't the best candidate for enumeration. Based on the nubmers though seems like a good unique identifier. Let's ignore the phone number and only iterate the rest.

# COMMAND ----------

# Remove 'phone' from the list of columns we want to enumerate
stringColList.remove('phone')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Field Enumeration
# MAGIC 
# MAGIC Before we do our final write to our gold table from our Delta files, we need to consider which fields we're going to want to report on. This is important because we'll need to decide how we want to chunk up our data into metrics and dimensions. Metrics themselves by nature should already be in some type of numeric field, but the dimensions that we'll want to report on ought to have enumeration values attributed - this will make it possible to use those fields for predictive reporting and ML down the road.
# MAGIC 
# MAGIC | Dataframe | Enumeration Fields |
# MAGIC | ------ | ----------- |
# MAGIC | df | carrier<br/>cState<br/>city<br/>county_name|

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

transformedCols = [categoricalCol + "_Index" for categoricalCol in stringColList]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "_Index",handleInvalid = "keep") for categoricalCol in stringColList]
stages

# COMMAND ----------

from pyspark.ml import Pipeline

indexer = Pipeline(stages=stages)
df_indexed = indexer.fit(df).transform(df)
display(df_indexed)

# COMMAND ----------

# MAGIC %md
# MAGIC Right on, now we see that we have indexed columns as integers to represent each unique value of our four identified string columns.

# COMMAND ----------

# MAGIC %md
# MAGIC The last thing we need to do is create lookup tables for the enumerated columns so we can piece this back together later, and store a final copy of the pruned dataset in Delta.

# COMMAND ----------

carrier_df = df_indexed.select(col("carrier"), col("carrier_Index")).distinct().orderBy(col("carrier_Index").asc())
cState_df = df_indexed.select(col("cState"), col("cState_Index")).distinct().orderBy(col("cState_Index").asc())
city_df = df_indexed.select(col("city"), col("city_Index")).distinct().orderBy(col("city_Index").asc())
county_name_df = df_indexed.select(col("county_name"), col("county_name_Index")).distinct().orderBy(col("county_name_Index").asc())

# COMMAND ----------

# Write the carrier lookup to delta
if spark._jsparkSession.catalog().tableExists('ademianczuk', 'l_wireless_carrier'):
  print("table already exists...... appending") 
else:
  carrier_df.write.format("delta").save("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-carrier")
  spark.sql(f"CREATE TABLE ademianczuk.l_wireless_carrier USING DELTA LOCATION 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-carrier'")

# COMMAND ----------

# Write the cState lookup to delta
if spark._jsparkSession.catalog().tableExists('ademianczuk', 'l_wireless_cState'):
  print("table already exists...... appending") 
else:
  cState_df.write.format("delta").save("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-cState")
  spark.sql(f"CREATE TABLE ademianczuk.l_wireless_cState USING DELTA LOCATION 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-cState'")

# COMMAND ----------

# Write the city lookup to delta
if spark._jsparkSession.catalog().tableExists('ademianczuk', 'l_wireless_city'):
  print("table already exists...... appending") 
else:
  city_df.write.format("delta").save("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-city")
  spark.sql(f"CREATE TABLE ademianczuk.l_wireless_city USING DELTA LOCATION 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-city'")

# COMMAND ----------

# Write the county lookup to delta
if spark._jsparkSession.catalog().tableExists('ademianczuk', 'l_wireless_county_name'):
  print("table already exists...... appending") 
else:
  county_name_df.write.format("delta").save("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-county-name")
  spark.sql(f"CREATE TABLE ademianczuk.l_wireless_county_name USING DELTA LOCATION 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-lookup-county-name'")

# COMMAND ----------

display(df_indexed.head(5))

# COMMAND ----------

df = df_indexed.select(col("czip"), col("phone"), col("lat"), col("lon"), col("population"), col("density"), col("carrier_Index"), col("cState_Index"), col("city_index"), col("county_name_index"))

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists('ademianczuk', 'g_wireless_data_enum'):
  print("table already exists...... appending")
else:
  df.write.partitionBy("cState_Index").format("delta").save("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-enum")
  spark.sql(f"CREATE TABLE ademianczuk.g_wireless_data_enum USING DELTA LOCATION 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-enum'")
