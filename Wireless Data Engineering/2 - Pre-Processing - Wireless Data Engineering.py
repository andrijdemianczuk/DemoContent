# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Pre-Processing
# MAGIC This notebooks starts working with and conditioning the data to get it ready for analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the latest version of the data

# COMMAND ----------

df = spark.table("ademianczuk.b_wireless_data")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Cache and commit
df.cache()
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Analyze and summarize the field data

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's get a count of all columns with null values, and how many nulls they contain

# COMMAND ----------

total_inc = df.count()

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col

df_na_summary = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

# COMMAND ----------

df_na_summary.cache()
df_na_summary.count()

# COMMAND ----------

display(df_na_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC Based on this infomration, we'll probably disregard ethnicity as a feature since the missing rate is so high. We'll also have a quick look at the drop rates for ownrent, latitude and longitude.

# COMMAND ----------

from pyspark.sql.functions import round
display(df_na_summary.withColumn("eth_drop", round(col("ethnicity")/total_inc*100,2))
       .withColumn("ownrent_drop", round(col("ownrent")/total_inc*100,2))
       .withColumn("lat_drop", round(col("latitude")/total_inc*100,2))
       .withColumn("lon_drop", round(col("longitude")/total_inc*100,2)).select("eth_drop", "ownrent_drop", "lat_drop", "lon_drop"))

# COMMAND ----------

# MAGIC %md
# MAGIC So own/rent might be better off with an N/A field to fill missing values since that's a valid response. Latitude and logitude should only be dropped if no zip code is provided. For instances that do have a zip code, but no lat/lon then we can infer that the zip center is the location - we can use an external lookup for this.
# MAGIC 
# MAGIC The key here, is to preserve as much data as we can.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fixing the missing data

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's get rid of ethinicity since it just adds an un-necessary and expensive dimension

# COMMAND ----------

df = df.drop(col("ethnicity"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's fix the lat/lon missing values

# COMMAND ----------

display(df.where(col("zip").isNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC Right, so at first glance this looks like a lot of bad data - This can be classified as noise and likely caused by format entry errors which happens from time to time. Earlier, we looked at our missing data rate for lat / lon. Since we're sitting below 5% (at roughly 2.32%) that falls well within the means of statistically insignificant. Later, we may want to come back to process this data so we'll create a couple of derivative tables from this.

# COMMAND ----------

#drop anything without a zip code
df = df.filter(col("zip").isNotNull())

# COMMAND ----------

df.filter((col("latitude").isNull()) & (col("longitude").isNull())).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Great, so although we got rid of the garbage instances, we now have a somewhat clean dataset to work with. We need to make some decisions as to how we want to proceed. We know that there are 999,997 records that don't have a coordinate. We do have a zip code however, so we can do one of three things:
# MAGIC 1. Create a lat/lon value at the location of the zip code. This will improve fidelty but makes a big assumption
# MAGIC 1. Drop them entirely. This improves accuracy and since we're only ditching ~2.2% of records it's not statistically significant
# MAGIC 1. Create two tables; one with the lat/lon values imputed from zip code, and one without.
# MAGIC 
# MAGIC Since we may want to re-use this data later, we'll opt for option 3.

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS ademianczuk.s_wireless_data_full")
# dbutils.fs.rm("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-full", True)

# COMMAND ----------

# Define the input and output formats and paths and the table name.
write_format = 'delta'
save_path = 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-full'
table_name = 'ademianczuk.s_wireless_data_full'

# Create the table.
if spark._jsparkSession.catalog().tableExists('ademianczuk', 's_wireless_data_full'):
  print("table already exists..... appending data")
else:
  print("table does not exist..... creating a new one")
  # Write the data to its target.
  df.write \
    .format(write_format) \
    .save(save_path)
  spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# MAGIC %md
# MAGIC So let's have a look now at our missing values count. The rest of our values look fairly benign so we can fill them with replacements. We know that the only columns left are all String columns, so we'll go and replace the null values with 'unknown'

# COMMAND ----------

# Let's get rid of the null coordinates. This leaves us with 42M records to analyze
df = df.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

# COMMAND ----------

df = df.na.fill(value="unknown")

# COMMAND ----------

display(df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])) 

# COMMAND ----------

# MAGIC %md
# MAGIC Since 'NaN' is a valid string (e.g., this could be someone's first or last name, or a bogus city for example) we can't necessarily dismiss this data.

# COMMAND ----------

display(df.filter(isnan(col("city"))))

# COMMAND ----------

# Define the input and output formats and paths and the table name.
write_format = 'delta'
save_path = 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-clean'
table_name = 'ademianczuk.s_wireless_data_clean'

# Create the table.
if spark._jsparkSession.catalog().tableExists('ademianczuk', 's_wireless_data_clean'):
  print("table already exists..... appending data")
else:
  print("table does not exist..... creating a new one")
  # Write the data to its target.
  df.write \
    .format(write_format) \
    .save(save_path)
  spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# MAGIC %md
# MAGIC That should cover things for this section. Now we have a clean and fully populated dataset to work off of, so we can start re-coding some data to create lookup tables and shrink our footprint.
