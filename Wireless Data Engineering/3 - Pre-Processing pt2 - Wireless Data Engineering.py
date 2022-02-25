# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook we'll do some binning and enumeration to create lookup tables to convert as much of our data to numeric types.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the latest version of the data

# COMMAND ----------

df = spark.table("ademianczuk.s_wireless_data_clean")

# COMMAND ----------

display(df.head(5))

# COMMAND ----------

# Let's just do a quick test to see if we can get a summary of the most popular carriers. This is to make sure our dataframe is performing well.
from pyspark.sql.functions import col
display(df.groupBy(col("carrier")).count().sort(col("count").desc()).head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Summary of values

# COMMAND ----------

# MAGIC %md
# MAGIC Let's get a quick summary of values. This will help us further classify our data and create some new tables with binning strategies for our partitions.

# COMMAND ----------

from pyspark.sql.functions import countDistinct

display(df.select([countDistinct(c).alias(c) for c in df.columns]))

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like we might still have some data quality issues; let's revisit this quickly. Let's get a summary of unique values in 'ownrent' and 'state'

# COMMAND ----------

display(df.select('state').distinct().collect())

# COMMAND ----------

# Load a list of state codes to cross-reference
df_states = spark.table("ademianczuk.l_state_codes")

# COMMAND ----------

# Now let's join them and see what we get
df = df.join(df_states,df.state == df_states.Code, "inner")

# COMMAND ----------

# Not bad for our attrition rate. Now that we have clean states, carriers and locations we can start doing our final enrichment.
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enriching our data to make it useful

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll enrich our data and cross-reference it with USPS-available census data. This data is available here: https://simplemaps.com/data/us-zips

# COMMAND ----------

# Let's do the same for zip codes since the supporting data is less than reliable
df_zips = spark.table("ademianczuk.l_us_zips")

# COMMAND ----------

# Trim the dataset to only the columns we need, and get rid of some more garbage
df = df.select(col("first_name").alias("fname"), 
               col("last_name").alias("lname"),
               col("address").alias("address"),
               col("zip").alias("zip"),
               col("phone").alias("phone"),
               col("carrier").alias("carrier"),
               col("ademianczuk.l_state_codes.State").alias("cState"),
               col("ademianczuk.l_state_codes.Code").alias("cCode"))

# COMMAND ----------

# There is one nested column that contains an illegal character (","). Since we don't need anything afterwad, let's trim those columns
df_zips = df_zips.select(col("zip").alias("czip"),col("lat"),col("lng").alias("lon"),col("city"),
                        col("population"),col("density"),col("county_fips"),col("county_name"))

# COMMAND ----------

# second stage of enrichment
df = df.join(df_zips,df.zip == df_zips.czip, "inner")

# COMMAND ----------

display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Committing the enriched data back to Delta Lake

# COMMAND ----------

# Now that we have our more complete dataset, let's commit that dataframe to a delta table with a partition.

if spark._jsparkSession.catalog().tableExists('ademianczuk', 's_wireless_data_enriched'):
  print("table already exists...... appending")
  
  # Full Overwrite
#   df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
#   df.write.format("delta").mode("overwrite").saveAsTable("default.people10m")
  
#   # Selective Overwrite
#   df.write \
#   .format("delta") \
#   .mode("overwrite") \
#   .option("replaceWhere", "start_date >= '2017-01-01' AND end_date <= '2017-01-31'") \
#   .save("/tmp/delta/events")
  
else:
  df.write.partitionBy("cCode").format("delta").save("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-enriched")
  spark.sql(f"CREATE TABLE ademianczuk.s_wireless_data_enriched USING DELTA LOCATION 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-enriched'")

# COMMAND ----------

spark.sql(f"OPTIMIZE ademianczuk.s_wireless_data_enriched")

# COMMAND ----------

# # Clean if necessary
# dbutils.fs.rm("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-enriched", True)
# spark.sql("DROP TABLE IF EXISTS ademianczuk.s_wireless_data_enriched")
