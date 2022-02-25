# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregations
# MAGIC This notebook is intended to start building some aggregations and data simplification for use elsewhere. We'll focus on two key areas:
# MAGIC 1. Normalizing as much of the data as possible into smaller tables
# MAGIC 1. Enumerating data and hopefully create some lookup tables to go along with them

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load the most recent data

# COMMAND ----------

df = spark.table("ademianczuk.s_wireless_data_enriched")

# COMMAND ----------

df.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Planning the results
# MAGIC Before we start aggregating our data, we need to understand what we want the outcome to be and why.
# MAGIC 
# MAGIC 1. What - The 'gold' table will be an aggregation by state, city and municipality. We'll want:
# MAGIC   - A count of respondents
# MAGIC   - A count of distinct carriers
# MAGIC   - Average population
# MAGIC   - Average density
# MAGIC   - Although we could go further by examing standard deviation (from the norm), we'll keep our first use cases simple to see where we need to go.
# MAGIC 
# MAGIC 1. What - The enumerated indexes for the pertinent text fields
# MAGIC   - carrier
# MAGIC   - cState
# MAGIC   - city
# MAGIC   - county_name
# MAGIC   - The other text fields aren't really necessary so we will drop them altogether.

# COMMAND ----------

from pyspark.sql import functions as fn
from pyspark.sql.functions import col

df = df.groupBy(col("cState"), col("czip")).agg(fn.count("*").alias("resp_count"),
                                                fn.avg(col("population")).alias("pop_avg"),
                                                fn.avg(col("density")).alias("avg_density_ppsm"),
                                                fn.countDistinct(col("carrier")).alias("distinct_carriers"))

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Committing our first refined table (Gold)
# MAGIC Now that we have a general idea of the stats we want to report on, let's commit this to our final iteration of the delta table. Since these are small tables, we'll have more of these than we have silver tables, and considerably more than bronze. As new data comes in, these tables will need to be updated. This can typically be done either in a scheduled job with MERGE or via delta live tables (dlt)

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists('ademianczuk', 'g_wireless_data_by_zip'):
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
  df.write.partitionBy("cState").format("delta").save("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-by-zip")
  spark.sql(f"CREATE TABLE ademianczuk.g_wireless_data_by_zip USING DELTA LOCATION 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta-by-zip'")

# COMMAND ----------

spark.sql(f"OPTIMIZE ademianczuk.g_wireless_data_by_zip")
