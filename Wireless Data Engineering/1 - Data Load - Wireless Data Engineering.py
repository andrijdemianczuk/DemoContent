# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Load
# MAGIC This notebook sources all of the data we're going to use in this demo and loads it into the initial working tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the data from the files into a dataframe

# COMMAND ----------

df = spark.read.format("csv") \
      .options(header='true', inferSchema='true') \
      .load("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's have a quick look at the schema to see if we need do any type casting. This is important since this will result in faster and easier data transformations later on.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Since this is the first exposure to the dataset, we can infer that some of these can be converted to primitive types right away. Here are some assumptions:
# MAGIC - zip: int
# MAGIC - latitude: double
# MAGIC - longitude: double

# COMMAND ----------

display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Later, we can look into conditioning some of the other fields too. Let's keep this in mind for our pre-processing stage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fixing the columns
# MAGIC So it looks like all of the columns (except the first one) have a leading space which is mucking things up. Let's go clean this up along with the initial types.

# COMMAND ----------

from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType,StringType,LongType
from pyspark.sql.functions import col

# COMMAND ----------

df = df.withColumnRenamed("first name", "first_name") \
      .withColumnRenamed(" last name", "last_name") \
      .withColumnRenamed(" address", "address") \
      .withColumnRenamed(" city", "city") \
      .withColumnRenamed(" county", "county") \
      .withColumnRenamed(" state", "state") \
      .withColumnRenamed(" zip", "zip") \
      .withColumnRenamed(" phone", "phone") \
      .withColumnRenamed(" carrier", "carrier") \
      .withColumnRenamed(" gender", "gender") \
      .withColumnRenamed(" ethnicity", "ethnicity") \
      .withColumnRenamed(" ownrent", "ownrent") \
      .withColumnRenamed(" latitude", "latitude") \
      .withColumnRenamed(" longitude", "longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC Now when we compare our schemas, we can see that we no longer have the preceding space.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Casting the known columns to primitive data types

# COMMAND ----------

df = df.withColumn("zip", col("zip").cast(IntegerType())) \
      .withColumn("latitude", col("latitude").cast(LongType())) \
      .withColumn("longitude", col("longitude").cast(LongType()))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll enumerate and split our tables out later for anything that we can group.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Commit the raw data to Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our initial dataframe, we need to commit it to Delta. There are two options:
# MAGIC 1. Managed: DBR will handle the storage location of the delta files
# MAGIC 1. Self-managed: The user chooses where the delta files will be stored

# COMMAND ----------

# DBTITLE 1,Managed
# tableName = 'b_wireless_data'
# sourceType = 'delta'

# df.write \
#   .format(sourceType) \
#   .saveAsTable(tableName)

# display(spark.sql("SELECT * FROM " + tableName))

# COMMAND ----------

# DBTITLE 1,Self-Managed
# Define the input and output formats and paths and the table name.
write_format = 'delta'
save_path = 'dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/wireless-delta'
table_name = 'ademianczuk.b_wireless_data'

# Create the table.
if spark._jsparkSession.catalog().tableExists('ademianczuk', 'b_wireless_data'):

  print("table already exists..... appending data")
  
  # An example of a simple append to an existing table. This is a good alternative, but we may want to UPSERT / MERGE
  #df.write.format("delta").mode("append").save(save_path_events)

  # Create a view and merge. Since this is a large table with no unique identifiers, this is only here for example.
#   df.createOrReplaceTempView("vw_wireless_data")  
#   spark.sql("MERGE INTO ademianczuk.b_wireless_data USING vw_wireless_data \
#     ON vw_wireless_data.first_name = ademianczuk.b_wireless_data.first_name \
#     AND vw_wireless_data.last_name = ademianczuk.b_wireless_data.last_name \
#     WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
  
else:
  print("table does not exist..... creating a new one")
  # Write the data to its target.
  df.write \
    .format(write_format) \
    .save(save_path)
  spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")
