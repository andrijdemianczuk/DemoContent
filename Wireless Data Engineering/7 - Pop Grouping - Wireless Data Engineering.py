# Databricks notebook source
# MAGIC %md
# MAGIC # Wireless Data Analytics Demo
# MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/images/Spectrum_Regulator_Industry_2.jpeg" width=1200/>
# MAGIC 
# MAGIC This demo set of notebooks demonstrates how to process a wireless communications dataset. We will be ingeesting data from several csv files, normalizing the data and storing it in Delta Lake. After some processing, the formatted data will be exported to ADLS for business consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Instructions:
# MAGIC 1. To best re-create the training environment, use the same cluster runtime and version from training.
# MAGIC 2. Add additional data processing on your loaded table to match the model schema if necessary (see the "Define input and output" section below).
# MAGIC 3. "Run All" the notebook.
# MAGIC 4. Note: If the `%pip` does not work for your model (i.e. it does not have a `requirements.txt` file logged), modify to use `%conda` if possible.

# COMMAND ----------

model_name = "ad-carrier-idx-reg"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Recreation
# MAGIC To best re-create the training environment, use the same cluster runtime and version from training.. The cell below downloads the model artifacts associated with your model in the remote registry, which include `conda.yaml` and `requirements.txt` files. In this notebook, `pip` is used to reinstall dependencies by default.
# MAGIC 
# MAGIC ### (Optional) Conda Instructions
# MAGIC Models logged with an MLflow client version earlier than 1.18.0 do not have a `requirements.txt` file. If you are using a Databricks ML runtime (versions 7.4-8.x), you can replace the `pip install` command below with the following lines to recreate your environment using `%conda` instead of `%pip`.
# MAGIC ```
# MAGIC conda_yml = os.path.join(local_path, "conda.yaml")
# MAGIC %conda env update -f $conda_yml
# MAGIC ```

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os

model_uri = f"models:/{model_name}/2"
local_path = ModelsArtifactRepository(model_uri).download_artifacts("") # download model from remote registry

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define input and output
# MAGIC The table path assigned to`input_table_name` will be used for batch inference and the predictions will be saved to `output_table_path`. After the table has been loaded, you can perform additional data processing, such as renaming or removing columns, to ensure the model and table schema matches.

# COMMAND ----------

# redefining key variables here because %pip and %conda restarts the Python interpreter
model_name = "ad-carrier-idx-reg"
input_table_name = "ademianczuk.g_wireless_data_by_zip"
output_table_path = "/FileStore/batch-inference/ad-carrier-idx-reg"

# COMMAND ----------

# load table as a Spark DataFrame
table = spark.table(input_table_name)

# optionally, perform additional data processing (may be necessary to conform the schema)


# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct

model_uri = f"models:/{model_name}/2"

# create spark user-defined function for model prediction
predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double")

# COMMAND ----------

output_df = table.withColumn("prediction", predict(struct(*table.columns)))

# COMMAND ----------

# MAGIC %md ## Save predictions
# MAGIC **The default output path on DBFS is accessible to everyone in this Workspace. If you want to limit access to the output you must change the path to a protected location.**
# MAGIC The cell below will save the output table to the specified FileStore path. `datetime.now()` is appended to the path to prevent overwriting the table in the event that this notebook is run in a batch inference job. To overwrite existing tables at the path, replace the cell below with:
# MAGIC ```python
# MAGIC output_df.write.mode("overwrite").save(output_table_path)
# MAGIC ```

# COMMAND ----------

from datetime import datetime

output_df.write.save(f"{output_table_path}_{datetime.now().isoformat()}".replace(":", "."))

# COMMAND ----------

output_df.display()

# COMMAND ----------

# Top ten states with the greatest likelihood of distinct carrier support
from pyspark.sql import functions as fn
display(output_df.groupBy("cState").agg(fn.avg("prediction").alias("pred")).orderBy(fn.col("pred").desc()).head(10))

# COMMAND ----------


