# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import date
import io

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="harizadls-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.harizadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

today = date.today()

# COMMAND ----------

filename = f"equipment_bronze_output_{today}"

# COMMAND ----------

filename

# COMMAND ----------

bronze_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .parquet(f"abfss://bronzehariz@harizadls.dfs.core.windows.net/equipment/{filename}")
)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

bronze_df.show()

# COMMAND ----------

bronze_df.toPandas()  # Shows first 5 rows in pandas format

# COMMAND ----------

# Silver Stage
from pyspark.sql.functions import datediff, current_timestamp, when, col

def process_silver(bronze_df):
    # 1. Add equipment age column
    df = bronze_df.withColumn("equipment_age_days", 
        datediff(current_timestamp(), "installation_date"))
    
    # 2. Add equipment status based on thresholds
    df = df.withColumn("equipment_status",
        when((col("temperature") > 100) | (col("pressure") > 400) | 
             (col("vibration") > 8) | (col("flow_rate") > 1000), "WARNING")
        .otherwise("NORMAL"))
    
    # 3. Modified maintenance criteria to be more balanced
    df = df.withColumn("maintenance_needed",
        when(
            # High age AND (high temperature OR high vibration)
            (col("equipment_age_days") > 365) & 
            ((col("temperature") > 90) | (col("vibration") > 5)), "YES"
        )
        .when(
            # Critical conditions
            (col("temperature") > 95) & 
            (col("vibration") > 7) & 
            (col("pressure") > 350), "YES"
        )
        .when(
            # Equipment specific conditions
            (col("equipment_type") == "PUMP") & 
            (col("flow_rate") < 100), "YES"
        )
        .when(
            (col("equipment_type") == "HEAT EXCHANGER") & 
            (col("temperature") > 85), "YES"
        )
        .otherwise("NO"))

    return df

# Apply silver cleaning
silver_df = process_silver(bronze_df)

silver_df.toPandas()  # Shows first 5 rows in pandas format

# COMMAND ----------

today = date.today()
today

# COMMAND ----------

output_folder_path = f"abfss://silverhariz@harizadls.dfs.core.windows.net/equipment/equipment_silver_output_{today}"

# COMMAND ----------

print(output_folder_path)

# COMMAND ----------

silver_df.write.mode("overwrite").parquet(output_folder_path)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

