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

dbutils.secrets.list('kotak-sakti-scope-111')

# COMMAND ----------

storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="harizadls-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.harizadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("abfss://incoming@harizadls.dfs.core.windows.net/wide_fact_table_equipment_data.csv"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.toPandas()  # Shows first 5 rows in pandas format

# COMMAND ----------

# Bronze Stage
from pyspark.sql.functions import col, to_timestamp, when, current_timestamp

def process_bronze(df):
    # 1. Convert timestamp to proper format
    df = df.withColumn("timestamp", to_timestamp("timestamp"))
    df = df.withColumn("installation_date", to_timestamp("installation_date"))
    
    # 2. Remove duplicates
    df = df.dropDuplicates()
    
    # 3. Remove null values
    df = df.na.drop()
    
    # 4. Convert numeric columns to proper type
    numeric_columns = ["temperature", "pressure", "vibration", "flow_rate"]
    for col_name in numeric_columns:
        df = df.withColumn(col_name, col(col_name).cast("double"))
    
    # 5. Convert string columns to uppercase for consistency
    string_columns = ["functional_location", "equipment_type", "manufacturer", "country", "region"]
    for col_name in string_columns:
        df = df.withColumn(col_name, upper(col(col_name)))
    
    return df

# Apply bronze cleaning
bronze_df = process_bronze(df)
bronze_df.toPandas()

# COMMAND ----------

today = date.today()
today

# COMMAND ----------

output_folder_path = f"abfss://bronzehariz@harizadls.dfs.core.windows.net/equipment/equipment_bronze_output_{today}"

# COMMAND ----------

df.write.mode("overwrite").parquet(output_folder_path)

# COMMAND ----------

