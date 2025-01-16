# Databricks notebook source
# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from datetime import date

# COMMAND ----------

# Get today's date
today = date.today()

# Get Azure Storage credentials
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="harizadls-key")

# Configure Azure Storage
spark.conf.set("fs.azure.account.key.harizadls.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Define correct schema matching your data
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("functional_location", StringType(), True),
    StructField("equipment_type", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("installation_date", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("flow_rate", DoubleType(), True),
    StructField("last_maintenance_date", TimestampType(), True),
    StructField("maintenance_type", StringType(), True),
    StructField("days_since_last_maintenance", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("equipment_age", IntegerType(), True),
    StructField("efficiency", DoubleType(), True),
    StructField("anomaly_score", DoubleType(), True),
    StructField("equipment_age_days", IntegerType(), True),
    StructField("equipment_status", StringType(), False),
    StructField("maintenance_needed", StringType(), False)
])

# COMMAND ----------

# Read silver data with schema
silver_df = (spark.read
    .option("header", "true")
    .schema(schema)  # Apply the defined schema
    .parquet(f"abfss://silverhariz@harizadls.dfs.core.windows.net/equipment/equipment_silver_output_{today}")
)


# COMMAND ----------

# Show sample data and shape
print("Sample data:")
silver_df.limit(5).toPandas()
print(f"\nShape: {(silver_df.count(), len(silver_df.columns))}")

# COMMAND ----------

# Get Snowflake credentials
snowflake_pw = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="snowflake-hariz")


# COMMAND ----------

# Configure Snowflake connection
options = {
    'sfUrl': 'kb47967.southeast-asia.azure.snowflakecomputing.com',
    'sfUser': 'HARIZPAR4',
    'sfPassword': snowflake_pw,
    'sfDatabase': 'EQUIPMENT_DB',
    'sfSchema': 'SILVER_SCHEMA',
    'sfWarehouse': 'COMPUTE_WH'
}


# COMMAND ----------

# Write to Snowflake
try:
    silver_df.write \
        .format("snowflake") \
        .options(**options) \
        .option("dbtable", "EQUIPMENT_SILVER") \
        .mode("overwrite") \
        .save()
    print("\n✓ Data successfully written to Snowflake")
except Exception as e:
    print(f"\nError writing to Snowflake: {str(e)}")
    raise

# COMMAND ----------

