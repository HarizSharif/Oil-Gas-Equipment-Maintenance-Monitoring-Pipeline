# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Equipment Gold Processing to Snowflake Loader

# COMMAND ----------
# Import required libraries
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from datetime import date

today = date.today()

# COMMAND ----------
# Get Azure Storage credentials from secrets
storage_account_key = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="harizadls-key")

# Configure Azure Storage access
spark.conf.set(
    "fs.azure.account.key.harizadls.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------


# COMMAND ----------
# Get Snowflake credentials from secrets
snowflake_pw = dbutils.secrets.get(scope="kotak-sakti-scope-111", key="snowflake-hariz")

# Configure Snowflake connection - with corrected URL format and connection settings
options = {
    'sfUrl': 'https://kb47967.southeast-asia.azure.snowflakecomputing.com',  # Removed 'https://'
    'sfUser': 'HARIZPAR4',
    'sfPassword': snowflake_pw,
    'sfDatabase': 'EQUIPMENT_DB',
    'sfSchema': 'GOLD_SCHEMA',
    'sfWarehouse': 'COMPUTE_WH',
    'application': 'Databricks_Gold_Loader',  # Added application name
    'authenticator': 'snowflake',  # Explicit authenticator
    'connection_timeout': 240  # Increased timeout
}

# COMMAND ----------

def initialize_snowflake():
    """Initialize Snowflake environment step by step"""
    try:
        # 1. First connection without database/schema
        spark.read \
            .format("snowflake") \
            .options(**options) \
            .option("query", "USE WAREHOUSE COMPUTE_WH") \
            .load()
            
        # 2. Create and use database
        spark.read \
            .format("snowflake") \
            .options(**options) \
            .option("query", "CREATE DATABASE IF NOT EXISTS EQUIPMENT_DB") \
            .load()
            
        spark.read \
            .format("snowflake") \
            .options(**options) \
            .option("query", "USE DATABASE EQUIPMENT_DB") \
            .load()
            
        # 3. Create and use schema
        spark.read \
            .format("snowflake") \
            .options(**options) \
            .option("query", "CREATE SCHEMA IF NOT EXISTS GOLD_SCHEMA") \
            .load()
            
        spark.read \
            .format("snowflake") \
            .options(**options) \
            .option("query", "USE SCHEMA GOLD_SCHEMA") \
            .load()
            
        print("✓ Snowflake environment initialized successfully")
        
        # 4. Update options with database and schema for subsequent operations
        global options
        options.update({
            'sfDatabase': 'EQUIPMENT_DB',
            'sfSchema': 'GOLD_SCHEMA'
        })
        
        return True
        
    except Exception as e:
        print(f"Error initializing Snowflake: {str(e)}")
        return False


# COMMAND ----------

def read_gold_data():
    """Read gold layer data from Azure Data Lake"""
    try:
        base_path = f"abfss://goldhariz@harizadls.dfs.core.windows.net/equipment/equipment_gold_output_{str(today)}"
        
        dataframes = {
            'dim_location': spark.read.parquet(f"{base_path}/dim_location/"),
            'dim_equipment': spark.read.parquet(f"{base_path}/dim_equipment/"),
            'dim_date': spark.read.parquet(f"{base_path}/dim_date/"),
            'fact_measurements': spark.read.parquet(f"{base_path}/fact_measurements/"),
            'equipment_bridge': spark.read.parquet(f"{base_path}/equipment_bridge/")
        }
        
        for table, df in dataframes.items():
            print(f"✓ Loaded {table}: {df.count()} rows")
            
        return dataframes
    except Exception as e:
        print(f"Error reading data: {str(e)}")
        raise

# COMMAND ----------

def write_to_snowflake(dataframes):
    """Write dataframes to Snowflake tables"""
    try:
        # First ensure we're in the right database and schema
        spark.read \
            .format("snowflake") \
            .options(**options) \
            .option("query", """
                USE DATABASE EQUIPMENT_DB;
                USE SCHEMA GOLD_SCHEMA;
            """) \
            .load()
        
        for table_name, df in dataframes.items():
            print(f"Loading {table_name}...")
            
            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f"{field.name} {field.dataType.simpleString()}" 
                           for field in df.schema.fields])}
            )
            """
            
            spark.read \
                .format("snowflake") \
                .options(**options) \
                .option("query", create_table_query) \
                .load()
            
            # Write data
            df.write \
                .format("snowflake") \
                .options(**options) \
                .option("dbtable", table_name) \
                .mode("overwrite") \
                .save()
            
            print(f"✓ {table_name} loaded successfully")
            
    except Exception as e:
        print(f"Error writing to Snowflake: {str(e)}")
        raise

# COMMAND ----------

# Main execution
try:
    print("Initializing Snowflake environment...")
    if initialize_snowflake():
        print("\nReading data from Azure Data Lake...")
        gold_dataframes = read_gold_data()
        
        print("\nWriting data to Snowflake...")
        write_to_snowflake(gold_dataframes)
        
        print("\n✓ All operations completed successfully")
    else:
        raise Exception("Failed to initialize Snowflake environment")
        
except Exception as e:
    print(f"\n× Error in processing: {str(e)}")
    raise

# COMMAND ----------


# COMMAND ----------
# Test Snowflake connection before proceeding
def test_snowflake_connection():
    """Test Snowflake connection before main processing"""
    try:
        test_df = spark.read \
            .format("snowflake") \
            .options(**options) \
            .option("query", "SELECT CURRENT_USER(), CURRENT_ROLE()") \
            .load()
        
        test_df.show()
        print("✓ Snowflake connection test successful")
        return True
    except Exception as e:
        print(f"× Snowflake connection test failed: {str(e)}")
        return False


# COMMAND ----------

# COMMAND ----------
# Read gold layer data with error handling
def read_gold_data():
    """Read gold layer data from Azure Data Lake Storage"""
    try:
        base_path = f"abfss://goldhariz@harizadls.dfs.core.windows.net/equipment/equipment_gold_output_{str(today)}"
        
        tables = {
            'dim_location': f"{base_path}/dim_location/",
            'dim_equipment': f"{base_path}/dim_equipment/",
            'dim_date': f"{base_path}/dim_date/",
            'fact_measurements': f"{base_path}/fact_measurements/",
            'equipment_bridge': f"{base_path}/equipment_bridge/"
        }
        
        dataframes = {}
        for table_name, path in tables.items():
            print(f"Reading {table_name}...")
            dataframes[table_name] = spark.read.parquet(path)
            print(f"✓ {table_name} loaded successfully")
            
        return dataframes
    except Exception as e:
        print(f"Error reading gold data: {str(e)}")
        raise

# COMMAND ----------

# COMMAND ----------
# Main execution with connection test
try:
    # First test the connection
    if test_snowflake_connection():
        print("\nReading data from Azure Data Lake...")
        gold_dataframes = read_gold_data()
        
        print("\nWriting data to Snowflake...")
        write_to_snowflake(gold_dataframes)
        
        print("\nVerifying data loads...")
        verify_loads()
        
        print("\n✓ Process completed successfully")
    else:
        raise Exception("Snowflake connection test failed")
        
except Exception as e:
    print(f"\n× Error in processing: {str(e)}")
    raise