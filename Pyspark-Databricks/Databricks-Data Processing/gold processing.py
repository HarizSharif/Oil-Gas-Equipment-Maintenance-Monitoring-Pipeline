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

filename = f"equipment_silver_output_{today}"

# COMMAND ----------

filename

# COMMAND ----------

silver_df = (
    spark.read
    .option("mergeSchema", "true")
    .parquet("abfss://silverhariz@harizadls.dfs.core.windows.net/equipment/equipment_silver_output_2024-10-22")
)
display(silver_df)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.show()

# COMMAND ----------

silver_df.toPandas()  # Shows first 5 rows in pandas format

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, date_format, col, when, datediff, current_timestamp, to_date
from pyspark.sql.types import TimestampType

# Set legacy time parser policy (add this before processing)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

def process_gold(silver_df):
    try:
        # 1. Create Location Dimension
        dim_location = silver_df.select(
            "country",
            "region"
        ).distinct().withColumn(
            "location_id", 
            monotonically_increasing_id()
        )
        
        # 2. Create Equipment Dimension
        dim_equipment = silver_df.select(
            "equipment_type",
            "manufacturer"
        ).distinct().withColumn(
            "equipment_id",
            monotonically_increasing_id()
        )
        
        # 3. Create Equipment Bridge
        equipment_bridge = silver_df.select(
            "functional_location",
            "equipment_type",
            "manufacturer",
            "installation_date"
        ).dropDuplicates()
        
        equipment_bridge = equipment_bridge.join(
            dim_equipment,
            ["equipment_type", "manufacturer"],
            "left"
        ).select(
            "functional_location",
            "equipment_id",
            "installation_date"
        )
        
        # 4. Create Date Dimension with safe date parsing
        dim_date = silver_df.select(
            col("last_maintenance_date").cast("date").alias("date"),
            year("last_maintenance_date").alias("year"),
            month("last_maintenance_date").alias("month"),
            dayofmonth("last_maintenance_date").alias("day"),
            quarter("last_maintenance_date").alias("quarter"),
            date_format("last_maintenance_date", "MMMM").alias("month_name"),
            date_format("last_maintenance_date", "E").alias("day_of_week"),
            dayofweek("last_maintenance_date").alias("day_of_week_number"),
            dayofyear("last_maintenance_date").alias("day_of_year")
        ).dropDuplicates()
        
        # Add fiscal periods
        dim_date = dim_date.withColumn(
            "fiscal_year",
            when(col("month") >= 4, col("year"))
            .otherwise(col("year") - 1)
        ).withColumn(
            "fiscal_quarter",
            when(col("month") >= 4, (col("month") - 4) / 3 + 1)
            .otherwise((col("month") + 8) / 3)
        )
        
        # 5. Join location_id to main data
        df_with_location = silver_df.join(
            dim_location,
            ["country", "region"],
            "left"
        )
        
        # 6. Create Fact Table with equipment_id
        fact_measurements = df_with_location.join(
            equipment_bridge,
            ["functional_location"],
            "left"
        ).select(
            monotonically_increasing_id().alias("measurement_id"),
            "functional_location",
            "equipment_id",  # Added equipment_id
            "location_id",
            "temperature",
            "pressure", 
            "vibration",
            "flow_rate",
            "equipment_age_days",
            "equipment_status",
            "maintenance_needed",
            "last_maintenance_date"
        )
        
        # Print verification
        print("\nVerifying table counts:")
        print(f"Location Dimension: {dim_location.count()} rows")
        print(f"Equipment Dimension: {dim_equipment.count()} rows")
        print(f"Equipment Bridge: {equipment_bridge.count()} rows")
        print(f"Date Dimension: {dim_date.count()} rows")
        print(f"Fact Measurements: {fact_measurements.count()} rows")
        
        return fact_measurements, dim_equipment, equipment_bridge, dim_location, dim_date
        
    except Exception as e:
        print(f"Error in process_gold: {str(e)}")
        raise

# Execute the process
fact_measurements, dim_equipment, equipment_bridge, dim_location, dim_date = process_gold(silver_df)

# Show samples
print("\nEquipment Bridge Sample:")
equipment_bridge.show(3)

print("\nEquipment Dimension Sample:")
dim_equipment.show(3)

print("\nFact Measurements Sample:")
fact_measurements.show(3)

# COMMAND ----------

# First, capture all returned values
fact_measurements, dim_equipment, equipment_bridge, dim_location, dim_date = process_gold(silver_df)

# Now you can show the samples
print("Equipment Bridge Sample:")
equipment_bridge.show(3)

print("Equipment Dimension Sample:")
dim_equipment.show(3)

print("Fact Measurements Sample:")
fact_measurements.show(3)

# COMMAND ----------

today = date.today()
today

# COMMAND ----------

output_folder_path = f"abfss://goldhariz@harizadls.dfs.core.windows.net/equipment/equipment_gold_output_{today}"

# COMMAND ----------

def write_to_adls(fact_measurements, dim_equipment, equipment_bridge, dim_location, dim_date):
    try:
        # Set up output path
        from datetime import date
        today = date.today()
        output_folder_path = f"abfss://goldhariz@harizadls.dfs.core.windows.net/equipment/equipment_gold_output_{today}"
        
        # Write fact table
        fact_measurements.write.mode("overwrite").parquet(
            f"{output_folder_path}/fact_measurements"
        )
        print(f"Written fact_measurements to {output_folder_path}/fact_measurements")
        
        # Write dimension tables
        dim_location.write.mode("overwrite").parquet(
            f"{output_folder_path}/dim_location"
        )
        print(f"Written dim_location to {output_folder_path}/dim_location")
        
        dim_equipment.write.mode("overwrite").parquet(
            f"{output_folder_path}/dim_equipment"
        )
        print(f"Written dim_equipment to {output_folder_path}/dim_equipment")
        
        # Write equipment bridge table
        equipment_bridge.write.mode("overwrite").parquet(
            f"{output_folder_path}/equipment_bridge"
        )
        print(f"Written equipment_bridge to {output_folder_path}/equipment_bridge")
        
        dim_date.write.mode("overwrite").parquet(
            f"{output_folder_path}/dim_date"
        )
        print(f"Written dim_date to {output_folder_path}/dim_date")
        
        print(f"\nAll tables written successfully to: {output_folder_path}")
        
        return output_folder_path
        
    except Exception as e:
        print(f"Error writing to ADLS: {str(e)}")
        raise

# Execute the complete process
try:
    # 1. Process gold data
    fact_measurements, dim_equipment, equipment_bridge, dim_location, dim_date = process_gold(silver_df)
    
    # 2. Write to ADLS
    output_path = write_to_adls(
        fact_measurements, 
        dim_equipment, 
        equipment_bridge, 
        dim_location, 
        dim_date
    )
    
    # 3. Verify by reading back a sample
    print("\nVerifying written data:")
    print("\nEquipment Bridge sample:")
    spark.read.parquet(f"{output_path}/equipment_bridge").show(5)
    
    print("\nFact measurements sample with joins:")
    spark.read.parquet(f"{output_path}/fact_measurements") \
        .join(spark.read.parquet(f"{output_path}/equipment_bridge"), "functional_location") \
        .join(spark.read.parquet(f"{output_path}/dim_equipment"), "equipment_id") \
        .select(
            "functional_location",
            "equipment_type",
            "manufacturer",
            "temperature",
            "pressure",
            "maintenance_needed"
        ).show(5)

except Exception as e:
    print(f"Error in pipeline execution: {str(e)}")

# COMMAND ----------

# Step 1: Process the gold stage
try:
    fact_measurements, dim_equipment, equipment_bridge, dim_location, dim_date = process_gold(silver_df)
    print("Gold stage processing completed successfully!")
    
    # Step 2: Write to ADLS
    output_path = write_to_adls(
        fact_measurements, 
        dim_equipment, 
        equipment_bridge, 
        dim_location, 
        dim_date
    )
    print("Data written to ADLS successfully!")
    
    # Step 3: Verify written data
    print("\nVerifying written data:")
    
    # Check fact measurements
    print("\nFact Measurements sample:")
    fact_check = spark.read.parquet(f"{output_path}/fact_measurements")
    fact_check.show(5)
    
    # Check equipment bridge
    print("\nEquipment Bridge sample:")
    bridge_check = spark.read.parquet(f"{output_path}/equipment_bridge")
    bridge_check.show(5)
    
    # Check joined data
    print("\nJoined data sample (Fact + Bridge + Equipment):")
    fact_check.join(
        bridge_check, "functional_location"
    ).join(
        spark.read.parquet(f"{output_path}/dim_equipment"), "equipment_id"
    ).select(
        "functional_location",
        "equipment_type",
        "manufacturer",
        "temperature",
        "pressure",
        "maintenance_needed"
    ).show(5)
    
except Exception as e:
    print(f"Error in pipeline execution: {str(e)}")
    raise

# Optional: Print row counts
print("\nRow counts in each table:")
print(f"Fact Measurements: {fact_check.count():,} rows")
print(f"Equipment Bridge: {bridge_check.count():,} rows")
print(f"Equipment Dimension: {spark.read.parquet(f'{output_path}/dim_equipment').count():,} rows")
print(f"Location Dimension: {spark.read.parquet(f'{output_path}/dim_location').count():,} rows")
print(f"Date Dimension: {spark.read.parquet(f'{output_path}/dim_date').count():,} rows")

# COMMAND ----------

fact_measurements.show()
equipment_bridge.show()
dim_equipment.show()
dim_location.show()
dim_date.show()

# COMMAND ----------

fact_measurements.printSchema()

# COMMAND ----------

