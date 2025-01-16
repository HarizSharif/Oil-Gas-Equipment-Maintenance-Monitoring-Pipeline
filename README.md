# Oil & Gas Equipment Maintenance Monitoring Pipeline

## Project Overview
This project implements a comprehensive data pipeline for equipment maintenance monitoring in the oil and gas industry. The pipeline processes critical equipment data including temperature, pressure, vibration, flow rate, maintenance history, efficiency metrics, and energy scores across different countries and regions. This solution enables proactive maintenance planning and equipment performance optimization.

## Architecture
The solution uses a modern data lakehouse architecture with the following components:

- **Azure Data Lake Storage**: Primary storage solution with dedicated containers
- **Azure Databricks**: Data processing and transformation
- **Snowflake**: Data warehousing
- **Power BI**: Data visualization and reporting

## Data Layer Structure

### 1. Bronze Layer (Raw Data)
- Stores incoming equipment sensor data
- Performs initial data validation
- Basic transformations:
  - Time format standardization
  - Duplicate removal
  - Data type conversions
  - Case standardization for string columns

### 2. Silver Layer (Validated Data)
- Adds derived columns for equipment analysis:
  - Equipment age (calculated from installation date)
  - Equipment status (based on operational thresholds)
  - Maintenance indicators
- Implements critical monitoring logic:
  - Warning status triggers:
    - Temperature > 100
    - Pressure > 400
    - Vibration > 8
    - Flow rate > 1000
  - Maintenance flags based on conditions:
    - Age > 365 days
    - Temperature > 90 or Vibration > 5
    - Critical conditions for specific equipment types:
      - Pumps: Temperature > 95, Vibration > 7, Pressure > 350, Flow rate < 100
      - Heat exchangers: Temperature > 85

### 3. Gold Layer (Analytics-Ready)
- Normalized data structure optimized for maintenance analysis
- Dimension tables:
  - Location dimension (facilities, regions)
  - Equipment dimension (types, specifications)
  - Equipment bridge (handling many-to-many relationships)
  - Time dimension
- Fact tables for measurements and maintenance events
- All data stored in Parquet format for optimal performance

## Data Processing

### Orchestration
- Automated pipeline execution using Databricks workflows
- Daily schedule at 5:34 AM
- Sequential processing: Bronze → Silver → Gold
- Ensures daily maintenance insights are available early morning

### Snowflake Integration
- Direct connection to Azure Data Lake Storage
- SQL-based transformations for maintenance analytics
- Dimension and fact table creation
- Continuous data validation

## Dashboard Integration
- Power BI dashboard with direct connection to Snowflake
- Live equipment status monitoring
- Maintenance planning insights
- Performance metrics visualization

## Setup Requirements

### Azure Resources
1. Resource Group creation
2. Data Lake Storage configuration
3. Databricks workspace setup
4. Key Vault configuration

### Access Configuration
- Azure tenant ID
- Storage account credentials
- Authentication tokens
- Required permissions setup

## Data Validation
- Equipment sensor data validation
- Maintenance record verification
- Schema validation at each layer
- Row count verification
- Data quality checks
- Transformation validation

## Security
- Azure Key Vault integration
- Secure credential management
- Proper access control implementation
- Industry-standard security protocols

## Contact
For any questions or issues, please contact the data engineering team.

---
*Note: This is a production system monitoring critical oil & gas equipment. Please ensure all changes are properly tested before deployment.*
