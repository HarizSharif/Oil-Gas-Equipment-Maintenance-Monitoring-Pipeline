# Oil & Gas Equipment Maintenance Monitoring Pipeline

## Project Overview
This project implements a comprehensive data pipeline for equipment maintenance monitoring in the oil and gas industry. The pipeline processes critical equipment data including temperature, pressure, vibration, flow rate, maintenance history, efficiency metrics, and energy scores across different countries and regions. This solution enables proactive maintenance planning and equipment performance optimization.

## Architecture Overview
![Pipeline Flow](https://github.com/user-attachments/assets/b64c186f-fd90-419d-8a8d-4ba791da499e)


The solution follows a modern data lakehouse architecture with three main components:

### Data Sources
- Maintenance Records
- IoT Devices
- Equipment Sensor Data (CSV Files)

### Data Processing Layers

#### Azure Data Lake Storage
- **Incoming Container**: Initial landing zone for all data sources
- **Data Lake Layers**:
  1. Bronze Layer (Raw Data)
  2. Silver Layer (Business Logic)
  3. Gold Layer (Normalized Data)

#### Azure Databricks
- **PySpark Processing**:
  - Data Transformation
  - Business Rules Implementation
  - Schema Validation
- **Notebooks**:
  - Bronze Processing
  - Silver Processing
  - Gold Processing
- **Orchestration**:
  - Daily execution at 5:34 AM
  - Job Monitoring
  - Dependencies Management

#### Data Warehouse (Snowflake)
- **Fact Tables**:
  - Equipment Measurements
- **Dimension Tables**:
  - Location
  - Equipment
  - Equipment Bridge
  - Time

#### Visualization (Power BI)
- Real-time Dashboards
- Equipment Monitoring
- Maintenance Alerts

## Data Layer Details

### 1. Bronze Layer
- Raw data ingestion
- Initial validation
- Type conversion
- Basic data quality checks

### 2. Silver Layer
- Business logic implementation
- Derived metrics calculation
- Equipment status calculations:
  - Warning status triggers:
    - Temperature > 100
    - Pressure > 400
    - Vibration > 8
    - Flow rate > 1000
  - Maintenance flags based on conditions:
    - Age > 365 days
    - Temperature > 90 or Vibration > 5
    - Critical conditions for specific equipment types

### 3. Gold Layer
- Normalized table structure
- Fact and dimension modeling
- Analytics-ready data format
- Stored in Parquet format

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

## Data Processing Workflow
1. Data ingestion into incoming container
2. Bronze layer processing via Databricks notebook
3. Silver layer transformation and business logic application
4. Gold layer normalization and dimensional modeling
  ![Date Pipieline Azure Databricks](https://github.com/user-attachments/assets/046a09f2-f8ed-405a-b66e-600f983c552e)
5. Data warehouse loading in Snowflake
6. Power BI dashboard refresh

## Security
- Azure Key Vault integration
- Secure credential management
- Proper access control implementation
- Industry-standard security protocols

## Monitoring and Maintenance
- Job execution monitoring
- Data quality validation
- Pipeline performance tracking
- Error handling and notifications

## Contact
For any questions or issues, please mailto:hariz.sharif93@gmail.com
