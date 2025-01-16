# Oil & Gas Equipment Maintenance Monitoring Pipeline

## Project Overview
This project implements a comprehensive data pipeline for equipment maintenance monitoring in the oil and gas industry. The pipeline processes critical equipment data including temperature, pressure, vibration, flow rate, maintenance history, efficiency metrics, and energy scores across different countries and regions. This solution enables proactive maintenance planning and equipment performance optimization.

<<<<<<< HEAD
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
=======
## Architecture Overview
![Pipeline Flow](https://github.com/user-attachments/assets/0abe761d-f2a3-4623-875b-dd407a89b282)


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
>>>>>>> 6c3b102efd735dee953f59956a60279ec34c567a
  - Warning status triggers:
    - Temperature > 100
    - Pressure > 400
    - Vibration > 8
    - Flow rate > 1000
  - Maintenance flags based on conditions:
    - Age > 365 days
    - Temperature > 90 or Vibration > 5
<<<<<<< HEAD
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
=======
    - Critical conditions for specific equipment types

### 3. Gold Layer
- Normalized table structure
- Fact and dimension modeling
- Analytics-ready data format
- Stored in Parquet format
>>>>>>> 6c3b102efd735dee953f59956a60279ec34c567a

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

<<<<<<< HEAD
## Data Validation
- Equipment sensor data validation
- Maintenance record verification
- Schema validation at each layer
- Row count verification
- Data quality checks
- Transformation validation
=======
## Data Processing Workflow
1. Data ingestion into incoming container
2. Bronze layer processing via Databricks notebook
3. Silver layer transformation and business logic application
4. Gold layer normalization and dimensional modeling
5. Data warehouse loading in Snowflake
6. Power BI dashboard refresh
>>>>>>> 6c3b102efd735dee953f59956a60279ec34c567a

## Security
- Azure Key Vault integration
- Secure credential management
- Proper access control implementation
- Industry-standard security protocols

<<<<<<< HEAD
## Contact
For any questions or issues, please email to hariz.sharif93@gmail.com.
=======
## Monitoring and Maintenance
- Job execution monitoring
- Data quality validation
- Pipeline performance tracking
- Error handling and notifications

## Contact
For any questions or issues, please contact the data engineering team.

---
*Note: This is a production system monitoring critical oil & gas equipment. Please ensure all changes are properly tested before deployment.*
>>>>>>> 6c3b102efd735dee953f59956a60279ec34c567a
