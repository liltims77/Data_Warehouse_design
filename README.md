# Data Warehouse Design. 
## A good data warehouse design involves:
 1. Understanding the business goals
 2. Identify relevant data sources.
 3. Define the destination schema.
 4. Create data warehouse design schema.
 5. Plan the ETL process for dataware house design.
 6. Choose approporiate hardware and software tools.
 7 Deploy and maintain the data warehouse.
 8. Managing data quality and performance overtime

## Introduction
This is a datawarehouse project that involves the design of efficient data models that can jandle both analytical and operational needs.
## Architecture

## Technology Used
The ETL process was done using Python in jupyter notebook and then loaded into the Postgres database. 
- Programming Language - Python

## Dataset used
- Float Data: This dataset contains staffing and allocation information for projects,
including details like team member name, project name, role, estimated hours, and
project dates.
- ClickUp Data: This dataset contains task and time tracking information, including details
like team member name, task name, project name, date, hours logged, and billable
hours.

## Dimension model for data (star schema)

## ETL Process
1. Extract data from the two datasets.
2. Transform data to ensure consistency
   - Drop duplicates and reset index
   - check and clean columns for invalid or missing data
   - Normalize names of columns for consistency
   - model data into Fact and Dimension tables
3. Load data into the data warehouse
   - Populate dimension tables
   - Load fact table using foreign keys from dimension table
