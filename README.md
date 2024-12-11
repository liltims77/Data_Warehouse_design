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
   - model data into Fact and Dimension tables.
   - The fact_allocations table is a central component of the data warehouse, created by combining raw data from the source dataset with dimension tables to enrich the information. The process involves merging the raw data with the dim_team_member table to associate   team members and their roles, and with the dim_project table to include project and client details. This integration ensures the fact table contains both raw metrics and dimensional attributes, enabling consistent, enriched, and query-ready data for analytical purposes. The fact_allocations table serves as the foundation for the star schema design, supporting efficient data analysis and reporting.
   - The fact_allocations table is further refined by integrating additional details from the dim_task table. This merge enriches the fact table with task-specific information by aligning on the task column. To ensure consistency and readability, column names are standardized: start date, end date, and estimated hours are renamed to Start_Date, End_Date, and Estimated_Hours, respectively. These updates enhance the fact table's structure and readability, making it more consistent with established naming conventions and better suited for downstream analytical processes.
   - The fact_allocations table is further enhanced by aligning its date information with the dim_date table. The start_date column is first converted into a datetime format to ensure consistency and compatibility. This is followed by a merge operation that links the fact_allocations table with the dim_date table on the start_date column, associating it with a unique Date_ID. After the merge, the redundant date column is removed to maintain a clean and streamlined structure. These steps ensure that the fact table incorporates temporal data in a structured and normalized manner, enabling efficient time-based analysis and alignment with the star schema design.     
3. Load data into the data warehouse
   - Populate dimension tables
   - Load fact table using foreign keys from dimension table
