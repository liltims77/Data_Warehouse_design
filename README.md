# Data Warehouse Design. 
## A good data warehouse design involves:
 1. Understanding the business goals
 2. Identify relevant data sources.
 3. Define the destination schema.
 4. Create data warehouse design schema.
 5. Plan the ETL process for dataware house design.
 6. Choose approporiate hardware and software tools.
 7. Deploy and maintain the data warehouse.
 8. Managing data quality and performance overtime

## Introduction
This project demonstrates the development of a robust data warehouse designed to handle both analytical and operational needs. The primary goal is to create an efficient data architecture that integrates diverse datasets into a cohesive structure, enabling seamless data analysis and supporting day-to-day business operations.

The project involves:
- Data Modeling: Designing a star schema for analytical purposes and an entity-relationship model for operational use, ensuring scalability, data integrity, and performance optimization.
- ETL Processes: Extracting, transforming, and loading data into the data warehouse while maintaining cleanliness and consistency.
- Optimized Queries: Writing and refining queries to ensure high performance for reporting and operational needs.
This data warehouse is designed to support real-world business scenarios, such as resource allocation tracking, project management, and time-based performance analysis, making it a critical tool for decision-making and operational efficiency.

## Technology Used
The ETL process was done using Python in jupyter notebook and then loaded into the Postgres database. 
- Programming Language - Python
- SQL

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
     ### Dimension tables modeling
 - The dim_team_member table is constructed to provide a unique mapping of team members and their roles. This process begins by extracting the name and role columns from the float_df dataset and removing duplicate entries to ensure each team member-role combination is represented only once. A unique identifier, Team_Member_ID, is then assigned to each record based on its index, incremented by one to maintain a sequential and non-zero structure. Finally, the table is reordered to include Team_Member_ID, name, and role as its columns. This dimension table ensures consistency and uniqueness while linking team members and roles to the fact table in the star schema.
 - The dim_project table is created to establish a unique mapping of projects and their associated clients. The process begins by extracting the project and client columns from the float_df dataset and removing duplicate combinations to ensure that each project-client pair is represented only once. A unique identifier, Project_ID, is then assigned to each record, derived from its index incremented by one to maintain sequential and non-zero identifiers. The table is finalized with columns ordered as Project_ID, client, and project, ensuring a clean and organized structure. This dimension table serves as a key reference for associating projects and clients with the fact table in the star schema.
 - The dim_task table is created to provide a unique mapping of tasks and their billable status. This process begins by extracting the task and billable columns from the clickup_df dataset and removing duplicate entries to ensure that each task-billable combination is represented only once. A unique identifier, Task_ID, is then generated for each record by incrementing its index by one, ensuring sequential and unique identification. Finally, the table is organized with columns Task_ID, task, and billable, resulting in a clean and structured dimension table. This table serves as a key reference for linking task-related information to the fact table in the star schema.
 - The dim_date table is created to provide a structured and enriched representation of unique dates. This begins by extracting the date column from the clickup_df dataset, converting it to a datetime format, and removing duplicates to ensure each date is included only once. A unique identifier, Date_ID, is assigned to each date sequentially. Additional temporal attributes are then derived, including Year, Month (by name), Day, Week (ISO week), and Quarter. These attributes offer granular insights for time-based analysis. The final table is structured with columns Date_ID, Day, Week, Month, Quarter, Year, and date, making it a comprehensive dimension table for temporal data in the star schema.
 
     
     ### Fact table modeling
   - The fact_allocations table is a central component of the data warehouse, created by combining raw data from the source dataset with dimension tables to enrich the information. The process involves merging the raw data with the dim_team_member table to associate   team members and their roles, and with the dim_project table to include project and client details.This is done by merging dataset (float_df) with two dimension tables (dim_team_member and dim_project). It contains raw data with fields like name, role, project, and client. The on ['name', 'role'] specifies that the merge happens where name and role match in both float_df and dim_team_member. This operation further joins the intermediate result with dim_project on ['project', 'client'] this specifies the join happens where project and client match between the intermediate result and dim_project. This integration ensures the fact table contains both raw metrics and dimensional attributes, enabling consistent, enriched, and query-ready data for analytical purposes. The fact_allocations table serves as the foundation for the star schema design, supporting efficient data analysis and reporting.
   - The fact_allocations table is further refined by integrating additional details from the dim_task table. This merge enriches the fact table with task-specific information by aligning on the task column. To ensure consistency and readability, column names are standardized: start date, end date, and estimated hours are renamed to Start_Date, End_Date, and Estimated_Hours, respectively. These updates enhance the fact table's structure and readability, making it more consistent with established naming conventions and better suited for downstream analytical processes.
   - The fact_allocations table is further enhanced by aligning its date information with the dim_date table. The start_date column is first converted into a datetime format to ensure consistency and compatibility. This is followed by a merge operation that links the fact_allocations table with the dim_date table on the start_date column, associating it with a unique Date_ID. After the merge, the redundant date column is removed to maintain a clean and streamlined structure. These steps ensure that the fact table incorporates temporal data in a structured and normalized manner, enabling efficient time-based analysis and alignment with the star schema design.     
3. Load data into the data warehouse
   - Populate dimension tables
   - Load fact table using foreign keys from dimension table
 
## ETL SCRIPT
# Import necessary modules and libraries


```python
import pandas as pd
```


```python
from sqlalchemy import create_engine
```


```python
import psycopg2
```

# Loading the datasets


```python
# loading the datasets
try:
    float_df = pd.read_csv("Float - allocations.csv.csv")
    clickup_df = pd.read_csv("ClickUp - clickup.csv.csv")
    print("Datasets loaded successfully!")
except Exception as e:
    print(f"Error loading datasets: {e}")
    raise
```

    Datasets loaded successfully!


# Data cleaning and preparation


```python

try:
    # Drop duplicates and reset index for `float_df`
    float_df = float_df.drop_duplicates().reset_index(drop=True)
    float_df["float_id"] = float_df.index + 1

    # Convert date columns in float_df into datetime format
    float_df["Start Date"] = pd.to_datetime(float_df["Start Date"])
    float_df["End Date"] = pd.to_datetime(float_df["End Date"])

    # Normalize column names for `float_df`
    float_df.columns = [col.strip().replace(" ", "_").lower() for col in float_df.columns]

    # Drop duplicates for clickup_df 
    clickup_df = clickup_df.drop_duplicates().reset_index(drop=True)
    clickup_df["clickup_id"] = clickup_df.index + 1
    clickup_df.columns = [col.strip().replace(" ", "_").lower() for col in clickup_df.columns]
    print("Data cleaning successful!")
except Exception as e:
    print(f"Error during data cleaning: {e}")
    raise
```

    Data cleaning successful!



```python
# Checking for missing values in each column of float_df
print(float_df.isnull().sum())
```

    client             0
    project            0
    role               0
    name               0
    task               0
    start_date         0
    end_date           0
    estimated_hours    0
    float_id           0
    dtype: int64



```python
float_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client</th>
      <th>project</th>
      <th>role</th>
      <th>name</th>
      <th>task</th>
      <th>start_date</th>
      <th>end_date</th>
      <th>estimated_hours</th>
      <th>float_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Product Designer</td>
      <td>Isabella Rodriguez</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Design Manager</td>
      <td>John Smith</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Front End Engineer</td>
      <td>Liu Wei</td>
      <td>Development</td>
      <td>2023-07-31</td>
      <td>2023-08-28</td>
      <td>189</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>QA Engineer</td>
      <td>Emily Patel</td>
      <td>Testing</td>
      <td>2023-08-21</td>
      <td>2023-09-04</td>
      <td>77</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Project Manager</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-09-04</td>
      <td>92</td>
      <td>5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Client 1</td>
      <td>Brand Guideline</td>
      <td>Brand Designer</td>
      <td>Xu Li</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>6</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Client 1</td>
      <td>Brand Guideline</td>
      <td>Design Manager</td>
      <td>John Smith</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>32</td>
      <td>7</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Client 1</td>
      <td>Brand Guideline</td>
      <td>Project Manager</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>8</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Localization Specialist UK</td>
      <td>Vladyslav Shevchenko</td>
      <td>Localization</td>
      <td>2023-07-10</td>
      <td>2023-08-14</td>
      <td>182</td>
      <td>9</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Brand Designer</td>
      <td>Xu Li</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>182</td>
      <td>10</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Design Manager</td>
      <td>John Smith</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>52</td>
      <td>11</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Brand Designer</td>
      <td>Ana Suarez</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>0</td>
      <td>12</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Project Manager</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-07-10</td>
      <td>2023-08-28</td>
      <td>36</td>
      <td>13</td>
    </tr>
  </tbody>
</table>
</div>




```python
# # Checking for missing values in each column of clickup_df
print(clickup_df.isnull().sum())
```

    client        0
    project       0
    name          0
    task          0
    date          0
    hours         0
    note          0
    billable      0
    clickup_id    0
    dtype: int64



```python
clickup_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client</th>
      <th>project</th>
      <th>name</th>
      <th>task</th>
      <th>date</th>
      <th>hours</th>
      <th>note</th>
      <th>billable</th>
      <th>clickup_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Isabella Rodriguez</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>6.5</td>
      <td>Refined design elements</td>
      <td>Yes</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Isabella Rodriguez</td>
      <td>Design</td>
      <td>2023-07-04</td>
      <td>6.5</td>
      <td>Drafted initial design concepts</td>
      <td>Yes</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Isabella Rodriguez</td>
      <td>Design</td>
      <td>2023-07-05</td>
      <td>6.0</td>
      <td>Drafted initial design concepts</td>
      <td>Yes</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Isabella Rodriguez</td>
      <td>Design</td>
      <td>2023-07-06</td>
      <td>7.0</td>
      <td>Made revisions to design based on feedback</td>
      <td>Yes</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Isabella Rodriguez</td>
      <td>Design</td>
      <td>2023-07-07</td>
      <td>7.0</td>
      <td>Made revisions to design based on feedback</td>
      <td>Yes</td>
      <td>5</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>451</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-08-24</td>
      <td>0.0</td>
      <td>Checked in on project progress</td>
      <td>Yes</td>
      <td>452</td>
    </tr>
    <tr>
      <th>452</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-08-25</td>
      <td>0.0</td>
      <td>Communicated with client</td>
      <td>Yes</td>
      <td>453</td>
    </tr>
    <tr>
      <th>453</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-08-26</td>
      <td>0.0</td>
      <td>Checked in on project progress</td>
      <td>Yes</td>
      <td>454</td>
    </tr>
    <tr>
      <th>454</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-08-27</td>
      <td>0.0</td>
      <td>Facilitated team meeting</td>
      <td>Yes</td>
      <td>455</td>
    </tr>
    <tr>
      <th>455</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-08-28</td>
      <td>0.0</td>
      <td>Facilitated team meeting</td>
      <td>Yes</td>
      <td>456</td>
    </tr>
  </tbody>
</table>
<p>456 rows × 9 columns</p>
</div>



# Transform the datasets into dimensional and fact tables


```python
# create Dim_Team_Member dimesion from float_df
try:
    dim_team_member = float_df[['name', 'role']].drop_duplicates().reset_index(drop=True)
    dim_team_member['Team_Member_ID'] = dim_team_member.index + 1
    dim_team_member = dim_team_member[['Team_Member_ID', 'name', 'role']]
except Exception as e:
    print(f"Error during creatind dim_team_member: {e}")
    raise
    


```


```python
dim_team_member
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Team_Member_ID</th>
      <th>name</th>
      <th>role</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Isabella Rodriguez</td>
      <td>Product Designer</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>John Smith</td>
      <td>Design Manager</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Liu Wei</td>
      <td>Front End Engineer</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Emily Patel</td>
      <td>QA Engineer</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Ali Khan</td>
      <td>Project Manager</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>Xu Li</td>
      <td>Brand Designer</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>Vladyslav Shevchenko</td>
      <td>Localization Specialist UK</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>Ana Suarez</td>
      <td>Brand Designer</td>
    </tr>
  </tbody>
</table>
</div>




```python
# create Dim_Project dimension from float_df
try:
    dim_project = float_df[['project', 'client']].drop_duplicates().reset_index(drop=True)
    dim_project['Project_ID'] = dim_project.index + 1
    dim_project = dim_project[['Project_ID', 'client', 'project']]
except Exception as e:
    print(f"Error during creating dim_project: {e}")
    raise

```


```python
dim_project
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Project_ID</th>
      <th>client</th>
      <th>project</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Client 1</td>
      <td>Website Development</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Client 1</td>
      <td>Brand Guideline</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
    </tr>
  </tbody>
</table>
</div>




```python
# create Dim_Task from clickup_df
try:
    dim_task = clickup_df[['task', 'billable']].drop_duplicates().reset_index(drop=True)
    dim_task['Task_ID'] = dim_task.index + 1
    dim_task = dim_task[['Task_ID', 'task', 'billable']]
except Exception as e:
    print(f"Error during creating dim_task: {e}")
    raise
```


```python
dim_task
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Task_ID</th>
      <th>task</th>
      <th>billable</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Design</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Project Meeting</td>
      <td>No</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Design</td>
      <td>No</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Development</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Development</td>
      <td>No</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>Testing</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>Testing</td>
      <td>No</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>Management</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>Management</td>
      <td>No</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>Localization</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>Localization</td>
      <td>No</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Create Dim_Date table
try:
    unique_dates = pd.to_datetime(clickup_df['date']).drop_duplicates().reset_index(drop=True)
    dim_date = pd.DataFrame({'date': unique_dates})
    dim_date['Date_ID'] = dim_date.index + 1
    dim_date['Year'] = dim_date['date'].dt.year
    dim_date['Month'] = dim_date['date'].dt.month_name()
    dim_date['Day'] = dim_date['date'].dt.day
    dim_date['Week'] = dim_date['date'].dt.isocalendar().week
    dim_date['Quarter'] = dim_date['date'].dt.quarter
    dim_date = dim_date[['Date_ID', 'Day', 'Week', 'Month', 'Quarter','Year', 'date' ]]
except Exception as e:
    print(f"Error during creating dim_date: {e}")
    raise
```


```python
dim_date
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date_ID</th>
      <th>Day</th>
      <th>Week</th>
      <th>Month</th>
      <th>Quarter</th>
      <th>Year</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>3</td>
      <td>27</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-03</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>4</td>
      <td>27</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-04</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>5</td>
      <td>27</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-05</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>6</td>
      <td>27</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-06</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>7</td>
      <td>27</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-07</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>59</th>
      <td>60</td>
      <td>26</td>
      <td>30</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-26</td>
    </tr>
    <tr>
      <th>60</th>
      <td>61</td>
      <td>27</td>
      <td>30</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-27</td>
    </tr>
    <tr>
      <th>61</th>
      <td>62</td>
      <td>28</td>
      <td>30</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-28</td>
    </tr>
    <tr>
      <th>62</th>
      <td>63</td>
      <td>29</td>
      <td>30</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-29</td>
    </tr>
    <tr>
      <th>63</th>
      <td>64</td>
      <td>30</td>
      <td>30</td>
      <td>July</td>
      <td>3</td>
      <td>2023</td>
      <td>2023-07-30</td>
    </tr>
  </tbody>
</table>
<p>64 rows × 7 columns</p>
</div>




```python
# merge float_df with dim_team_member dimension table and dim_project
fact_allocations = float_df.merge(dim_team_member, on=['name', 'role']) \
    .merge(dim_project, on=['project', 'client'])


```


```python
fact_allocations
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>client</th>
      <th>project</th>
      <th>role</th>
      <th>name</th>
      <th>task</th>
      <th>start_date</th>
      <th>end_date</th>
      <th>estimated_hours</th>
      <th>float_id</th>
      <th>Team_Member_ID</th>
      <th>Project_ID</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Product Designer</td>
      <td>Isabella Rodriguez</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Design Manager</td>
      <td>John Smith</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>2</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Front End Engineer</td>
      <td>Liu Wei</td>
      <td>Development</td>
      <td>2023-07-31</td>
      <td>2023-08-28</td>
      <td>189</td>
      <td>3</td>
      <td>3</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>QA Engineer</td>
      <td>Emily Patel</td>
      <td>Testing</td>
      <td>2023-08-21</td>
      <td>2023-09-04</td>
      <td>77</td>
      <td>4</td>
      <td>4</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Client 1</td>
      <td>Website Development</td>
      <td>Project Manager</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-09-04</td>
      <td>92</td>
      <td>5</td>
      <td>5</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Client 1</td>
      <td>Brand Guideline</td>
      <td>Design Manager</td>
      <td>John Smith</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>32</td>
      <td>7</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Client 1</td>
      <td>Brand Guideline</td>
      <td>Project Manager</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>8</td>
      <td>5</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Client 1</td>
      <td>Brand Guideline</td>
      <td>Brand Designer</td>
      <td>Xu Li</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>6</td>
      <td>6</td>
      <td>2</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Design Manager</td>
      <td>John Smith</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>52</td>
      <td>11</td>
      <td>2</td>
      <td>3</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Project Manager</td>
      <td>Ali Khan</td>
      <td>Management</td>
      <td>2023-07-10</td>
      <td>2023-08-28</td>
      <td>36</td>
      <td>13</td>
      <td>5</td>
      <td>3</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Brand Designer</td>
      <td>Xu Li</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>182</td>
      <td>10</td>
      <td>6</td>
      <td>3</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Localization Specialist UK</td>
      <td>Vladyslav Shevchenko</td>
      <td>Localization</td>
      <td>2023-07-10</td>
      <td>2023-08-14</td>
      <td>182</td>
      <td>9</td>
      <td>7</td>
      <td>3</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Client 2</td>
      <td>Book Localization to Ukraine</td>
      <td>Brand Designer</td>
      <td>Ana Suarez</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>0</td>
      <td>12</td>
      <td>8</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python
fact_allocations = fact_allocations[['Team_Member_ID', 'Project_ID', 'task', 'start_date', 'end_date', 'estimated_hours']]
```


```python
fact_allocations 
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Team_Member_ID</th>
      <th>Project_ID</th>
      <th>task</th>
      <th>start_date</th>
      <th>end_date</th>
      <th>estimated_hours</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Development</td>
      <td>2023-07-31</td>
      <td>2023-08-28</td>
      <td>189</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>1</td>
      <td>Testing</td>
      <td>2023-08-21</td>
      <td>2023-09-04</td>
      <td>77</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>1</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-09-04</td>
      <td>92</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>32</td>
    </tr>
    <tr>
      <th>6</th>
      <td>5</td>
      <td>2</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
    </tr>
    <tr>
      <th>7</th>
      <td>6</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>52</td>
    </tr>
    <tr>
      <th>9</th>
      <td>5</td>
      <td>3</td>
      <td>Management</td>
      <td>2023-07-10</td>
      <td>2023-08-28</td>
      <td>36</td>
    </tr>
    <tr>
      <th>10</th>
      <td>6</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>182</td>
    </tr>
    <tr>
      <th>11</th>
      <td>7</td>
      <td>3</td>
      <td>Localization</td>
      <td>2023-07-10</td>
      <td>2023-08-14</td>
      <td>182</td>
    </tr>
    <tr>
      <th>12</th>
      <td>8</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
# merge fact_allocations on dim_task

fact_allocations = fact_allocations.merge(dim_task, on='task', how='left')
fact_allocations.rename(columns={
    'start date': 'Start_Date',
    'end date': 'End_Date',
    'estimated hours': 'Estimated_Hours'
}, inplace=True)
```


```python
fact_allocations
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Team_Member_ID</th>
      <th>Project_ID</th>
      <th>task</th>
      <th>start_date</th>
      <th>end_date</th>
      <th>estimated_hours</th>
      <th>Task_ID</th>
      <th>billable</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>1</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>3</td>
      <td>No</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>1</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>3</td>
      <td>No</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>1</td>
      <td>Development</td>
      <td>2023-07-31</td>
      <td>2023-08-28</td>
      <td>189</td>
      <td>4</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>5</th>
      <td>3</td>
      <td>1</td>
      <td>Development</td>
      <td>2023-07-31</td>
      <td>2023-08-28</td>
      <td>189</td>
      <td>5</td>
      <td>No</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>1</td>
      <td>Testing</td>
      <td>2023-08-21</td>
      <td>2023-09-04</td>
      <td>77</td>
      <td>6</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>7</th>
      <td>4</td>
      <td>1</td>
      <td>Testing</td>
      <td>2023-08-21</td>
      <td>2023-09-04</td>
      <td>77</td>
      <td>7</td>
      <td>No</td>
    </tr>
    <tr>
      <th>8</th>
      <td>5</td>
      <td>1</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-09-04</td>
      <td>92</td>
      <td>8</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>9</th>
      <td>5</td>
      <td>1</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-09-04</td>
      <td>92</td>
      <td>9</td>
      <td>No</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>32</td>
      <td>1</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>32</td>
      <td>3</td>
      <td>No</td>
    </tr>
    <tr>
      <th>12</th>
      <td>5</td>
      <td>2</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>8</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>13</th>
      <td>5</td>
      <td>2</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>9</td>
      <td>No</td>
    </tr>
    <tr>
      <th>14</th>
      <td>6</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>1</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>15</th>
      <td>6</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>3</td>
      <td>No</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>52</td>
      <td>1</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>52</td>
      <td>3</td>
      <td>No</td>
    </tr>
    <tr>
      <th>18</th>
      <td>5</td>
      <td>3</td>
      <td>Management</td>
      <td>2023-07-10</td>
      <td>2023-08-28</td>
      <td>36</td>
      <td>8</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>19</th>
      <td>5</td>
      <td>3</td>
      <td>Management</td>
      <td>2023-07-10</td>
      <td>2023-08-28</td>
      <td>36</td>
      <td>9</td>
      <td>No</td>
    </tr>
    <tr>
      <th>20</th>
      <td>6</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>182</td>
      <td>1</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>21</th>
      <td>6</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>182</td>
      <td>3</td>
      <td>No</td>
    </tr>
    <tr>
      <th>22</th>
      <td>7</td>
      <td>3</td>
      <td>Localization</td>
      <td>2023-07-10</td>
      <td>2023-08-14</td>
      <td>182</td>
      <td>10</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>23</th>
      <td>7</td>
      <td>3</td>
      <td>Localization</td>
      <td>2023-07-10</td>
      <td>2023-08-14</td>
      <td>182</td>
      <td>11</td>
      <td>No</td>
    </tr>
    <tr>
      <th>24</th>
      <td>8</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>0</td>
      <td>1</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>25</th>
      <td>8</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>0</td>
      <td>3</td>
      <td>No</td>
    </tr>
  </tbody>
</table>
</div>




```python

fact_allocations['start_date'] = pd.to_datetime(fact_allocations['start_date'])
fact_allocations = fact_allocations.merge(dim_date[['date', 'Date_ID']], left_on='start_date', right_on='date', how='left')
fact_allocations.drop(columns=['date'], inplace=True)
```


```python
fact_allocations
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Team_Member_ID</th>
      <th>Project_ID</th>
      <th>task</th>
      <th>start_date</th>
      <th>end_date</th>
      <th>estimated_hours</th>
      <th>Task_ID</th>
      <th>billable</th>
      <th>Date_ID</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>1</td>
      <td>Yes</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>3</td>
      <td>No</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>1</td>
      <td>Yes</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>1</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>3</td>
      <td>No</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3</td>
      <td>1</td>
      <td>Development</td>
      <td>2023-07-31</td>
      <td>2023-08-28</td>
      <td>189</td>
      <td>4</td>
      <td>Yes</td>
      <td>23</td>
    </tr>
    <tr>
      <th>5</th>
      <td>3</td>
      <td>1</td>
      <td>Development</td>
      <td>2023-07-31</td>
      <td>2023-08-28</td>
      <td>189</td>
      <td>5</td>
      <td>No</td>
      <td>23</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>1</td>
      <td>Testing</td>
      <td>2023-08-21</td>
      <td>2023-09-04</td>
      <td>77</td>
      <td>6</td>
      <td>Yes</td>
      <td>44</td>
    </tr>
    <tr>
      <th>7</th>
      <td>4</td>
      <td>1</td>
      <td>Testing</td>
      <td>2023-08-21</td>
      <td>2023-09-04</td>
      <td>77</td>
      <td>7</td>
      <td>No</td>
      <td>44</td>
    </tr>
    <tr>
      <th>8</th>
      <td>5</td>
      <td>1</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-09-04</td>
      <td>92</td>
      <td>8</td>
      <td>Yes</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>5</td>
      <td>1</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-09-04</td>
      <td>92</td>
      <td>9</td>
      <td>No</td>
      <td>1</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>32</td>
      <td>1</td>
      <td>Yes</td>
      <td>1</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>32</td>
      <td>3</td>
      <td>No</td>
      <td>1</td>
    </tr>
    <tr>
      <th>12</th>
      <td>5</td>
      <td>2</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>8</td>
      <td>Yes</td>
      <td>1</td>
    </tr>
    <tr>
      <th>13</th>
      <td>5</td>
      <td>2</td>
      <td>Management</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>24</td>
      <td>9</td>
      <td>No</td>
      <td>1</td>
    </tr>
    <tr>
      <th>14</th>
      <td>6</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>1</td>
      <td>Yes</td>
      <td>1</td>
    </tr>
    <tr>
      <th>15</th>
      <td>6</td>
      <td>2</td>
      <td>Design</td>
      <td>2023-07-03</td>
      <td>2023-07-24</td>
      <td>112</td>
      <td>3</td>
      <td>No</td>
      <td>1</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>52</td>
      <td>1</td>
      <td>Yes</td>
      <td>22</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>52</td>
      <td>3</td>
      <td>No</td>
      <td>22</td>
    </tr>
    <tr>
      <th>18</th>
      <td>5</td>
      <td>3</td>
      <td>Management</td>
      <td>2023-07-10</td>
      <td>2023-08-28</td>
      <td>36</td>
      <td>8</td>
      <td>Yes</td>
      <td>8</td>
    </tr>
    <tr>
      <th>19</th>
      <td>5</td>
      <td>3</td>
      <td>Management</td>
      <td>2023-07-10</td>
      <td>2023-08-28</td>
      <td>36</td>
      <td>9</td>
      <td>No</td>
      <td>8</td>
    </tr>
    <tr>
      <th>20</th>
      <td>6</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>182</td>
      <td>1</td>
      <td>Yes</td>
      <td>22</td>
    </tr>
    <tr>
      <th>21</th>
      <td>6</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>182</td>
      <td>3</td>
      <td>No</td>
      <td>22</td>
    </tr>
    <tr>
      <th>22</th>
      <td>7</td>
      <td>3</td>
      <td>Localization</td>
      <td>2023-07-10</td>
      <td>2023-08-14</td>
      <td>182</td>
      <td>10</td>
      <td>Yes</td>
      <td>8</td>
    </tr>
    <tr>
      <th>23</th>
      <td>7</td>
      <td>3</td>
      <td>Localization</td>
      <td>2023-07-10</td>
      <td>2023-08-14</td>
      <td>182</td>
      <td>11</td>
      <td>No</td>
      <td>8</td>
    </tr>
    <tr>
      <th>24</th>
      <td>8</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>0</td>
      <td>1</td>
      <td>Yes</td>
      <td>22</td>
    </tr>
    <tr>
      <th>25</th>
      <td>8</td>
      <td>3</td>
      <td>Design</td>
      <td>2023-07-24</td>
      <td>2023-08-28</td>
      <td>0</td>
      <td>3</td>
      <td>No</td>
      <td>22</td>
    </tr>
  </tbody>
</table>
</div>



# Load data into the database(postgres)


```python
from sqlalchemy import create_engine
```


```python
engine = create_engine('postgresql://postgres:<password>@localhost:5433/postgres')
```


```python
engine.connect()
```




    <sqlalchemy.engine.base.Connection at 0x7f46066b9300>




```python
# Database Loading
try:
    # Load tables into the database
    dim_team_member.to_sql("dim_team_member", engine, if_exists="replace", index=False)
    dim_task.to_sql("dim_task", engine, if_exists="replace", index=False)
    dim_project.to_sql("dim_project", engine, if_exists="replace", index=False)
    dim_date.to_sql("dim_date", engine, if_exists="replace", index=False)
    fact_allocations.to_sql("fact_allocations", engine, if_exists="replace", index=False)
    print("Data loaded into the database successfully!")
except Exception as e:
    print(f"Error loading data into the database: {e}")
    raise
```

    Data loaded into the database successfully!

```python

```
## Load Fact and Dimension into EDW schema for analysis
Fact and dimension tables were loaded into the edw (enterprise datawarehouse) schema where they can be used for analysis, reporting and also go get deep insight required for the organization.
![LOAD_INTO_EDW](https://github.com/user-attachments/assets/9639107d-e1f1-464b-bf8b-7e5ca6f6ffca)

## Data integrity and cleanliness
- Data Integrity refers to the accuracy, consistency, and trustworthiness of data over its entire lifecycle. It ensures that data is complete and unaltered, except through authorized modifications. This concept encompasses aspects like preventing corruption, ensuring proper access control, and maintaining data accuracy through regular validation.
- Data Cleanliness focuses on ensuring that data is free from errors, inconsistencies, and redundancies. Clean data is devoid of duplicates, inaccuracies, and irrelevant information, making it suitable for analysis and use.
### Steps taken to ensure data integrity for this project
- Dropping Duplicates and Resetting Index: Removing duplicate records prevents redundant data from skewing analysis or insights. Resetting the index ensures data organization and proper tracking after duplicates are removed.
- Checking and Cleaning Columns for Invalid or Missing Data: This step ensures that each column contains valid entries, replacing or removing missing values as necessary to maintain the dataset's usability and consistency.
- Normalizing Column Names for Consistency: Standardizing column names ensures uniformity, which is particularly important when merging datasets or automating processes. Consistent naming conventions improve readability and reduce errors during analysis.

## Validate data correctness for Fact and Dimension tables
Data from source datasets was also cleaned and loaded to the database to validate correctness of data accross board.
https://github.com/liltims77/Data_Warehouse_design/blob/145224378bdcb829237a42b7f761424e1c677f29/upload_source_datasets.ipynb


1. Row Count Verification: I ensured that the number of records (count) in dimension tables matches the unique entries source data. Example comparing float source data with dim_team_member dimension table. The counts from both queries returned 8 rows.
  ![validation_1](https://github.com/user-attachments/assets/d8bbe2ef-3fec-4431-bf6e-e73277474dc8)
  
  
2. Primary Key Uniqueness: Verified that primary keys in dimension tables are unique and not null. Example for dim_team_member. Both queries returned zero, indicating no duplicates or nulls.
  ![validation_2](https://github.com/user-attachments/assets/06cfde44-dee3-4eae-a429-e8db3f5f4a41)
 
  
3. Referential Integrity: I ensure that foreign keys in fact table correspond to valid entries in dimension tables. Example for fact_allocations referencing dim_team_member. This query should return no results, indicating all Team_Member_IDs in fact_allocations are valid.
  ![validation_3](https://github.com/user-attachments/assets/c173bfe6-7992-445c-b5c0-476057d3d904)

  
4. Data Consistency Checks: I compare aggregated values between source data and fact tables to ensure consistency. Example for Estimated_Hours. The sums of both queries returned (2228), confirming data consistency.
  ![validation_4](https://github.com/user-attachments/assets/0afc6940-95ba-431d-9fdc-bf87b4d187e0)
   
  
5. Date Dimension Validation: I ensure that all dates in fact table have corresponding entries in date dimension. This query returned no results, indicating complete date coverage.
  ![validation_5](https://github.com/user-attachments/assets/2006bbee-6fc2-4f17-87f1-69fcda2ac086)
   
  
6. Data Type and Format Validation: Confirm that data types and formats are consistent with expectations. Example for Start_Date. This query identifies any dates not in the 'YYYY-MM-DD' format.
  ![validation_6](https://github.com/user-attachments/assets/caf33979-b72e-4c98-bc3b-5a6415e58817)




# Database Query Optimization
![Query optimization question](https://github.com/user-attachments/assets/525a5e2c-f4bc-45d0-beee-99dd5614d75b)

## Optimization Techniques

To optimize the given query for performance, the following steps were taken, ensuring efficient data retrieval and processing, especially for complex datasets:

1. Indexing for Efficient Joins and Aggregations: 
Indexes on ClickUp.Name and Float.Name: These indexes significantly enhance the performance of the JOIN operation by enabling faster lookups of matching rows. This is particularly useful for large datasets where join operations can become costly.
Index on ClickUp.hours: Since the query involves filtering and aggregation on the hours column (SUM(c.hours)), indexing this column reduces disk I/O, improving computation speed. ![1 adding_indexes](https://github.com/user-attachments/assets/3e159310-8f86-4bc1-8f00-d9d8d8a9cd85)
2. Early Filtering with the WHERE Clause: 
The addition of a WHERE clause (c.hours > 0) before the GROUP BY operation helps to filter out unnecessary rows at the earliest stage of query execution. By reducing the dataset size before aggregation, this step minimizes the computational load and speeds up query execution.
3. Utilizing Common Table Expressions (CTEs): 
A WITH clause (CTE) is used to aggregate data separately, allowing for better organization and easier optimization of the query logic. By isolating the aggregation step, the query becomes more readable and maintainable, especially in scenarios involving complex operations or reusable components. ![2  using CTE](https://github.com/user-attachments/assets/0c8cfd40-fc8e-44eb-9846-e684f3285c75)
4. Handling Dates with Aggregation: 
To ensure no ambiguity in results, the Date column is aggregated using MAX(c.Date) in cases where the latest date is needed. If raw date values are required, this logic can be adjusted accordingly to meet specific use cases. ![1  using_MAX_date](https://github.com/user-attachments/assets/b6359574-8e05-4427-8d54-0a7411399bc6)
5. Optimized Sorting: 
Sorting by Total_Allocated_Hours is retained in the final step. However, since it is performed after filtering and aggregation, the number of rows being sorted is significantly reduced, improving overall efficiency.
6. Partitioning for Extremely Large Datasets: 
If we have a very large datasets, such as millions of records in the ClickUp and Float tables, partitioning these tables by a column like Date or Role can help in faster query execution. Partitioning enables PostgreSQL to process only the relevant partitions, reducing the time and resources required for operations.

# Data Models for Analytical and Operational Purposes
  ## A. Dimensional Model (Star Schema)
![dimension_model_starschema](https://github.com/user-attachments/assets/42bcbf18-da34-43d3-9d5b-0c0437dd71d3)

The dimensional model is designed for analytical purposes using a star schema structure. It consists of:

- Fact Table: fact_allocations
. Purpose: Captures quantitative data such as hours allocated, billable status, and date ranges.
. Attributes:
Team_Member_ID, Project_ID, Task_ID, Date_ID (Foreign Keys to dimension tables).
start_date, end_date, estimated_hours, and billable (Metrics for analysis).
- Dimension Tables:
1. dim_team_member:
Describes team members and their roles.
Attributes: Team_Member_ID, name, role.
Enables resource utilization analysis.
2. dim_task:
Describes tasks and their billable status.
Attributes: Task_ID, task, billable.
Supports task-level and billing-related analysis.
3. dim_project:
Represents projects and their associated clients.
Attributes: Project_ID, client, project.
Facilitates project and client performance tracking.
4. dim_date:
Provides temporal information.
Attributes: Date_ID, Day, Week, Month, Quarter, Year, date.
Enables time-based aggregations, such as weekly, monthly, or quarterly trends.

## Why the Dimensional Model?
Optimized for querying and aggregations, ideal for reporting and business intelligence.
Simple, scalable, and easy to understand for non-technical users.



   ## B. Entity-Relationship Diagram (Operational Model)
![ER_diagram_model](https://github.com/user-attachments/assets/3634078e-7165-4a4b-aa72-65ad177b36c4)
The Entity-Relationship Diagram (ERD) represents the operational data model, focusing on how different entities relate to each other in a normalized structure. It is used for transactional systems where data integrity and consistency are critical.
The ERD is designed for operational purposes, providing a normalized view of the data to support daily operations.

## Entities and Relationships:
1. Entities:
- TeamMember: Represents individual team members and their roles. Attributes include Team_Member_ID, name, role.
- Task: Represents the tasks assigned to team members. Attributes include Task_ID, task, billable.
- Project: Represents projects linked to tasks and clients. Attributes include Project_ID, client, project.
- Date: Represents temporal data, like specific dates and their associated calendar attributes. Attributes include Date_ID, date, Year, Month, Week, Day, Quarter.
2. Relationships:
- Represents the relationships between team members, tasks, projects, and dates, storing details like start date, end date, estimated hours, and billable status. The Allocations table links TeamMember, Task, Project, and Date to capture task assignments and resource allocations.

## Why the Operational Model?
Ensures data normalization, reducing redundancy and maintaining data consistency.
Supports transactional needs such as tracking tasks, billing, and managing project timelines.

## Design Decisions
1. Dimensional Model (Star Schema):
- Chosen for its efficiency in handling analytical workloads.
- Simplifies querying and supports business reporting.
2. Entity-Relationship Diagram (ERD):
- Provides a normalized structure to ensure operational data consistency.
- Supports detailed tracking and management of resources and tasks.
- Ensures each entity (e.g., TeamMember, Project) is stored once, with relationships defined via primary and foreign keys.


## Conclusion
These models address both analytical and operational needs:
The ER model is ideal for operational systems that prioritize data accuracy and consistency, while the Fact and Dimension model is designed for analytical systems, focusing on performance and ease of data aggregation. Both models complement each other in addressing different business needs.
- The Star Schema enables efficient querying and reporting for business intelligence.
- The ERD supports daily operations with a robust and normalized data structure.
By combining these two models, the solution ensures scalability, consistency, and efficiency in handling data for various purposes.


# Big Data Processing with Spark
## Spark script
```python
# Import the SparkSession class from the PySpark SQL module
from pyspark.sql import SparkSession
```


```python
# Import specific functions from PySpark SQL module
# 'col' is used to access columns in a DataFrame for transformations
# 'sum' is imported as 'spark_sum' to avoid conflicts with Python's built-in 'sum' function
from pyspark.sql.functions import col, sum as spark_sum

```


```python
# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Data Processing") \
    .getOrCreate()
```

    24/12/12 22:29:20 WARN Utils: Your hostname, liltimz resolves to a loopback address: 127.0.1.1; using 172.29.249.245 instead (on interface eth0)
    24/12/12 22:29:20 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    24/12/12 22:29:23 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable



```python
spark
```





    <div>
        <p><b>SparkSession - in-memory</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://172.29.249.245:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.4.1</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>Data Processing</code></dd>
    </dl>
</div>

    </div>





```python
# Downloading dataset using wget
# !wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet

```

    --2024-12-13 02:44:39--  https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet
    Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 18.239.238.133, 18.239.238.152, 18.239.238.212, ...
    Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|18.239.238.133|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 308924937 (295M) [application/x-www-form-urlencoded]
    Saving to: ‘fhvhv_tripdata_2021-01.parquet’
    
    fhvhv_tripdata_2021 100%[===================>] 294.61M  4.27MB/s    in 5m 9s   
    
    2024-12-13 02:50:11 (976 KB/s) - ‘fhvhv_tripdata_2021-01.parquet’ saved [308924937/308924937]
    



```python

```


```python



```python
# Read a Parquet file into a Spark DataFrame
df = spark.read \
    .option("header", "true") \
    .parquet('fhvhv_tripdata_2021-01.parquet')
```


```python
df.show()
```

                                                                                    

    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    |hvfhs_license_num|dispatching_base_num|originating_base_num|   request_datetime|  on_scene_datetime|    pickup_datetime|   dropoff_datetime|PULocationID|DOLocationID|trip_miles|trip_time|base_passenger_fare|tolls| bcf|sales_tax|congestion_surcharge|airport_fee|tips|driver_pay|shared_request_flag|shared_match_flag|access_a_ride_flag|wav_request_flag|wav_match_flag|
    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    |           HV0003|              B02682|              B02682|2021-01-01 00:28:09|2021-01-01 00:31:42|2021-01-01 00:33:44|2021-01-01 00:49:07|         230|         166|      5.26|      923|              22.28|  0.0|0.67|     1.98|                2.75|       null| 0.0|     14.99|                  N|                N|                  |               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:45:56|2021-01-01 00:55:19|2021-01-01 00:55:19|2021-01-01 01:18:21|         152|         167|      3.65|     1382|              18.36|  0.0|0.55|     1.63|                 0.0|       null| 0.0|     17.06|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:21:15|2021-01-01 00:22:41|2021-01-01 00:23:56|2021-01-01 00:38:05|         233|         142|      3.51|      849|              14.05|  0.0|0.48|     1.25|                2.75|       null|0.94|     12.98|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:39:12|2021-01-01 00:42:37|2021-01-01 00:42:51|2021-01-01 00:45:50|         142|         143|      0.74|      179|               7.91|  0.0|0.24|      0.7|                2.75|       null| 0.0|      7.41|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:46:11|2021-01-01 00:47:17|2021-01-01 00:48:14|2021-01-01 01:08:42|         143|          78|       9.2|     1228|              27.11|  0.0|0.81|     2.41|                2.75|       null| 0.0|     22.44|                  N|                N|                  |               N|             N|
    |           HV0005|              B02510|                null|2021-01-01 00:04:00|               null|2021-01-01 00:06:59|2021-01-01 00:43:01|          88|          42|     9.725|     2162|              28.11|  0.0|0.84|     2.49|                2.75|       null| 0.0|      28.9|                  N|                N|                 N|               N|             N|
    |           HV0005|              B02510|                null|2021-01-01 00:40:06|               null|2021-01-01 00:50:00|2021-01-01 01:04:57|          42|         151|     2.469|      897|              25.03|  0.0|0.75|     2.22|                 0.0|       null| 0.0|     15.01|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 00:10:36|2021-01-01 00:12:28|2021-01-01 00:14:30|2021-01-01 00:50:27|          71|         226|     13.53|     2157|              29.67|  0.0|1.04|     3.08|                 0.0|       null| 0.0|      34.2|                  N|                N|                  |               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-01 00:21:17|2021-01-01 00:22:25|2021-01-01 00:22:54|2021-01-01 00:30:20|         112|         255|       1.6|      446|               6.89|  0.0|0.21|     0.61|                 0.0|       null| 0.0|      6.26|                  N|                N|                  |               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-01 00:36:57|2021-01-01 00:38:09|2021-01-01 00:40:12|2021-01-01 00:53:31|         255|         232|       3.2|      800|              11.51|  0.0|0.53|     1.03|                2.75|       null|2.82|     10.99|                  N|                N|                  |               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-01 00:53:31|2021-01-01 00:56:21|2021-01-01 00:56:45|2021-01-01 01:17:42|         232|         198|      5.74|     1257|              17.18|  0.0|0.52|     1.52|                2.75|       null| 0.0|     17.61|                  N|                N|                  |               N|             N|
    |           HV0003|              B02835|              B02835|2021-01-01 00:22:58|2021-01-01 00:27:01|2021-01-01 00:29:04|2021-01-01 00:36:27|         113|          48|       1.8|      443|               8.18|  0.0|0.25|     0.73|                2.75|       null| 0.0|      6.12|                  N|                N|                  |               N|             N|
    |           HV0003|              B02835|              B02835|2021-01-01 00:46:44|2021-01-01 00:47:49|2021-01-01 00:48:56|2021-01-01 00:59:12|         239|          75|       2.9|      616|               13.1|  0.0|0.45|     1.17|                2.75|       null|0.94|      8.77|                  N|                N|                  |               N|             N|
    |           HV0004|              B02800|                null|2021-01-01 00:12:50|               null|2021-01-01 00:15:24|2021-01-01 00:38:31|         181|         237|      9.66|     1387|              32.95|  0.0| 0.0|     2.34|                2.75|       null| 0.0|      21.1|                  N|                N|                 N|               N|             N|
    |           HV0004|              B02800|                null|2021-01-01 00:35:32|               null|2021-01-01 00:45:00|2021-01-01 01:06:45|         236|          68|      4.38|     1305|              22.91|  0.0| 0.0|     1.63|                2.75|       null|3.43|     15.82|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:10:22|2021-01-01 00:11:03|2021-01-01 00:11:53|2021-01-01 00:18:06|         256|         148|      2.03|      373|               7.84|  0.0|0.42|      0.7|                2.75|       null|2.82|      6.93|                  N|                N|                  |               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:25:00|2021-01-01 00:26:31|2021-01-01 00:28:31|2021-01-01 00:41:40|          79|          80|      3.08|      789|               13.2|  0.0| 0.4|     1.17|                2.75|       null| 0.0|     11.54|                  N|                N|                  |               N|             N|
    |           HV0003|              B02682|              B02682|2021-01-01 00:44:56|2021-01-01 00:49:55|2021-01-01 00:50:49|2021-01-01 00:55:59|          17|         217|      1.17|      310|               7.91|  0.0|0.24|      0.7|                 0.0|       null| 0.0|      6.94|                  N|                N|                  |               N|             N|
    |           HV0005|              B02510|                null|2021-01-01 00:05:04|               null|2021-01-01 00:08:40|2021-01-01 00:39:39|          62|          29|    10.852|     1859|              31.18|  0.0|0.94|     2.77|                 0.0|       null| 0.0|     27.61|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02836|              B02836|2021-01-01 00:40:44|2021-01-01 00:53:34|2021-01-01 00:53:48|2021-01-01 01:11:40|          22|          22|      3.52|     1072|              28.67|  0.0|0.86|     2.54|                 0.0|       null| 0.0|     17.64|                  N|                N|                  |               N|             N|
    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    only showing top 20 rows
    



```python
df.schema
```




    StructType([StructField('hvfhs_license_num', StringType(), True), StructField('dispatching_base_num', StringType(), True), StructField('originating_base_num', StringType(), True), StructField('request_datetime', TimestampNTZType(), True), StructField('on_scene_datetime', TimestampNTZType(), True), StructField('pickup_datetime', TimestampNTZType(), True), StructField('dropoff_datetime', TimestampNTZType(), True), StructField('PULocationID', LongType(), True), StructField('DOLocationID', LongType(), True), StructField('trip_miles', DoubleType(), True), StructField('trip_time', LongType(), True), StructField('base_passenger_fare', DoubleType(), True), StructField('tolls', DoubleType(), True), StructField('bcf', DoubleType(), True), StructField('sales_tax', DoubleType(), True), StructField('congestion_surcharge', DoubleType(), True), StructField('airport_fee', DoubleType(), True), StructField('tips', DoubleType(), True), StructField('driver_pay', DoubleType(), True), StructField('shared_request_flag', StringType(), True), StructField('shared_match_flag', StringType(), True), StructField('access_a_ride_flag', StringType(), True), StructField('wav_request_flag', StringType(), True), StructField('wav_match_flag', StringType(), True)])




```python
# Extract the first 1001 lines from the Parquet file and save it to a new file named head.parquet
!head -n 1001 fhvhv_tripdata_2021-01.parquet > head.parquet
```


```python



```python
# Repartition the DataFrame into 24 partitions
# Repartitioning redistributes the data across a specified number of partitions
# This is useful for parallel processing in distributed systems like Spark
# Increasing partitions can improve performance for large datasets by leveraging more resources
# However, excessive repartitioning can lead to overhead and reduced performance
df = df.repartition(24)
```


```python
# Print the schema of the DataFrame
df.printSchema()
```

    root
     |-- hvfhs_license_num: string (nullable = true)
     |-- dispatching_base_num: string (nullable = true)
     |-- originating_base_num: string (nullable = true)
     |-- request_datetime: timestamp_ntz (nullable = true)
     |-- on_scene_datetime: timestamp_ntz (nullable = true)
     |-- pickup_datetime: timestamp_ntz (nullable = true)
     |-- dropoff_datetime: timestamp_ntz (nullable = true)
     |-- PULocationID: long (nullable = true)
     |-- DOLocationID: long (nullable = true)
     |-- trip_miles: double (nullable = true)
     |-- trip_time: long (nullable = true)
     |-- base_passenger_fare: double (nullable = true)
     |-- tolls: double (nullable = true)
     |-- bcf: double (nullable = true)
     |-- sales_tax: double (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- airport_fee: double (nullable = true)
     |-- tips: double (nullable = true)
     |-- driver_pay: double (nullable = true)
     |-- shared_request_flag: string (nullable = true)
     |-- shared_match_flag: string (nullable = true)
     |-- access_a_ride_flag: string (nullable = true)
     |-- wav_request_flag: string (nullable = true)
     |-- wav_match_flag: string (nullable = true)
    



```python
# Import the 'types' module from PySpark SQL
from pyspark.sql import types
```


```python
df.printSchema()
```

    root
     |-- hvfhs_license_num: string (nullable = true)
     |-- dispatching_base_num: string (nullable = true)
     |-- originating_base_num: string (nullable = true)
     |-- request_datetime: timestamp_ntz (nullable = true)
     |-- on_scene_datetime: timestamp_ntz (nullable = true)
     |-- pickup_datetime: timestamp_ntz (nullable = true)
     |-- dropoff_datetime: timestamp_ntz (nullable = true)
     |-- PULocationID: long (nullable = true)
     |-- DOLocationID: long (nullable = true)
     |-- trip_miles: double (nullable = true)
     |-- trip_time: long (nullable = true)
     |-- base_passenger_fare: double (nullable = true)
     |-- tolls: double (nullable = true)
     |-- bcf: double (nullable = true)
     |-- sales_tax: double (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- airport_fee: double (nullable = true)
     |-- tips: double (nullable = true)
     |-- driver_pay: double (nullable = true)
     |-- shared_request_flag: string (nullable = true)
     |-- shared_match_flag: string (nullable = true)
     |-- access_a_ride_flag: string (nullable = true)
     |-- wav_request_flag: string (nullable = true)
     |-- wav_match_flag: string (nullable = true)
    



```python
df.show()
```

    [Stage 7:============================================>              (3 + 1) / 4]

    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    |hvfhs_license_num|dispatching_base_num|originating_base_num|   request_datetime|  on_scene_datetime|    pickup_datetime|   dropoff_datetime|PULocationID|DOLocationID|trip_miles|trip_time|base_passenger_fare|tolls| bcf|sales_tax|congestion_surcharge|airport_fee|tips|driver_pay|shared_request_flag|shared_match_flag|access_a_ride_flag|wav_request_flag|wav_match_flag|
    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    |           HV0003|              B02764|              B02764|2021-01-15 18:37:48|2021-01-15 18:40:23|2021-01-15 18:42:24|2021-01-15 19:02:35|         256|          17|      2.59|     1212|              15.37|  0.0|0.46|     1.36|                 0.0|       null| 0.0|     18.07|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-19 10:53:43|2021-01-19 10:57:39|2021-01-19 10:57:44|2021-01-19 11:01:45|          71|          89|      0.69|      241|                8.7|  0.0|0.26|     0.77|                 0.0|       null| 0.0|       5.4|                  N|                N|                  |               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-23 15:14:16|2021-01-23 15:25:36|2021-01-23 15:26:02|2021-01-23 15:39:44|          76|          37|      2.14|      822|              10.97|  0.0|0.33|     0.97|                 0.0|       null| 0.0|      9.24|                  N|                N|                  |               N|             N|
    |           HV0005|              B02510|                null|2021-01-20 13:13:07|               null|2021-01-20 13:15:15|2021-01-20 13:26:27|         107|         148|     1.463|      672|              10.86|  0.0|0.33|     0.96|                2.75|       null| 3.0|      7.24|                  N|                N|                 N|               N|             N|
    |           HV0005|              B02510|                null|2021-01-27 09:21:02|               null|2021-01-27 09:23:08|2021-01-27 09:40:56|          75|         170|     4.837|     1068|              20.45|  0.0|0.61|     1.81|                2.75|       null| 0.0|     14.32|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02875|              B02875|2021-01-31 09:20:21|2021-01-31 09:22:20|2021-01-31 09:24:21|2021-01-31 09:38:10|          92|          73|       2.6|      829|               11.8|  0.0|0.35|     1.05|                 0.0|       null| 0.0|       0.0|                  N|                N|                  |               N|             Y|
    |           HV0005|              B02510|                null|2021-01-28 07:49:51|               null|2021-01-28 07:55:01|2021-01-28 08:06:49|          76|          72|     2.368|      708|               13.0|  0.0|0.39|     1.15|                 0.0|       null| 0.0|      8.56|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-16 13:31:17|2021-01-16 13:33:28|2021-01-16 13:34:30|2021-01-16 13:57:08|         246|         229|      3.68|     1358|              16.06|  0.0|0.48|     1.43|                2.75|       null| 0.0|     15.42|                  N|                N|                  |               N|             N|
    |           HV0003|              B02884|              B02884|2021-01-07 08:59:22|2021-01-07 09:01:48|2021-01-07 09:02:05|2021-01-07 09:38:56|         170|          25|      5.55|     2211|              27.95|  0.0|0.84|     2.48|                2.75|       null| 0.0|     24.62|                  N|                N|                  |               N|             N|
    |           HV0003|              B02880|              B02880|2021-01-21 19:46:52|2021-01-21 19:47:44|2021-01-21 19:48:24|2021-01-21 19:55:59|         216|         215|      1.99|      455|               8.08|  0.0|0.24|     0.72|                 0.0|       null| 0.0|      6.01|                  N|                N|                  |               N|             N|
    |           HV0003|              B02869|              B02869|2021-01-05 15:11:12|2021-01-05 15:13:41|2021-01-05 15:14:41|2021-01-05 15:21:34|          76|          63|      1.29|      413|               7.91|  0.0|0.24|      0.7|                 0.0|       null| 0.0|       5.4|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-01 03:20:21|2021-01-01 03:22:29|2021-01-01 03:24:21|2021-01-01 03:36:40|          74|         136|      5.69|      739|              17.97|  0.0|0.54|     1.59|                 0.0|       null| 0.0|     14.84|                  N|                N|                  |               N|             N|
    |           HV0003|              B02883|              B02883|2021-01-09 15:11:30|2021-01-09 15:13:01|2021-01-09 15:13:12|2021-01-09 15:50:25|          37|         244|     14.47|     2233|              38.87| 6.12|1.35|     3.99|                 0.0|       null| 0.0|     34.64|                  N|                N|                  |               N|             N|
    |           HV0003|              B02869|              B02869|2021-01-15 14:52:36|2021-01-15 14:55:27|2021-01-15 14:56:28|2021-01-15 15:08:35|          22|         108|      2.15|      727|              11.69|  0.0|0.35|     1.04|                 0.0|       null| 0.0|      8.45|                  N|                N|                  |               N|             N|
    |           HV0003|              B02866|              B02866|2021-01-24 00:13:26|2021-01-24 00:15:34|2021-01-24 00:15:59|2021-01-24 00:34:46|          32|         168|      7.89|     1127|              20.26|  0.0|0.61|      1.8|                 0.0|       null| 0.0|     18.13|                  N|                N|                  |               N|             N|
    |           HV0003|              B02764|              B02764|2021-01-16 04:19:02|2021-01-16 04:24:47|2021-01-16 04:26:24|2021-01-16 04:38:27|         250|         212|      2.35|      723|               9.21|  0.0|0.28|     0.82|                 0.0|       null| 0.0|      8.65|                  N|                N|                  |               N|             N|
    |           HV0003|              B02872|              B02872|2021-01-05 13:40:38|2021-01-05 13:42:41|2021-01-05 13:43:48|2021-01-05 13:53:52|          82|         173|      1.36|      604|               9.86|  0.0| 0.3|     0.88|                 0.0|       null| 0.0|      6.56|                  N|                N|                  |               N|             N|
    |           HV0003|              B02871|              B02871|2021-01-06 04:19:40|2021-01-06 04:20:38|2021-01-06 04:21:52|2021-01-06 04:24:12|          37|          37|      0.51|      140|               6.33|  0.0|0.19|     0.56|                 0.0|       null| 0.0|      5.39|                  N|                N|                  |               N|             N|
    |           HV0005|              B02510|                null|2021-01-16 00:11:16|               null|2021-01-16 00:19:17|2021-01-16 00:25:57|          45|         232|     1.255|      400|               7.77|  0.0|0.23|     0.69|                2.75|       null| 3.0|      5.47|                  N|                N|                 N|               N|             N|
    |           HV0003|              B02877|              B02877|2021-01-01 19:24:47|2021-01-01 19:28:21|2021-01-01 19:28:45|2021-01-01 19:39:36|         239|         158|      3.88|      651|              17.79|  0.0|0.53|     1.58|                2.75|       null| 0.0|     10.94|                  N|                N|                  |               N|             N|
    +-----------------+--------------------+--------------------+-------------------+-------------------+-------------------+-------------------+------------+------------+----------+---------+-------------------+-----+----+---------+--------------------+-----------+----+----------+-------------------+-----------------+------------------+----------------+--------------+
    only showing top 20 rows
    


                                                                                    


```python
# Import PySpark SQL functions module with an alias 'F'
from pyspark.sql import functions as F
```


```python
# Transform the DataFrame by adding new columns and selecting specific fields

df \
    # Create a new column 'pickup_date' by extracting the date from 'pickup_datetime'
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    
    # Create a new column 'dropoff_date' by extracting the date from 'dropoff_datetime'
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    
    # Select specific columns for further processing or inspection
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    
    # Show the first 20 rows of the resulting DataFrame
    .show()

```

    [Stage 10:===========================================>              (3 + 1) / 4]

    +-----------+------------+------------+------------+
    |pickup_date|dropoff_date|PULocationID|DOLocationID|
    +-----------+------------+------------+------------+
    | 2021-01-30|  2021-01-30|          74|          42|
    | 2021-01-23|  2021-01-23|          75|          51|
    | 2021-01-20|  2021-01-20|         234|         161|
    | 2021-01-20|  2021-01-20|         246|         116|
    | 2021-01-16|  2021-01-16|          18|         247|
    | 2021-01-26|  2021-01-26|          26|          26|
    | 2021-01-27|  2021-01-27|         231|          87|
    | 2021-01-23|  2021-01-23|         229|         148|
    | 2021-01-22|  2021-01-23|         222|          76|
    | 2021-01-09|  2021-01-09|          70|           7|
    | 2021-01-31|  2021-01-31|         219|         191|
    | 2021-01-11|  2021-01-11|         216|         225|
    | 2021-01-09|  2021-01-09|          49|         164|
    | 2021-01-15|  2021-01-15|         107|         158|
    | 2021-01-31|  2021-01-31|          61|          36|
    | 2021-01-26|  2021-01-26|         203|         203|
    | 2021-01-15|  2021-01-15|         116|          47|
    | 2021-01-27|  2021-01-27|         181|          14|
    | 2021-01-30|  2021-01-30|          61|          49|
    | 2021-01-01|  2021-01-01|         262|          41|
    +-----------+------------+------------+------------+
    only showing top 20 rows
    


                                                                                    


```python
# Perform the aggregation: Sum miles traveled by pickup location
aggregated_df = df.groupBy("PULocationID").agg(
    spark_sum("trip_miles").alias("total_miles_traveled")
)
```


```python
# Sort the results in descending order of total miles
aggregated_df = aggregated_df.orderBy(col("total_miles_traveled").desc())
```


```python
# Show the aggregated results
aggregated_df.show()

```

    [Stage 15:===================================================>    (22 + 2) / 24]

    +------------+--------------------+
    |PULocationID|total_miles_traveled|
    +------------+--------------------+
    |         132|  1557488.3880000005|
    |         138|   820273.3120000021|
    |          61|   811149.6109999974|
    |          76|   759131.2820000019|
    |          42|   673048.3449999972|
    |         244|    642341.228999999|
    |          37|   597753.1059999964|
    |          39|   523929.1189999995|
    |          74|   507618.4289999965|
    |          89|  506878.12600000115|
    |         225|   493308.9000000046|
    |           7|   479864.4870000013|
    |          75|  475414.34199999925|
    |          79|   471258.2480000017|
    |          17|   471169.6469999993|
    |         188|  456665.89599999774|
    |         231|   442949.4580000007|
    |          35|  435490.47300000134|
    |         168|   435432.8990000009|
    |          48|  429663.33100000105|
    +------------+--------------------+
    only showing top 20 rows
    


                                                                                    


```python
# Aggregating revenue components by pickup location
revenue_df = df.groupBy("PULocationID").agg(
    spark_sum("base_passenger_fare").alias("total_fare"),
    spark_sum("tips").alias("total_tips"),
    spark_sum("tolls").alias("total_tolls"),
    (spark_sum("base_passenger_fare") + spark_sum("tips") + spark_sum("tolls")).alias("total_revenue")
)
```


```python
# Order results by total revenue in descending order
revenue_df = revenue_df.orderBy(col("total_revenue").desc())
```


```python
# Show the result
revenue_df.show()
```

    [Stage 21:=================================================>      (21 + 3) / 24]

    +------------+------------------+------------------+------------------+------------------+
    |PULocationID|        total_fare|        total_tips|       total_tolls|     total_revenue|
    +------------+------------------+------------------+------------------+------------------+
    |         132| 4421060.100000002|150707.65999999995|266604.16000000294|4838371.9200000055|
    |          61|3549191.7400000263| 83230.73000000005|  56794.3299999998| 3689216.800000026|
    |         138|2623634.6199999996|         114626.52|246236.33000000185|2984497.4700000016|
    |          76|2838696.6900000074| 25652.32000000001| 50450.76999999974| 2914799.780000007|
    |          37|2586615.2200000174| 69947.91000000003|  71450.3799999996| 2728013.510000017|
    |          42|  2509780.78000004| 55064.50000000004|108541.53999999938|2673386.8200000394|
    |         244| 2233489.120000002| 67305.06000000001|125807.67999999982|2426601.8600000017|
    |         225|2177129.1400000183| 52442.77000000001| 47570.77999999983| 2277142.690000018|
    |          79|2049765.3400000252| 79791.22000000002| 89679.80999999975| 2219236.370000025|
    |          17|2068479.2100000281| 54078.18000000002|42499.559999999874| 2165056.950000028|
    |          89| 2051974.040000014| 54366.53000000001|52107.449999999786|2158448.0200000135|
    |         231|1937236.2100000156| 76585.63000000002| 91530.59999999983|2105352.4400000153|
    |         188|1998377.2399999988|          48676.73| 36498.27999999991|2083552.2499999986|
    |          39| 1987947.770000008| 20522.83000000001|31218.349999999948|2039688.9500000079|
    |          74|1874244.7900000163| 42797.23000000001| 91205.78999999944|2008247.8100000157|
    |           7|1799608.0500000115| 74579.75000000003| 82371.62999999944|1956559.4300000109|
    |          75| 1778321.480000016| 53113.84000000002| 90701.25999999944|1922136.5800000157|
    |         181|1747344.8300000078| 98228.06000000004|  50775.3199999998|1896348.2100000076|
    |          48|1679446.6800000162| 68048.81000000001| 147262.0499999995|1894757.5400000159|
    |          35|1792504.2500000251|           13549.9| 35811.67999999992| 1841865.830000025|
    +------------+------------------+------------------+------------------+------------------+
    only showing top 20 rows
    


                                                                                    


```python

# Save the results to Parquet
revenue_df.write.mode("overwrite").parquet("output/revenue_by_pickup_location.parquet")
```

                                                                                    


```python
# Calculate average trip distance and time by base
avg_trip_metrics_df = df.groupBy("dispatching_base_num").agg(
    F.avg("trip_miles").alias("avg_trip_distance"),
    F.avg("trip_time").alias("avg_trip_time")
)

# Show the result
avg_trip_metrics_df.show()

# Save the results to Parquet
avg_trip_metrics_df.write.mode("overwrite").parquet("output/avg_trip_metrics_by_base.parquet")

```

                                                                                    

    +--------------------+------------------+------------------+
    |dispatching_base_num| avg_trip_distance|     avg_trip_time|
    +--------------------+------------------+------------------+
    |              B02876| 4.535434313402564| 947.8165750741119|
    |              B03136|3.8182075976457996| 939.3638309256287|
    |              B02877| 4.479920138191076| 936.3618615601046|
    |              B02869| 4.557642524408418|   956.82496051741|
    |              B02883| 4.549834271641001| 951.7772652585221|
    |              B02835| 4.444018058352365| 931.5443888691793|
    |              B02884| 4.490663047106027| 945.1042402415455|
    |              B02880| 4.555735023872853| 950.5938341738481|
    |              B02878| 4.384423693884554| 928.8864149891191|
    |              B02836|4.4604985573326665| 938.0215723292844|
    |              B02872| 4.417193889465468| 944.6387043764055|
    |              B02512| 4.473853858153785| 964.5206981528792|
    |              B02867| 4.517315830378619| 952.9899010802533|
    |              B02866|4.4954683225877385| 952.0282144635502|
    |              B02871| 4.429145735189426| 957.0779314419013|
    |              B02889| 4.468690745525375| 950.2587785646394|
    |              B02844| 4.691741353383459|1252.9166917293232|
    |              B02510|4.6152262432869575|  972.821324813976|
    |              B02888| 4.496494181658427| 946.7558380552207|
    |              B02682|  4.46830397482579| 944.7508760910326|
    +--------------------+------------------+------------------+
    only showing top 20 rows
    


                                                                                    


```python
# Count trips by drop-off location
dropoff_count_df = df.groupBy("DOLocationID").count().alias("trip_count")

# Order by trip count in descending order
dropoff_count_df = dropoff_count_df.orderBy(col("count").desc())

# Show the top 10 drop-off locations
dropoff_count_df.show(10)

```

    [Stage 52:===================================================>    (22 + 2) / 24]

    +------------+------+
    |DOLocationID| count|
    +------------+------+
    |         265|369502|
    |          61|218654|
    |          76|181207|
    |          37|148401|
    |          42|144159|
    |         244|128205|
    |          17|126426|
    |         225|125420|
    |         188|121635|
    |          89|120126|
    +------------+------+
    only showing top 10 rows
    


                                                                                    


```python
# Extract hour from pickup datetime
hourly_trips_df = df.withColumn("hour", F.hour("pickup_datetime"))

# Count trips by hour
hourly_trip_counts = hourly_trips_df.groupBy("hour").count().alias("trip_count")

# Order by hour
hourly_trip_counts = hourly_trip_counts.orderBy(col("hour"))

# Show the result
hourly_trip_counts.show()
```

    [Stage 58:=================================================>      (21 + 3) / 24]

    +----+------+
    |hour| count|
    +----+------+
    |   0|361590|
    |   1|267724|
    |   2|196304|
    |   3|150020|
    |   4|135427|
    |   5|188020|
    |   6|317002|
    |   7|467825|
    |   8|587343|
    |   9|545605|
    |  10|528413|
    |  11|536807|
    |  12|572737|
    |  13|618757|
    |  14|674211|
    |  15|698975|
    |  16|708869|
    |  17|765136|
    |  18|765787|
    |  19|711875|
    +----+------+
    only showing top 20 rows
    


                                                                                    


```python
# This can improve parallelism and performance for large-scale transformations
df = df.repartition(24)
```


```python
# Verify the new number of partitions
print(f"Number of partitions after repartitioning: {df.rdd.getNumPartitions()}")
```

    [Stage 62:===========================================>              (3 + 1) / 4]

    Number of partitions after repartitioning: 24



```python
# Perform a transformation (e.g., aggregating total miles by pickup location)
aggregated_df = df.groupBy("PULocationID").agg(
    spark_sum("trip_miles").alias("total_miles_traveled")
)
```


```python
# Calculate revenue per mile for each pickup location
pricing_efficiency_df = df.withColumn("total_revenue", F.col("base_passenger_fare") + F.col("tips") + F.col("tolls")) \
    .groupBy("PULocationID").agg(
        (F.sum("total_revenue") / F.sum("trip_miles")).alias("revenue_per_mile")
    )

# Order by revenue per mile in descending order
pricing_efficiency_df = pricing_efficiency_df.orderBy(col("revenue_per_mile").desc())

# Show the result
pricing_efficiency_df.show()

```

    [Stage 65:===================================================>    (22 + 2) / 24]

    +------------+------------------+
    |PULocationID|  revenue_per_mile|
    +------------+------------------+
    |         105| 5.970031545741325|
    |         158| 5.162613742959451|
    |         211| 5.137757542715089|
    |         249| 5.107520722462131|
    |         113| 5.065563055110756|
    |         114| 5.015485077531107|
    |         234| 4.989990119586515|
    |         237| 4.938703931272939|
    |         125|4.9086389504198715|
    |         161|  4.90826174946643|
    |          90|4.8992049671870115|
    |         144| 4.896951486930965|
    |         246| 4.865361593947758|
    |         217| 4.841148664717978|
    |          43| 4.813942657132882|
    |         194| 4.798026733981097|
    |         255| 4.771493134776462|
    |         189| 4.763575819408854|
    |          26| 4.757514567269955|
    |         163| 4.755057410109206|
    +------------+------------------+
    only showing top 20 rows
    


                                                                                    


```python
# End the Spark session
spark.stop()
```


```python

```
## Approach and Performance Considerations
## Approach
This project leverages Apache Spark, a powerful distributed computing framework, to process and analyze a large dataset (308924937 million records) containing trip records. The dataset consists of millions of records, making it an ideal candidate to demonstrate Spark’s ability to handle big data efficiently.
Key transformations and analyses performed include:

1. Miles Traveled by Pickup Location:
- Aggregated total miles (trip_miles) for each pickup location (PULocationID).
- This helped identify areas with the highest travel activity, providing insights into demand hotspots.
2. Revenue Analysis:
- Calculated total revenue generated by combining fare components (base_passenger_fare, tips, and tolls) and grouped the data by pickup location.
- Highlighted locations contributing the most to overall revenue, useful for business strategy.
3. Operational Efficiency by Dispatch Base:
- Computed the average trip distance and time (trip_miles and trip_time) for each dispatching base.
- Enabled a comparative analysis of performance across different bases.
4. Drop-off Location Popularity:
- Counted trips by drop-off location (DOLocationID) to determine the most common destinations.
- Provided actionable insights for optimizing services.
5. Hourly Trip Analysis:
- Extracted the hour from pickup_datetime and analyzed the number of trips by hour.
- Identified peak and off-peak hours, aiding resource planning and scheduling.
6. Pricing Efficiency:
- Calculated revenue per mile for each pickup location to evaluate pricing efficiency.
- This metric offers valuable insights into revenue optimization strategies.
## Performance Considerations
To handle the large dataset efficiently and ensure scalability, the following measures were implemented:

1. Distributed Processing:
- Spark’s ability to process data in parallel across multiple nodes was leveraged to achieve scalability.
- Repartitioning the dataset ensured optimal distribution of data, minimizing bottlenecks during transformations.
2. Resource Optimization:
- Key Spark configurations such as executor.memory and executor.cores were tuned to maximize resource utilization without overloading the system.
- The number of partitions was adjusted based on the dataset size and available resources.
3. Efficient Aggregations:
- Built-in aggregation functions (sum, avg, count) were used for operations like revenue calculation and trip counting, ensuring high performance even with millions of records.
4. Lazy Execution:
- Spark’s lazy evaluation deferred computation until an action (e.g., .show(), .write()) was triggered, allowing the framework to optimize execution plans.
5. Optimized File Format:
- The dataset and results were stored in Parquet, a columnar file format optimized for fast reads and writes.
- This reduced storage requirements and improved I/O performance during queries.
6. Sorting and Filtering:
- Data was sorted and filtered post-aggregation to minimize intermediate data shuffling and improve execution time for transformations.

By combining Spark’s robust distributed processing capabilities with best practices for big data handling, this project demonstrates how large datasets can be processed at scale to extract actionable insights efficiently.

## Conclusion
This project illustrates how to transform and analyze large-scale datasets using Apache Spark while addressing both analytical needs and performance considerations. The results provide critical insights for improving operational efficiency, optimizing resource allocation, and informing business strategies.





  


  
  


  




