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
