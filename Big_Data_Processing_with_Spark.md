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
ls
```

    [0m[01;32m'1. using_MAX_date.png'[0m*                 [01;32mUntitled.ipynb[0m*
     [01;32m1.adding_indexes.png[0m*                  [01;32m'archive (1).zip'[0m*
    [01;32m'2. using CTE.png'[0m*                      [01;32mdimension_model_starschema.png[0m*
     [01;32mBig_Data_Processing_with_Spark.ipynb[0m*   [01;32mfhvhv_tripdata_2021-01.parquet[0m*
    [01;32m'ClickUp - clickup.csv.csv'[0m*             [01;32mquestion2.txt[0m*
     [01;32mER_diagram_model.png[0m*                   [01;32mupload_source_datasets.ipynb[0m*
     [01;32mETL_SCRIPTS.ipynb[0m*                      [01;32mvalidation_1.png[0m*
    [01;32m'Float - allocations.csv.csv'[0m*           [01;32mvalidation_2.png[0m*
    [01;32m'Global Health Statistics.csv'[0m*          [01;32mvalidation_3.png[0m*
     [01;32mLOAD_INTO_EDW.png[0m*                      [01;32mvalidation_4.png[0m*
     [01;32mLOAD_OLD.ipynb[0m*                         [01;32mvalidation_5.png[0m*
    [01;32m'Query optimization question.png'[0m*       [01;32mvalidation_6.png[0m*
    [01;32m'STARSCHEMA_AND ER_CODES.txt'[0m*



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
ls
```

    [0m[01;32m'1. using_MAX_date.png'[0m*                 [01;32mUntitled.ipynb[0m*
     [01;32m1.adding_indexes.png[0m*                  [01;32m'archive (1).zip'[0m*
    [01;32m'2. using CTE.png'[0m*                      [01;32mdimension_model_starschema.png[0m*
     [01;32mBig_Data_Processing_with_Spark.ipynb[0m*   [01;32mfhvhv_tripdata_2021-01.parquet[0m*
    [01;32m'ClickUp - clickup.csv.csv'[0m*             [01;32mhead.parquet[0m*
     [01;32mER_diagram_model.png[0m*                   [01;32mquestion2.txt[0m*
     [01;32mETL_SCRIPTS.ipynb[0m*                      [01;32mupload_source_datasets.ipynb[0m*
    [01;32m'Float - allocations.csv.csv'[0m*           [01;32mvalidation_1.png[0m*
    [01;32m'Global Health Statistics.csv'[0m*          [01;32mvalidation_2.png[0m*
     [01;32mLOAD_INTO_EDW.png[0m*                      [01;32mvalidation_3.png[0m*
     [01;32mLOAD_OLD.ipynb[0m*                         [01;32mvalidation_4.png[0m*
    [01;32m'Query optimization question.png'[0m*       [01;32mvalidation_5.png[0m*
    [01;32m'STARSCHEMA_AND ER_CODES.txt'[0m*           [01;32mvalidation_6.png[0m*



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
