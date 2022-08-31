Code Assessment
=============

This is just a demo ETL batch process that takes the input data in JSON format, parses it, performs some filtering on it and then finally save the output data to a file.
The output data is stored in the Parquet format. In this case I chose Parquet as the only file format for this use case and the choice is obvious since the data contains a large number of fields

###  Schema Decision
During the initial triage of the data, it is discovered that the underlying data has verying schema and may contains duplicate fields names
So in order for the dataframe to accept duplicate fields names, we need to set spark.sql.caseSensitive to true to allow the case sensitive names to be treated differently. 
As highlighted above, since the schema evolves with each underlying JSON file, it's important to not enforce schema explicitly to allow it to discover new fields
Below is the current state of the input schema.
root
 |-- eventData: struct (nullable = true)
 |    |-- geoFenceCollection: string (nullable = true)
 |    |-- geoFenceName: string (nullable = true)
 |    |-- geofence: struct (nullable = true)
 |    |    |-- featureCollectionId: string (nullable = true)
 |    |    |-- featureId: string (nullable = true)
 |    |    |-- properties: struct (nullable = true)
 |    |    |    |-- address: string (nullable = true)
 |    |    |    |-- address2: string (nullable = true)
 |    |    |    |-- address3: string (nullable = true)
 |    |    |    |-- centroid: array (nullable = true)
 |    |    |    |    |-- element: double (containsNull = true)
 |    |    |    |-- comment: string (nullable = true)
 |    |    |    |-- createdBy: string (nullable = true)
 |    |    |    |-- createdTimestamp: string (nullable = true)
 |    |    |    |-- lastModifiedBy: string (nullable = true)
 |    |    |    |-- lastModifiedTimestamp: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- pool: string (nullable = true)
 |    |-- latitude: string (nullable = true)
 |    |-- location: struct (nullable = true)
 |    |    |-- latitude: double (nullable = true)
 |    |    |-- longitude: double (nullable = true)
 |    |-- longitude: string (nullable = true)
 |    |-- sensor: string (nullable = true)
 |    |-- sensorData: struct (nullable = true)
 |    |    |-- batteryLevel: double (nullable = true)
 |    |    |-- batteryRange: long (nullable = true)
 |    |    |-- evBatteryLevel: double (nullable = true)
 |    |    |-- fuelAmount: double (nullable = true)
 |    |    |-- fuelLevel: double (nullable = true)
 |    |    |-- mileage: double (nullable = true)
 |    |    |-- remainingLife: double (nullable = true)
 |    |    |-- status: string (nullable = true)
 |    |    |-- wheels: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- axle: long (nullable = true)
 |    |    |    |    |-- position: string (nullable = true)
 |    |    |    |    |-- pressure: double (nullable = true)
 |    |    |    |    |-- side: string (nullable = true)
 |    |    |    |    |-- uom: string (nullable = true)
 |    |-- value: boolean (nullable = true)
 |-- eventId: string (nullable = true)
 |-- geofenceAlertTS: string (nullable = true)
 |-- subType: string (nullable = true)
 |-- subtype: string (nullable = true)
 |-- timestamp: string (nullable = true)
 |-- type: string (nullable = true)
 |-- vehicleId: string (nullable = true)

Output schema
root
 |-- geoFenceCollection: string (nullable = true)
 |-- geoFenceName: string (nullable = true)
 |-- gf_featureCollectionId: string (nullable = true)
 |-- gf_featureId: string (nullable = true)
 |-- gf_address: string (nullable = true)
 |-- gf_address2: string (nullable = true)
 |-- gf_address3: string (nullable = true)
 |-- gf_centroid: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- gf_comment: string (nullable = true)
 |-- gf_createdBy: string (nullable = true)
 |-- gf_createdTimestamp: timestamp (nullable = true)
 |-- gf_lastModifiedBy: string (nullable = true)
 |-- gf_lastModifiedTimestamp: timestamp (nullable = true)
 |-- gf_name: string (nullable = true)
 |-- gf_pool: string (nullable = true)
 |-- latitude: string (nullable = true)
 |-- longitude: string (nullable = true)
 |-- sensor: string (nullable = true)
 |-- sensor_batteryLevel: double (nullable = true)
 |-- sensor_batteryRange: long (nullable = true)
 |-- sensor_evBatteryLevel: double (nullable = true)
 |-- sensor_fuelAmount: double (nullable = true)
 |-- sonsor_fuelLevel: double (nullable = true)
 |-- sonsor_mileage: double (nullable = true)
 |-- sensor_remainingLife: double (nullable = true)
 |-- sensor_status: string (nullable = true)
 |-- sensor_whl_axle: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- sensor_whl_position: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- sensor_whl_pressure: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- sensor_whl_side: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- sensor_whl_uom: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- eventId: string (nullable = true)
 |-- geofenceAlertTS: string (nullable = true)
 |-- subType: string (nullable = true)
 |-- event_time: timestamp (nullable = true)
 |-- type: string (nullable = true)
 |-- vehicleId: string (nullable = true)

###  Partition Strategy
Based on the sampling of data, it was observed that sometime the column value remains empty for most of the field. 
To select a right candidate for the partition key, here are the four characteristics I have considered 
1. Not nullable field
2. Low cardinality
3. Suit for business use case with minimum response time as we would need low read scan while querying the data
4. Data spread should be minimum to avoid one partition having all the data (Addressing potential data hotspot/skewness issues)
+-------------------+----------+--------+
|                 dt|      type|count(1)|
+-------------------+----------+--------+
|2021-12-09 11:00:00|  LOCATION|     371|
|2021-12-30 07:00:00|DIAGNOSTIC|      15|
|2021-11-16 11:00:00|  LOCATION|       1|
|2021-11-14 10:00:00|DIAGNOSTIC|      82|
|2021-11-15 05:00:00|  GEOFENCE|      18|
|2021-10-16 06:00:00|DIAGNOSTIC|     286|
|2021-12-12 07:00:00|DIAGNOSTIC|     442|
|2021-10-10 08:00:00|  LOCATION|       1|
|2021-12-28 09:00:00|  LOCATION|      10|
|2021-10-15 11:00:00|  LOCATION|      73|
+-------------------+----------+--------+

In some cases, I am seeing the location partition contains a low data volume. In a real world solution, ideally this should contain more records. If this is not the case, however, there is still an opportunity to improve the partition strategy. 

###  Configuration
The columns names to be projected from the query while reading the raw data are specified in the configuraion file. This is done with the objective of supplying columns names without changing the pyspark script if the underlying schema changes.
With each scheduled run we keep track of the input schema. If a change in schema is observed after an issue occurs, one can follow through the log file to see the updated schema.


###  Setup
Download the git repo
```
git clone https://github.com/chandanparihar1986/code_assessment_250622.git
```


###  How to Build
```
Pre-requisite
    * Make sure the pyspark package is installed in the current python environment i.e. venv
    * pip install pyspark

cd <local_git_repo_path>
python3 -m main.py
```

###  Run the test
```
cd <local_git_repo_path>
python -m unittest tests/test_assessment.py 
```

###  Sample output


###  Out of scope
Parameterization using confile file 
   * All hardcoded values must be passed through the config file i.e. source, target, file path, etc.

CICD - stage or prod deployment

END to End pipeline   (Ingestion, Integration, Processing, Storage & Visualization)

###  Assumption
The ETL pipeline intends to work on large volume of data, possibily caturing memeory stats of millions of hosts around the world at a high frequency i.e. 1 MM/s


###  Performance
To increase the performance of the ETL pipeline, given the raw data contains mainly the numbers, we may consider catching the raw data on executor memory with a predefined partition i.e. say on Host column with low cardinality.
This can be done using spark's caching APIs i.e. df.cache() or df.persist(DISK/MEMORY). 
Also second reason to catch the raw data would be to utilize the executor memory as much as possible since the computation on the data frame is relatively simple
which would most likely not take up a lot of memory.
However, this should only be explored when experiencing slowness in the execution of the ETL pipeline or the pipeline is breaching the SLAs as catching data in memory has its own challenges i.e. out of memory exception, data getting spilled over to disk while shuffling, etc.