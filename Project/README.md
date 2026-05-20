# Big Data Project 2025/26 | Matic Zadobovšek, Matija Bažec

## Tasks

### T0.
- In this task we have focused on gathering all the needed information and getting stuff uploaded to Snowflake.
- On the Arnes HPC cluster we have already had data available in the `/d/hpc/projects/FRI/bigdata/data/Taxi` directory, but the flles are only available up to January 2025, so our first task was to get the missing files for `yellow`, `green`, `fhv` and `fhvhv` up until February 2026. 
- Under `scripts/t0/missing_data.py`you can see the script that does this, and saves the missing files under the `data_missing` directory on the HPC cluster. Our directory can be access under `/d/hpc/projects/FRI/bigdata/students/mz1034/Project`, where you will find the `data_missing` directory and the `t0_missing_data.py` script as well.
- On Snowflake, we have created a warehouse named `BIGDATA_MZMB_WH`, and set up a database named `BIGDATA_TAXI_MZMB`.

![alt text](images/t0/warehouse.png)

![alt text](images/t0/db.png)
- We have added three schemas to the database: `BRONZE`, `SILVER` and `GOLD`.

![alt text](images/t0/schemas.png)
- Now we have used the `t0.sql`script to prepare the 4 stages under the `BRONZE` schema, so we could upload the parquet files to the stages. We have `YELLOW_RAW_STAGE`, `GREEN_RAW_STAGE`, `FHV_RAW_STAGE` and `FHWHV_RAW_STAGE` stages, each for the corresponding taxi data.

![alt text](images/t0/stages.png)
- `scripts/t0/snowflake.py` was then used to upload all the parquet files from the given `/d/hpc/projects/FRI/bigdata/data/Taxi` and `data_missing` directories to the corresponding stages on Snowflake.
- When all parquet files have been uploaded to stages, we have created 4 new tables, which list the column names and their data types. We ended up with `T0_YELLOW_SCHEMA`, `T0_GREEN_SCHEMA`, `T0_FHV_SCHEMA` and `T0_FHWHV_SCHEMA` tables, which are all under the `BRONZE` schema. We have also added `T0_SCHEMA_ALL` table, which does a union of all the previous tables, so we have a complete overview of all the columns and their data types in one place.

![alt text](images/t0/tables.png)
- So far we have done no filtering or transformations, just getting all needed parquet files and uploading them into the `BRONZE` layer on Snowflake, where we have the tables with column names and types, so we can easily compare between files, because there are differences between the attribute names and types, depending on the year of the data.

### T1.

- Here we have focused on the `SILVER` layer, where we have taken care of schemas and attribute names differences between the files. Everything was performed in Snowflake.
- Main file that we are running is the `scripts/t1/t1.sql` file, which creates the tables in the `SILVER` layer.
- Main result are the 4 tables, `YELLOW_TRIPS`, `GREEN_TRIPS`, `FHV_TRIPS` and `FHWHV_TRIPS`, which are all under the `SILVER` schema. We have taken care of the differences, so these tables are unified and ready for further analysis. Note that we didn't care about the data quality issues here, but only about getting the data in the right format.

![alt text](images/t1/tables.png)
- Data columns that had different formats in original parquet files were unified using the `TRY_TO_DOUBLE` or `TRY_TO_NUMBER` functions.
- If we had different column names for the same attribute (e.g. `airport_fee` vs `Airport_fee`), we used the `COALESCE` function to get the value from the column that is not null, so we have only one column in the `SILVER` layer for each attribute.
- We also added attributes for year and month, which we got from the pickup datetime column.
- `YELLOW_TRIPS` ended up with: 
```sql
create or replace TABLE BIGDATA_TAXI_MZMB.SILVER.YELLOW_TRIPS cluster by (pickup_year, pickup_month)(
	VENDOR_ID NUMBER(38,0),
	PICKUP_DATETIME TIMESTAMP_NTZ(6),
	DROPOFF_DATETIME TIMESTAMP_NTZ(6),
	PASSENGER_COUNT FLOAT,
	TRIP_DISTANCE FLOAT,
	RATECODE_ID FLOAT,
	STORE_AND_FWD_FLAG VARCHAR(16777216),
	PU_LOCATION_ID NUMBER(38,0),
	DO_LOCATION_ID NUMBER(38,0),
	PAYMENT_TYPE NUMBER(38,0),
	FARE_AMOUNT FLOAT,
	EXTRA FLOAT,
	MTA_TAX FLOAT,
	TIP_AMOUNT FLOAT,
	TOLLS_AMOUNT FLOAT,
	IMPROVEMENT_SURCHARGE FLOAT,
	TOTAL_AMOUNT FLOAT,
	CONGESTION_SURCHARGE FLOAT,
	AIRPORT_FEE FLOAT,
	CBD_CONGESTION_FEE FLOAT,
	PICKUP_YEAR NUMBER(4,0),
	PICKUP_MONTH NUMBER(2,0),
	SOURCE_FILE VARCHAR(16777216)
);
```
![alt text](images/t1/preview_yellow.png)
- `GREEN_TRIPS` ended up with:
```sql
create or replace TABLE BIGDATA_TAXI_MZMB.SILVER.GREEN_TRIPS cluster by (pickup_year, pickup_month)(
	VENDOR_ID NUMBER(38,0),
	PICKUP_DATETIME TIMESTAMP_NTZ(6),
	DROPOFF_DATETIME TIMESTAMP_NTZ(6),
	STORE_AND_FWD_FLAG VARCHAR(16777216),
	RATECODE_ID FLOAT,
	PU_LOCATION_ID NUMBER(38,0),
	DO_LOCATION_ID NUMBER(38,0),
	PASSENGER_COUNT FLOAT,
	TRIP_DISTANCE FLOAT,
	FARE_AMOUNT FLOAT,
	EXTRA FLOAT,
	MTA_TAX FLOAT,
	TIP_AMOUNT FLOAT,
	TOLLS_AMOUNT FLOAT,
	EHAIL_FEE FLOAT,
	IMPROVEMENT_SURCHARGE FLOAT,
	TOTAL_AMOUNT FLOAT,
	PAYMENT_TYPE NUMBER(38,0),
	TRIP_TYPE NUMBER(38,0),
	CONGESTION_SURCHARGE FLOAT,
	CBD_CONGESTION_FEE FLOAT,
	PICKUP_YEAR NUMBER(4,0),
	PICKUP_MONTH NUMBER(2,0),
	SOURCE_FILE VARCHAR(16777216)
);
```
![alt text](images/t1/preview_green.png)
- `FHV_TRIPS` ended up with:
```sql
create or replace TABLE BIGDATA_TAXI_MZMB.SILVER.FHV_TRIPS cluster by (pickup_year, pickup_month)(
	DISPATCHING_BASE_NUM VARCHAR(16777216),
	PICKUP_DATETIME TIMESTAMP_NTZ(9),
	DROPOFF_DATETIME TIMESTAMP_NTZ(9),
	PU_LOCATION_ID NUMBER(38,0),
	DO_LOCATION_ID NUMBER(38,0),
	SR_FLAG NUMBER(38,0),
	AFFILIATED_BASE_NUMBER VARCHAR(16777216),
	PICKUP_YEAR NUMBER(4,0),
	PICKUP_MONTH NUMBER(2,0),
	SOURCE_FILE VARCHAR(16777216)
);
```
![alt text](images/t1/preview_fhv.png)
- `FHWHV_TRIPS` ended up with:
```sql
create or replace TABLE BIGDATA_TAXI_MZMB.SILVER.FHVHV_TRIPS cluster by (pickup_year, pickup_month)(
	HVFHS_LICENSE_NUM VARCHAR(16777216),
	DISPATCHING_BASE_NUM VARCHAR(16777216),
	ORIGINATING_BASE_NUM VARCHAR(16777216),
	REQUEST_DATETIME TIMESTAMP_NTZ(6),
	ON_SCENE_DATETIME TIMESTAMP_NTZ(6),
	PICKUP_DATETIME TIMESTAMP_NTZ(6),
	DROPOFF_DATETIME TIMESTAMP_NTZ(6),
	PU_LOCATION_ID NUMBER(38,0),
	DO_LOCATION_ID NUMBER(38,0),
	TRIP_MILES FLOAT,
	TRIP_TIME NUMBER(38,0),
	BASE_PASSENGER_FARE FLOAT,
	TOLLS FLOAT,
	BCF FLOAT,
	SALES_TAX FLOAT,
	CONGESTION_SURCHARGE FLOAT,
	AIRPORT_FEE FLOAT,
	TIPS FLOAT,
	DRIVER_PAY FLOAT,
	SHARED_REQUEST_FLAG VARCHAR(16777216),
	SHARED_MATCH_FLAG VARCHAR(16777216),
	ACCESS_A_RIDE_FLAG VARCHAR(16777216),
	WAV_REQUEST_FLAG VARCHAR(16777216),
	WAV_MATCH_FLAG VARCHAR(16777216),
	CBD_CONGESTION_FEE FLOAT,
	PICKUP_YEAR NUMBER(4,0),
	PICKUP_MONTH NUMBER(2,0),
	SOURCE_FILE VARCHAR(16777216)
);
```
![alt text](images/t1/preview_fhvhv.png)

