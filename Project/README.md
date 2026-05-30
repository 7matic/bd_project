# Big Data Project 2025/26 | Matic Zadobovšek, Matija Bažec

## Getting started (internal instructions)

- Most information regarding our dataset can be found on https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page website. Always refer to that.
- First you should go through the `Tasks` section below, to see what we have done. You should follow similar structure regarding organization in the next steps as well.
- Work between the two of us is distributed between the tasks, and each task has a team member name written in the title, so you can see who is supposed to do what.
- Project instructions are available in the `instructions.pdf` file.
- Check the `Project structure` section below to see how our project is organized, and how you should continue to save obtained results and code.
- Whenever you complete a task, you should update the `README.md` file with what you did, add key results and images. This will help us with the final report, because we will already have most of the content prepared here.
- On the HPC cluster our work can be found under `/d/hpc/projects/FRI/bigdata/students/mz1034/Project`, but majority of the work is done on Snowflake, and it should remain like that.
- When you prepare visualizations avoid having every word capitalized (This Looks Extremely Ugly).
- When working on Snowflake, you should work within your own workspace, because you can only share workspace with users with the same role, and that would mean everyone would have access to our workspace, which is not ideal. Just make sure to upload your final SQL and other code files to our repository, so we have everything in one place and we can easily run each other's code if needed.

## Project structure

```text
Project/
├── README.md
├── instructions.pdf
│
├── images/
│   ├── t0/
│   │   ├── warehouse.png
│   │   ├── db.png
│   │   ├── schemas.png
│   │   ├── stages.png
│   │   └── tables.png
│   │
│   ├── t1/
│   │   ├── tables.png
│   │   ├── preview_yellow.png
│   │   ├── preview_green.png
│   │   ├── preview_fhv.png
│   │   └── preview_fhvhv.png
│   │
│   └── t2/
│       ├── yellow_chart.png
│       ├── green_chart.png
│       ├── fhv_chart.png
│       └── fhvhv_chart.png
│
└── scripts/
    ├── t0/
    │   ├── missing_data.py
    │   ├── snowflake.py
    │   └── t0.sql
    │
    ├── t1/
    │   └── t1.sql
    │
    └── t2/
        └── t2.sql
```

## Tasks

### T0. MATIC
- In this task we have focused on gathering all the needed information and getting stuff uploaded to Snowflake.
- On the Arnes HPC cluster we have already had data available in the `/d/hpc/projects/FRI/bigdata/data/Taxi` directory, but the files are only available up to January 2025, so our first task was to get the missing files for `yellow`, `green`, `fhv` and `fhvhv` up until February 2026. 
- Under `scripts/t0/missing_data.py`you can see the script that does this, and saves the missing files under the `data_missing` directory on the HPC cluster. Our directory can be accessed under `/d/hpc/projects/FRI/bigdata/students/mz1034/Project`, where you will find the `data_missing` directory and the `t0_missing_data.py` script as well.
- On Snowflake, we have created a warehouse named `BIGDATA_MZMB_WH`, and set up a database named `BIGDATA_TAXI_MZMB`.

![alt text](images/t0/warehouse.png)

![alt text](images/t0/db.png)
- We have added three schemas to the database: `BRONZE`, `SILVER` and `GOLD`.

![alt text](images/t0/schemas.png)
- Now we have used the `t0.sql` script to prepare the 4 stages under the `BRONZE` schema, so we could upload the parquet files to the stages. We have `YELLOW_RAW_STAGE`, `GREEN_RAW_STAGE`, `FHV_RAW_STAGE` and `FHVHV_RAW_STAGE` stages, each for the corresponding taxi data.

![alt text](images/t0/stages.png)
- `scripts/t0/snowflake.py` was then used to upload all the parquet files from the given `/d/hpc/projects/FRI/bigdata/data/Taxi` and `data_missing` directories to the corresponding stages on Snowflake.
- When all parquet files have been uploaded to stages, we have created 4 new tables, which list the column names and their data types. We ended up with `T0_YELLOW_SCHEMA`, `T0_GREEN_SCHEMA`, `T0_FHV_SCHEMA` and `T0_FHVHV_SCHEMA` tables, which are all under the `BRONZE` schema. We have also added `T0_SCHEMA_ALL` table, which does a union of all the previous tables, so we have a complete overview of all the columns and their data types in one place.

![alt text](images/t0/tables.png)
- So far we have done no filtering or transformations, just getting all needed parquet files and uploading them into the `BRONZE` layer on Snowflake, where we have the tables with column names and types, so we can easily compare between files, because there are differences between the attribute names and types, depending on the year of the data.

### T1. MATIC

- Here we have focused on the `SILVER` layer, where we have taken care of schemas and attribute names differences between the files. Everything was performed in Snowflake.
- Main file that we are running is the `scripts/t1/t1.sql` file, which creates the tables in the `SILVER` layer.
- Main result are the 4 tables, `YELLOW_TRIPS`, `GREEN_TRIPS`, `FHV_TRIPS` and `FHVHV_TRIPS`, which are all under the `SILVER` schema. We have taken care of the differences, so these tables are unified and ready for further analysis. Note that we didn't care about the data quality issues here, but only about getting the data in the right format.

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
- `FHVHV_TRIPS` ended up with:
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

## T2. MATIC

- Here we did the data quality inspection, to check for potential issues in our data.
- Main file that we ran on Snowflake was the `scripts/t2/t2.sql` file.
- Work here went under the `GOLD` layer, where we created 3 new tables for each dataset: `QUALITY_FILE_YEAR`, `QUALITY_YEAR`, `QUALITY_CHART`.
- `QUALITY_FILE_YEAR` is basically the map step, where we calculate the quality statistics for each parquet file and pickup year.
- `QUALITY_YEAR` is the reduce step, where we aggregate those file-level results by year.
- `QUALITY_CHART` is the long format table, where we have `PICKUP_YEAR`, `ISSUE_TYPE`, `ISSUE_COUNT` and `TOTAL_ROWS`, so we can easily visualize the results.
- For charts we mostly used percentages instead of raw counts, because otherwise the biggest datasets/issues completely dominate the visualization.

### T2 results for Yellow Taxi

- For `YELLOW` we were interested in: rows outside the valid range (< 2012 or > 2026), pickup outside source file month (+/- 1 day tolerance), pickup = dropoff, dropoff time < pickup time, trip_distance = 0, passenger_count <= 0, fare_amount < 0, and total_amount < 0.
- We intentionally did not remove rows where `passenger_count` is null, because for many later records this value is missing and it would remove too much data for no good reason. We only treat passenger counts <= 0 as suspicious.
- We noticed that there are some weird pickup years like 2001, 2002, 2008, 2098, etc., but these usually contain a very small number of rows compared to valid years.

|PICKUP_YEAR|TOTAL_ROWS                   |UNEXPECTED_PICKUP_YEAR|PICKUP_OUTSIDE_SOURCE_MONTH                  |PICKUP_EQUALS_DROPOFF|DROPOFF_BEFORE_PICKUP|ZERO_TRIP_DISTANCE|NONPOSITIVE_PASSENGER_COUNT|NEGATIVE_FARE_AMOUNT|NEGATIVE_TOTAL_AMOUNT|
|-----------|-----------------------------|----------------------|---------------------------------------------|---------------------|---------------------|------------------|---------------------------|--------------------|---------------------|
|2001       |27                           |27                    |27                                           |0                    |0                    |4                 |0                          |0                   |0                    |
|2002       |498                          |498                   |498                                          |0                    |0                    |84                |0                          |18                  |18                   |
|2003       |50                           |50                    |50                                           |1                    |0                    |19                |0                          |0                   |0                    |
|2004       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2007       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2008       |771                          |771                   |771                                          |4                    |0                    |42                |0                          |6                   |6                    |
|2009       |1304                         |1304                  |1304                                         |11                   |0                    |77                |0                          |8                   |8                    |
|2010       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2011       |4                            |4                     |4                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2012       |171359008                    |0                     |1                                            |385642               |1292                 |958564            |1242327                    |0                   |0                    |
|2013       |171816340                    |0                     |0                                            |494450               |10996                |1119568           |4938                       |3080                |3064                 |
|2014       |165447580                    |0                     |1                                            |356083               |8907                 |954350            |7995                       |18523               |18533                |
|2015       |146039232                    |0                     |1                                            |171080               |3917                 |876727            |40683                      |51339               |51353                |
|2016       |131131805                    |0                     |0                                            |141290               |2180                 |773693            |13431                      |55661               |55664                |
|2017       |113500386                    |0                     |5                                            |107007               |1447                 |743181            |166087                     |56091               |56094                |
|2018       |102870524                    |0                     |1905                                         |89333                |1044                 |704268            |933067                     |67781               |67780                |
|2019       |84597309                     |0                     |1344                                         |77873                |1071                 |743602            |1525798                    |170050              |169986               |
|2020       |24649266                     |0                     |902                                          |17401                |20011                |330084            |489385                     |92833               |92683                |
|2021       |30903983                     |0                     |459                                          |21710                |37786                |407782            |703955                     |139322              |139710               |
|2022       |39655622                     |0                     |127                                          |18422                |13613                |573980            |763344                     |252883              |255689               |
|2023       |38310138                     |0                     |247                                          |13094                |2475                 |773445            |583005                     |381649              |376882               |
|2024       |41169691                     |0                     |7                                            |11935                |1575                 |776306            |401354                     |731023              |609343               |
|2025       |48722584                     |0                     |5                                            |544069               |2235                 |1402958           |260062                     |2848621             |973722               |
|2026       |7124754                      |0                     |5                                            |85665                |3                    |248988            |27671                      |66183               |67274                |
|2028       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2029       |7                            |7                     |7                                            |0                    |0                    |1                 |0                          |0                   |0                    |
|2031       |2                            |2                     |2                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2032       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2033       |3                            |3                     |3                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2037       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2038       |4                            |4                     |4                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2041       |3                            |3                     |3                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2042       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2053       |2                            |2                     |2                                            |1                    |0                    |1                 |0                          |0                   |0                    |
|2058       |3                            |3                     |3                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2066       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2070       |1                            |1                     |1                                            |0                    |0                    |1                 |0                          |0                   |0                    |
|2084       |8                            |8                     |8                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2088       |2                            |2                     |2                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2090       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |
|2098       |1                            |1                     |1                                            |0                    |0                    |0                 |0                          |0                   |0                    |

![alt text](images/t2/yellow_chart.png)

- Based on this, we created `SILVER.YELLOW_TRIPS_CLEAN`. This view keeps only rows with:
  - pickup year between 2012 and 2026,
  - pickup time within the expected source file month (+/- 1 day),
  - dropoff after pickup,
  - trip distance > 0,
  - non-negative fare and total amount,
  - passenger count either null or > 0.

### T2 results for Green Taxi

- For `GREEN` we used almost the same checks as for Yellow, but with the valid year range changed to 2014-2026.
- Checks used: rows outside the valid range (< 2014 or > 2026), pickup outside source file month (+/- 1 day tolerance), pickup = dropoff, dropoff time < pickup time, trip_distance = 0, passenger_count <= 0, fare_amount < 0, and total_amount < 0.
- Results are generally similar to Yellow, but the dataset is much smaller.
- We again saw some weird years outside the expected range, but the number of such rows is very small.

|PICKUP_YEAR|TOTAL_ROWS|UNEXPECTED_PICKUP_YEAR|PICKUP_OUTSIDE_SOURCE_MONTH|PICKUP_EQUALS_DROPOFF|DROPOFF_BEFORE_PICKUP|ZERO_TRIP_DISTANCE|NONPOSITIVE_PASSENGER_COUNT|NEGATIVE_FARE_AMOUNT|NEGATIVE_TOTAL_AMOUNT|
|-----------|----------|----------------------|---------------------------|---------------------|---------------------|------------------|---------------------------|--------------------|---------------------|
|2008       |115       |115                   |115                        |1                    |0                    |22                |0                          |1                   |1                    |
|2009       |315       |315                   |315                        |1                    |0                    |39                |0                          |3                   |3                    |
|2010       |348       |348                   |348                        |1                    |0                    |17                |0                          |1                   |1                    |
|2012       |3         |3                     |3                          |0                    |0                    |0                 |0                          |0                   |0                    |
|2014       |15837009  |0                     |0                          |9978                 |202                  |241457            |3346                       |11626               |11626                |
|2015       |19233765  |0                     |0                          |12831                |259                  |273839            |6100                       |30852               |30854                |
|2016       |16385541  |0                     |0                          |11225                |101                  |218030            |4375                       |32982               |32985                |
|2017       |11736906  |0                     |321                        |6640                 |51                   |135719            |1842                       |26636               |26636                |
|2018       |8899314   |0                     |519                        |5477                 |28                   |103608            |12371                      |23060               |23055                |
|2019       |6300814   |0                     |167                        |14270                |302                  |138590            |11688                      |19906               |19879                |
|2020       |1734166   |0                     |30                         |2448                 |94                   |64464             |3199                       |4422                |4415                 |
|2021       |1068729   |0                     |15                         |1212                 |2                    |40486             |1431                       |2090                |2114                 |
|2022       |840394    |0                     |126                        |1500                 |1                    |49391             |3517                       |2163                |2195                 |
|2023       |787055    |0                     |76                         |1033                 |0                    |39493             |6143                       |2217                |2239                 |
|2024       |660204    |0                     |61                         |659                  |2                    |34573             |6792                       |2143                |2173                 |
|2025       |591369    |0                     |76                         |566                  |1344                 |24439             |8256                       |1736                |1774                 |
|2026       |77655     |0                     |6                          |53                   |1                    |2676              |1129                       |225                 |228                  |
|2030       |2         |2                     |2                          |0                    |0                    |0                 |0                          |0                   |0                    |
|2035       |1         |1                     |1                          |0                    |0                    |1                 |0                          |0                   |0                    |
|2041       |1         |1                     |1                          |0                    |0                    |1                 |0                          |0                   |0                    |
|2062       |1         |1                     |1                          |0                    |0                    |1                 |0                          |0                   |0                    |
|2081       |1         |1                     |1                          |0                    |0                    |0                 |0                          |0                   |0                    |

![alt text](images/t2/green_chart.png)

- Based on this, we created `SILVER.GREEN_TRIPS_CLEAN`, using the same cleaning logic as Yellow, but with the valid year range set to 2014-2026.

### T2 results for FHV

- `FHV` has fewer columns than Yellow and Green, so there are fewer possible checks.
- For `FHV` we checked: rows outside the valid range (< 2015 or > 2026), pickup outside source file month (+/- 1 day tolerance), pickup = dropoff, dropoff time < pickup time, missing pickup location, missing dropoff location, and missing dispatching base.
- The results for FHV are different from the other datasets. Pickup timestamps are mostly fine, but dropoff timestamps are very problematic in the early years. For example, many rows in 2015 and 2016 have dropoff times before pickup times.
- We also saw a lot of missing pickup/dropoff locations, especially in older years.
- Because of this, we decided not to use one very strict clean view for all FHV analyses. If we filtered out every row with bad dropoff time or missing location, we would lose a huge amount of data.

|PICKUP_YEAR|TOTAL_ROWS|UNEXPECTED_PICKUP_YEAR|PICKUP_OUTSIDE_SOURCE_MONTH|PICKUP_EQUALS_DROPOFF|DROPOFF_BEFORE_PICKUP|MISSING_PICKUP_LOCATION|MISSING_DROPOFF_LOCATION|MISSING_DISPATCHING_BASE|
|-----------|----------|----------------------|---------------------------|---------------------|---------------------|-----------------------|------------------------|------------------------|
|2015       |63388532  |0                     |0                          |0                    |63387762             |17281411               |62151232                |0                       |
|2016       |132114083 |0                     |0                          |0                    |132112979            |39844413               |130428240               |0                       |
|2017       |192309557 |0                     |0                          |0                    |72807164             |42022338               |84283342                |1                       |
|2018       |260874754 |0                     |1                          |205                  |7509                 |37266902               |20000408                |49                      |
|2019       |43261276  |0                     |0                          |12                   |66                   |2024082                |723705                  |3                       |
|2020       |14945465  |0                     |0                          |0                    |0                    |3573702                |591444                  |0                       |
|2021       |14805265  |0                     |0                          |0                    |0                    |12106088               |2183776                 |0                       |
|2022       |14511664  |0                     |0                          |0                    |0                    |11020778               |2311546                 |0                       |
|2023       |15858639  |0                     |0                          |0                    |1                    |12333652               |2793673                 |0                       |
|2024       |17630326  |0                     |0                          |0                    |0                    |14015589               |2915588                 |0                       |
|2025       |24860987  |0                     |0                          |44                   |292                  |20391429               |4199778                 |0                       |
|2026       |1941722   |0                     |0                          |0                    |1                    |1646462                |216251                  |0                       |

![alt text](images/t2/fhv_chart.png)


- We created a lighter default clean view `SILVER.FHV_TRIPS_CLEAN`.
- This view only keeps rows with:
  - pickup year between 2015 and 2026,
  - pickup time within the expected source file month (+/- 1 day),
  - non-empty dispatching base number.
- This way we do not throw away too much FHV data too early.

### T2 results for FHVHV

- `FHVHV` has many more useful columns than FHV, so we can check more things.
- For `FHVHV` we checked: rows outside the valid range (< 2019 or > 2026), pickup outside source file month (+/- 1 day tolerance), pickup = dropoff, dropoff time < pickup time, trip_miles <= 0, trip_time <= 0, base_passenger_fare < 0, driver_pay < 0, missing pickup/dropoff location, and missing base information.
- Compared to FHV, this dataset looks much cleaner.
- Pickup years and source months are consistent, missing locations are basically not an issue, and most problems are very small compared to the total number of rows.

|PICKUP_YEAR|TOTAL_ROWS|UNEXPECTED_PICKUP_YEAR|PICKUP_OUTSIDE_SOURCE_MONTH|PICKUP_EQUALS_DROPOFF|DROPOFF_BEFORE_PICKUP|NONPOSITIVE_TRIP_MILES|NONPOSITIVE_TRIP_TIME|NEGATIVE_BASE_PASSENGER_FARE|NEGATIVE_DRIVER_PAY|MISSING_LOCATION|MISSING_BASE_INFO|
|-----------|----------|----------------------|---------------------------|---------------------|---------------------|----------------------|---------------------|----------------------------|-------------------|----------------|-----------------|
|2019       |234630264 |0                     |0                          |1804                 |7926                 |514155                |16843                |594752                      |653                |0               |2085             |
|2020       |143309871 |0                     |0                          |452                  |7444                 |152429                |669                  |165773                      |1587               |0               |0                |
|2021       |174596652 |0                     |0                          |613                  |7558                 |53147                 |953                  |200924                      |11221              |0               |0                |
|2022       |212416083 |0                     |0                          |10                   |8569                 |43445                 |9                    |233608                      |2678               |0               |0                |
|2023       |232490020 |0                     |0                          |26                   |8481                 |40779                 |26                   |68532                       |2936               |0               |0                |
|2024       |239470448 |0                     |0                          |27                   |9653                 |34059                 |30                   |18233                       |1868               |0               |0                |
|2025       |243589684 |0                     |0                          |27                   |12204                |28128                 |31                   |13495                       |875                |0               |0                |
|2026       |40816059  |0                     |0                          |1                    |0                    |4802                  |861                  |34110                       |59                 |0               |0                |

![alt text](images/t2/fhvhv_chart.png)

- Because FHVHV quality is much better, we created `SILVER.FHVHV_TRIPS_CLEAN` with stricter filtering.
- This view keeps only rows with:
  - pickup year between 2019 and 2026,
  - pickup time within the expected source file month (+/- 1 day),
  - dropoff after pickup,
  - trip_miles > 0,
  - trip_time > 0,
  - non-negative base passenger fare and driver pay,
  - non-null pickup/dropoff locations,
  - non-empty license/base information.

### Clean views created after T2

- After T2 we created clean views in the `SILVER` layer, because they are cleaned reusable trip-level datasets that will be used by later tasks.
- We did not put these views in `GOLD`, because `GOLD` is mostly used for aggregated outputs, charts, and final analysis tables.

- These views will be the main starting point for later exploratory analysis, augmentation, ML and other tasks.

### T3. MATIJA

For exploratory analysis, the work should start from the cleaned `SILVER` views that were created in T2:

- `SILVER.YELLOW_TRIPS_CLEAN`
- `SILVER.GREEN_TRIPS_CLEAN`
- `SILVER.FHV_TRIPS_CLEAN`
- `SILVER.FHVHV_TRIPS_CLEAN`

These views already remove the most problematic rows based on the data quality checks from T2. For FHV, the clean view is intentionally less strict, because the dataset contains many problematic dropoff timestamps and missing locations.

The results should be saved into the `GOLD` schema as aggregate tables, for example:

- `GOLD.T3_TRIPS_BY_MONTH`
- `GOLD.T3_TRIPS_BY_HOUR`

- REAL TASK STARTS HERE 

In this part we perform introductory exploratory data analysis for all four datasets.
We start from the cleaned `SILVER` views that were created in T2:

- `SILVER.YELLOW_TRIPS_CLEAN`
- `SILVER.GREEN_TRIPS_CLEAN`
- `SILVER.FHV_TRIPS_CLEAN`
- `SILVER.FHVHV_TRIPS_CLEAN`

These views already remove the most problematic rows based on the data quality checks from T2. For FHV, the clean view is intentionally less strict, because the dataset contains many problematic dropoff timestamps and missing locations.

The resutling tables are apropriatley saved in the `GOLD` schema as aggregate tables.



### T4. MATIC

### T5. MATIJA

For augmentation, the cleaned trip views from T2 should be used as the base data:

- `SILVER.YELLOW_TRIPS_CLEAN`
- `SILVER.GREEN_TRIPS_CLEAN`
- `SILVER.FHV_TRIPS_CLEAN`
- `SILVER.FHVHV_TRIPS_CLEAN`

External datasets should first be uploaded to Snowflake, into `BRONZE` stages. For example:

- taxi zone lookup / taxi zone geometry
- weather data
- schools
- businesses
- events
- attractions or other city datasets

Idea:

1. Upload external files to Snowflake stages.
2. Create external/helper tables, for example:
   - `T5_TAXI_ZONES`
   - `T5_WEATHER`
   - `T5_SCHOOLS`
   - `T5_BUSINESSES`
   - `T5_EVENTS`
3. Join these helper tables with the cleaned trip views (so in the end you have only augmented aggregations instead of having full tables, as that would be really slow and redundant).
4. Save the final enriched outputs into the `GOLD` schema.

Example:

```
T5_TAXI_ZONES
  location_id, borough, zone, service_zone, maybe geometry

T5_WEATHER
  datetime/hour, temperature, precipitation, snow, wind, etc.

T5_SCHOOLS_BY_ZONE
  location_id or borough, school_count

T5_BUSINESSES_BY_ZONE
  location_id or borough, business_count

T5_EVENTS_BY_DATE_OR_ZONE
  date/hour, zone/borough, event_count
```

### T6. MATIC

### T7. MATIC

### T8. MATIJA

### T9. MATIJA

### T10. MATIC
