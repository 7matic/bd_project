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