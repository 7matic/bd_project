USE WAREHOUSE BIGDATA_MZMB_WH;
USE DATABASE BIGDATA_TAXI_MZMB;
USE SCHEMA SILVER;

ALTER WAREHOUSE BIGDATA_MZMB_WH SET WAREHOUSE_SIZE = 'SMALL';

CREATE OR REPLACE TABLE SILVER.YELLOW_TRIPS
CLUSTER BY (pickup_year, pickup_month)
AS
SELECT
    TRY_TO_NUMBER($1:VendorID::VARCHAR) AS vendor_id,

    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:tpep_pickup_datetime::VARCHAR), 6) AS pickup_datetime,
    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:tpep_dropoff_datetime::VARCHAR), 6) AS dropoff_datetime,

    TRY_TO_DOUBLE($1:passenger_count::VARCHAR) AS passenger_count,
    TRY_TO_DOUBLE($1:trip_distance::VARCHAR) AS trip_distance,
    TRY_TO_DOUBLE($1:RatecodeID::VARCHAR) AS ratecode_id,
    $1:store_and_fwd_flag::VARCHAR AS store_and_fwd_flag,

    TRY_TO_NUMBER($1:PULocationID::VARCHAR) AS pu_location_id,
    TRY_TO_NUMBER($1:DOLocationID::VARCHAR) AS do_location_id,
    TRY_TO_NUMBER($1:payment_type::VARCHAR) AS payment_type,

    TRY_TO_DOUBLE($1:fare_amount::VARCHAR) AS fare_amount,
    TRY_TO_DOUBLE($1:extra::VARCHAR) AS extra,
    TRY_TO_DOUBLE($1:mta_tax::VARCHAR) AS mta_tax,
    TRY_TO_DOUBLE($1:tip_amount::VARCHAR) AS tip_amount,
    TRY_TO_DOUBLE($1:tolls_amount::VARCHAR) AS tolls_amount,
    TRY_TO_DOUBLE($1:improvement_surcharge::VARCHAR) AS improvement_surcharge,
    TRY_TO_DOUBLE($1:total_amount::VARCHAR) AS total_amount,
    TRY_TO_DOUBLE($1:congestion_surcharge::VARCHAR) AS congestion_surcharge,

    COALESCE(
        TRY_TO_DOUBLE($1:airport_fee::VARCHAR),
        TRY_TO_DOUBLE($1:Airport_fee::VARCHAR)
    ) AS airport_fee,

    TRY_TO_DOUBLE($1:cbd_congestion_fee::VARCHAR) AS cbd_congestion_fee,

    YEAR(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:tpep_pickup_datetime::VARCHAR), 6)) AS pickup_year,
    MONTH(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:tpep_pickup_datetime::VARCHAR), 6)) AS pickup_month,

    METADATA$FILENAME AS source_file

FROM @BRONZE.YELLOW_RAW_STAGE
  (FILE_FORMAT => BRONZE.PARQUET_FF);

SELECT COUNT(*) AS row_count
FROM SILVER.YELLOW_TRIPS;

SELECT
  pickup_year,
  COUNT(*) AS row_count
FROM SILVER.YELLOW_TRIPS
GROUP BY pickup_year
ORDER BY pickup_year;

SELECT
  MIN(pickup_datetime) AS min_pickup,
  MAX(pickup_datetime) AS max_pickup
FROM SILVER.YELLOW_TRIPS;

CREATE OR REPLACE TABLE SILVER.GREEN_TRIPS
CLUSTER BY (pickup_year, pickup_month)
AS
SELECT
    TRY_TO_NUMBER($1:VendorID::VARCHAR) AS vendor_id,

    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:lpep_pickup_datetime::VARCHAR), 6) AS pickup_datetime,
    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:lpep_dropoff_datetime::VARCHAR), 6) AS dropoff_datetime,

    $1:store_and_fwd_flag::VARCHAR AS store_and_fwd_flag,

    TRY_TO_DOUBLE($1:RatecodeID::VARCHAR) AS ratecode_id,
    TRY_TO_NUMBER($1:PULocationID::VARCHAR) AS pu_location_id,
    TRY_TO_NUMBER($1:DOLocationID::VARCHAR) AS do_location_id,

    TRY_TO_DOUBLE($1:passenger_count::VARCHAR) AS passenger_count,
    TRY_TO_DOUBLE($1:trip_distance::VARCHAR) AS trip_distance,

    TRY_TO_DOUBLE($1:fare_amount::VARCHAR) AS fare_amount,
    TRY_TO_DOUBLE($1:extra::VARCHAR) AS extra,
    TRY_TO_DOUBLE($1:mta_tax::VARCHAR) AS mta_tax,
    TRY_TO_DOUBLE($1:tip_amount::VARCHAR) AS tip_amount,
    TRY_TO_DOUBLE($1:tolls_amount::VARCHAR) AS tolls_amount,
    TRY_TO_DOUBLE($1:ehail_fee::VARCHAR) AS ehail_fee,
    TRY_TO_DOUBLE($1:improvement_surcharge::VARCHAR) AS improvement_surcharge,
    TRY_TO_DOUBLE($1:total_amount::VARCHAR) AS total_amount,

    TRY_TO_NUMBER($1:payment_type::VARCHAR) AS payment_type,
    TRY_TO_NUMBER($1:trip_type::VARCHAR) AS trip_type,

    TRY_TO_DOUBLE($1:congestion_surcharge::VARCHAR) AS congestion_surcharge,
    TRY_TO_DOUBLE($1:cbd_congestion_fee::VARCHAR) AS cbd_congestion_fee,

    YEAR(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:lpep_pickup_datetime::VARCHAR), 6)) AS pickup_year,
    MONTH(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:lpep_pickup_datetime::VARCHAR), 6)) AS pickup_month,

    METADATA$FILENAME AS source_file

FROM @BRONZE.GREEN_RAW_STAGE
  (FILE_FORMAT => BRONZE.PARQUET_FF);

SELECT COUNT(*) AS total_rows
FROM SILVER.GREEN_TRIPS;

SELECT
  pickup_year,
  COUNT(*) AS row_count
FROM SILVER.GREEN_TRIPS
GROUP BY pickup_year
ORDER BY pickup_year;

SELECT
  MIN(pickup_datetime) AS min_pickup,
  MAX(pickup_datetime) AS max_pickup
FROM SILVER.GREEN_TRIPS;

ALTER WAREHOUSE BIGDATA_MZMB_WH SET WAREHOUSE_SIZE = 'MEDIUM';

CREATE OR REPLACE TABLE SILVER.FHV_TRIPS
CLUSTER BY (pickup_year, pickup_month)
AS
SELECT
    $1:dispatching_base_num::VARCHAR AS dispatching_base_num,

    COALESCE(
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:pickup_datetime::VARCHAR), 6),
        TRY_TO_TIMESTAMP_NTZ($1:pickup_datetime::VARCHAR)
    ) AS pickup_datetime,

    COALESCE(
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:dropOff_datetime::VARCHAR), 6),
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:dropoff_datetime::VARCHAR), 6),
        TRY_TO_TIMESTAMP_NTZ($1:dropOff_datetime::VARCHAR),
        TRY_TO_TIMESTAMP_NTZ($1:dropoff_datetime::VARCHAR)
    ) AS dropoff_datetime,

    TRY_TO_NUMBER($1:PUlocationID::VARCHAR) AS pu_location_id,
    TRY_TO_NUMBER($1:DOlocationID::VARCHAR) AS do_location_id,

    TRY_TO_NUMBER($1:SR_Flag::VARCHAR) AS sr_flag,
    $1:Affiliated_base_number::VARCHAR AS affiliated_base_number,

    YEAR(
        COALESCE(
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:pickup_datetime::VARCHAR), 6),
            TRY_TO_TIMESTAMP_NTZ($1:pickup_datetime::VARCHAR)
        )
    ) AS pickup_year,

    MONTH(
        COALESCE(
            TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:pickup_datetime::VARCHAR), 6),
            TRY_TO_TIMESTAMP_NTZ($1:pickup_datetime::VARCHAR)
        )
    ) AS pickup_month,

    METADATA$FILENAME AS source_file

FROM @BRONZE.FHV_RAW_STAGE
  (FILE_FORMAT => BRONZE.PARQUET_FF);

SELECT COUNT(*) AS total_rows
FROM SILVER.FHV_TRIPS;

SELECT
  pickup_year,
  COUNT(*) AS row_count
FROM SILVER.FHV_TRIPS
GROUP BY pickup_year
ORDER BY pickup_year;

SELECT
  MIN(pickup_datetime) AS min_pickup,
  MAX(pickup_datetime) AS max_pickup
FROM SILVER.FHV_TRIPS;

CREATE OR REPLACE TABLE SILVER.FHVHV_TRIPS
CLUSTER BY (pickup_year, pickup_month)
AS
SELECT
    $1:hvfhs_license_num::VARCHAR AS hvfhs_license_num,
    $1:dispatching_base_num::VARCHAR AS dispatching_base_num,
    $1:originating_base_num::VARCHAR AS originating_base_num,

    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:request_datetime::VARCHAR), 6) AS request_datetime,
    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:on_scene_datetime::VARCHAR), 6) AS on_scene_datetime,
    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:pickup_datetime::VARCHAR), 6) AS pickup_datetime,
    TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:dropoff_datetime::VARCHAR), 6) AS dropoff_datetime,

    TRY_TO_NUMBER($1:PULocationID::VARCHAR) AS pu_location_id,
    TRY_TO_NUMBER($1:DOLocationID::VARCHAR) AS do_location_id,

    TRY_TO_DOUBLE($1:trip_miles::VARCHAR) AS trip_miles,
    TRY_TO_NUMBER($1:trip_time::VARCHAR) AS trip_time,

    TRY_TO_DOUBLE($1:base_passenger_fare::VARCHAR) AS base_passenger_fare,
    TRY_TO_DOUBLE($1:tolls::VARCHAR) AS tolls,
    TRY_TO_DOUBLE($1:bcf::VARCHAR) AS bcf,
    TRY_TO_DOUBLE($1:sales_tax::VARCHAR) AS sales_tax,
    TRY_TO_DOUBLE($1:congestion_surcharge::VARCHAR) AS congestion_surcharge,
    TRY_TO_DOUBLE($1:airport_fee::VARCHAR) AS airport_fee,
    TRY_TO_DOUBLE($1:tips::VARCHAR) AS tips,
    TRY_TO_DOUBLE($1:driver_pay::VARCHAR) AS driver_pay,

    $1:shared_request_flag::VARCHAR AS shared_request_flag,
    $1:shared_match_flag::VARCHAR AS shared_match_flag,
    $1:access_a_ride_flag::VARCHAR AS access_a_ride_flag,
    $1:wav_request_flag::VARCHAR AS wav_request_flag,
    $1:wav_match_flag::VARCHAR AS wav_match_flag,

    TRY_TO_DOUBLE($1:cbd_congestion_fee::VARCHAR) AS cbd_congestion_fee,

    YEAR(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:pickup_datetime::VARCHAR), 6)) AS pickup_year,
    MONTH(TO_TIMESTAMP_NTZ(TRY_TO_NUMBER($1:pickup_datetime::VARCHAR), 6)) AS pickup_month,

    METADATA$FILENAME AS source_file

FROM @BRONZE.FHVHV_RAW_STAGE
  (FILE_FORMAT => BRONZE.PARQUET_FF);

SELECT COUNT(*) AS total_rows
FROM SILVER.FHVHV_TRIPS;

SELECT
  pickup_year,
  COUNT(*) AS row_count
FROM SILVER.FHVHV_TRIPS
GROUP BY pickup_year
ORDER BY pickup_year;

SELECT
  MIN(pickup_datetime) AS min_pickup,
  MAX(pickup_datetime) AS max_pickup
FROM SILVER.FHVHV_TRIPS;

ALTER WAREHOUSE BIGDATA_MZMB_WH SET WAREHOUSE_SIZE = 'XSMALL';