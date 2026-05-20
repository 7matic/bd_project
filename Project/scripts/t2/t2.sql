USE WAREHOUSE BIGDATA_MZMB_WH;
USE DATABASE BIGDATA_TAXI_MZMB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE GOLD.T2_YELLOW_QUALITY_FILE_YEAR AS
WITH base AS (
    SELECT
        source_file,
        pickup_year,
        pickup_datetime,
        dropoff_datetime,
        trip_distance,
        passenger_count,
        fare_amount,
        total_amount,BIGDATA_TAXI_MZMB.BRONZE
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01') AS file_month_start
    FROM SILVER.YELLOW_TRIPS
)
SELECT
    source_file,
    pickup_year,
    COUNT(*) AS total_rows,

    COUNT_IF(pickup_year IS NULL OR pickup_year < 2012 OR pickup_year > 2026) AS unexpected_pickup_year,

    COUNT_IF(
        pickup_datetime IS NULL
        OR file_month_start IS NULL
        OR pickup_datetime < DATEADD(day, -1, file_month_start)
        OR pickup_datetime >= DATEADD(day, 1, DATEADD(month, 1, file_month_start))
    ) AS pickup_outside_source_month,

    COUNT_IF(pickup_datetime = dropoff_datetime) AS pickup_equals_dropoff,
    COUNT_IF(dropoff_datetime < pickup_datetime) AS dropoff_before_pickup,
    COUNT_IF(trip_distance = 0) AS zero_trip_distance,
    COUNT_IF(passenger_count <= 0) AS nonpositive_passenger_count,
    COUNT_IF(fare_amount < 0) AS negative_fare_amount,
    COUNT_IF(total_amount < 0) AS negative_total_amount

FROM base
GROUP BY source_file, pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_YELLOW_QUALITY_YEAR AS
SELECT
    pickup_year,
    SUM(total_rows) AS total_rows,

    SUM(unexpected_pickup_year) AS unexpected_pickup_year,
    SUM(pickup_outside_source_month) AS pickup_outside_source_month,
    SUM(pickup_equals_dropoff) AS pickup_equals_dropoff,
    SUM(dropoff_before_pickup) AS dropoff_before_pickup,
    SUM(zero_trip_distance) AS zero_trip_distance,
    SUM(nonpositive_passenger_count) AS nonpositive_passenger_count,
    SUM(negative_fare_amount) AS negative_fare_amount,
    SUM(negative_total_amount) AS negative_total_amount

FROM GOLD.T2_YELLOW_QUALITY_FILE_YEAR
GROUP BY pickup_year
ORDER BY pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_YELLOW_QUALITY_CHART AS
SELECT pickup_year, 'unexpected_pickup_year' AS issue_type, unexpected_pickup_year AS issue_count, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_outside_source_month', pickup_outside_source_month, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_equals_dropoff', pickup_equals_dropoff, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'dropoff_before_pickup', dropoff_before_pickup, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'zero_trip_distance', zero_trip_distance, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'nonpositive_passenger_count', nonpositive_passenger_count, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'negative_fare_amount', negative_fare_amount, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'negative_total_amount', negative_total_amount, total_rows FROM GOLD.T2_YELLOW_QUALITY_YEAR;

SELECT
    TO_VARCHAR(pickup_year) AS pickup_year_label,
    pickup_year,
    issue_type,
    ROUND(100 * issue_count / NULLIF(total_rows, 0), 4) AS issue_pct
FROM GOLD.T2_YELLOW_QUALITY_CHART
WHERE pickup_year BETWEEN 2012 AND 2026
ORDER BY pickup_year, issue_type;

CREATE OR REPLACE VIEW SILVER.YELLOW_TRIPS_CLEAN AS
SELECT *
FROM SILVER.YELLOW_TRIPS
WHERE pickup_year BETWEEN 2012 AND 2026
  AND pickup_datetime >= DATEADD(
        day, -1,
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01')
      )
  AND pickup_datetime < DATEADD(
        day, 1,
        DATEADD(month, 1, TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01'))
      )
  AND dropoff_datetime > pickup_datetime
  AND trip_distance > 0
  AND fare_amount >= 0
  AND total_amount >= 0
  AND (passenger_count IS NULL OR passenger_count > 0);


CREATE OR REPLACE TABLE GOLD.T2_GREEN_QUALITY_FILE_YEAR AS
WITH base AS (
    SELECT
        source_file,
        pickup_year,
        pickup_datetime,
        dropoff_datetime,
        trip_distance,
        passenger_count,
        fare_amount,
        total_amount,
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01') AS file_month_start
    FROM SILVER.GREEN_TRIPS
)
SELECT
    source_file,
    pickup_year,
    COUNT(*) AS total_rows,

    COUNT_IF(pickup_year IS NULL OR pickup_year < 2014 OR pickup_year > 2026) AS unexpected_pickup_year,

    COUNT_IF(
        pickup_datetime IS NULL
        OR file_month_start IS NULL
        OR pickup_datetime < DATEADD(day, -1, file_month_start)
        OR pickup_datetime >= DATEADD(day, 1, DATEADD(month, 1, file_month_start))
    ) AS pickup_outside_source_month,

    COUNT_IF(pickup_datetime = dropoff_datetime) AS pickup_equals_dropoff,
    COUNT_IF(dropoff_datetime < pickup_datetime) AS dropoff_before_pickup,
    COUNT_IF(trip_distance = 0) AS zero_trip_distance,
    COUNT_IF(passenger_count <= 0) AS nonpositive_passenger_count,
    COUNT_IF(fare_amount < 0) AS negative_fare_amount,
    COUNT_IF(total_amount < 0) AS negative_total_amount

FROM base
GROUP BY source_file, pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_GREEN_QUALITY_YEAR AS
SELECT
    pickup_year,
    SUM(total_rows) AS total_rows,

    SUM(unexpected_pickup_year) AS unexpected_pickup_year,
    SUM(pickup_outside_source_month) AS pickup_outside_source_month,
    SUM(pickup_equals_dropoff) AS pickup_equals_dropoff,
    SUM(dropoff_before_pickup) AS dropoff_before_pickup,
    SUM(zero_trip_distance) AS zero_trip_distance,
    SUM(nonpositive_passenger_count) AS nonpositive_passenger_count,
    SUM(negative_fare_amount) AS negative_fare_amount,
    SUM(negative_total_amount) AS negative_total_amount

FROM GOLD.T2_GREEN_QUALITY_FILE_YEAR
GROUP BY pickup_year
ORDER BY pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_GREEN_QUALITY_CHART AS
SELECT pickup_year, 'unexpected_pickup_year' AS issue_type, unexpected_pickup_year AS issue_count, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_outside_source_month', pickup_outside_source_month, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_equals_dropoff', pickup_equals_dropoff, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'dropoff_before_pickup', dropoff_before_pickup, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'zero_trip_distance', zero_trip_distance, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'nonpositive_passenger_count', nonpositive_passenger_count, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'negative_fare_amount', negative_fare_amount, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'negative_total_amount', negative_total_amount, total_rows FROM GOLD.T2_GREEN_QUALITY_YEAR;

SELECT
    TO_VARCHAR(pickup_year) AS pickup_year_label,
    pickup_year,
    issue_type,
    ROUND(100 * issue_count / NULLIF(total_rows, 0), 4) AS issue_pct
FROM GOLD.T2_GREEN_QUALITY_CHART
WHERE pickup_year BETWEEN 2014 AND 2026
ORDER BY pickup_year, issue_type;

CREATE OR REPLACE VIEW SILVER.GREEN_TRIPS_CLEAN AS
SELECT *
FROM SILVER.GREEN_TRIPS
WHERE pickup_year BETWEEN 2014 AND 2026
  AND pickup_datetime >= DATEADD(
        day, -1,
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01')
      )
  AND pickup_datetime < DATEADD(
        day, 1,
        DATEADD(month, 1, TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01'))
      )
  AND dropoff_datetime > pickup_datetime
  AND trip_distance > 0
  AND fare_amount >= 0
  AND total_amount >= 0
  AND (passenger_count IS NULL OR passenger_count > 0);


  CREATE OR REPLACE TABLE GOLD.T2_FHV_QUALITY_FILE_YEAR AS
WITH base AS (
    SELECT
        source_file,
        pickup_year,
        pickup_datetime,
        dropoff_datetime,
        pu_location_id,
        do_location_id,
        dispatching_base_num,
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01') AS file_month_start
    FROM SILVER.FHV_TRIPS
)
SELECT
    source_file,
    pickup_year,
    COUNT(*) AS total_rows,

    COUNT_IF(pickup_year IS NULL OR pickup_year < 2015 OR pickup_year > 2026) AS unexpected_pickup_year,

    COUNT_IF(
        pickup_datetime IS NULL
        OR file_month_start IS NULL
        OR pickup_datetime < DATEADD(day, -1, file_month_start)
        OR pickup_datetime >= DATEADD(day, 1, DATEADD(month, 1, file_month_start))
    ) AS pickup_outside_source_month,

    COUNT_IF(pickup_datetime = dropoff_datetime) AS pickup_equals_dropoff,
    COUNT_IF(dropoff_datetime < pickup_datetime) AS dropoff_before_pickup,

    COUNT_IF(pu_location_id IS NULL) AS missing_pickup_location,
    COUNT_IF(do_location_id IS NULL) AS missing_dropoff_location,
    COUNT_IF(dispatching_base_num IS NULL OR dispatching_base_num = '') AS missing_dispatching_base

FROM base
GROUP BY source_file, pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_FHV_QUALITY_YEAR AS
SELECT
    pickup_year,
    SUM(total_rows) AS total_rows,

    SUM(unexpected_pickup_year) AS unexpected_pickup_year,
    SUM(pickup_outside_source_month) AS pickup_outside_source_month,
    SUM(pickup_equals_dropoff) AS pickup_equals_dropoff,
    SUM(dropoff_before_pickup) AS dropoff_before_pickup,
    SUM(missing_pickup_location) AS missing_pickup_location,
    SUM(missing_dropoff_location) AS missing_dropoff_location,
    SUM(missing_dispatching_base) AS missing_dispatching_base

FROM GOLD.T2_FHV_QUALITY_FILE_YEAR
GROUP BY pickup_year
ORDER BY pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_FHV_QUALITY_CHART AS
SELECT pickup_year, 'unexpected_pickup_year' AS issue_type, unexpected_pickup_year AS issue_count, total_rows FROM GOLD.T2_FHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_outside_source_month', pickup_outside_source_month, total_rows FROM GOLD.T2_FHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_equals_dropoff', pickup_equals_dropoff, total_rows FROM GOLD.T2_FHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'dropoff_before_pickup', dropoff_before_pickup, total_rows FROM GOLD.T2_FHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'missing_pickup_location', missing_pickup_location, total_rows FROM GOLD.T2_FHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'missing_dropoff_location', missing_dropoff_location, total_rows FROM GOLD.T2_FHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'missing_dispatching_base', missing_dispatching_base, total_rows FROM GOLD.T2_FHV_QUALITY_YEAR;

SELECT
    TO_VARCHAR(pickup_year) AS pickup_year_label,
    pickup_year,
    issue_type,
    ROUND(100 * issue_count / NULLIF(total_rows, 0), 4) AS issue_pct
FROM GOLD.T2_FHV_QUALITY_CHART
WHERE pickup_year BETWEEN 2015 AND 2026
ORDER BY pickup_year, issue_type;

CREATE OR REPLACE VIEW SILVER.FHV_TRIPS_CLEAN AS
SELECT *
FROM SILVER.FHV_TRIPS
WHERE pickup_year BETWEEN 2015 AND 2026
  AND pickup_datetime >= DATEADD(
        day, -1,
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01')
      )
  AND pickup_datetime < DATEADD(
        day, 1,
        DATEADD(month, 1, TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01'))
      )
  AND dispatching_base_num IS NOT NULL
  AND dispatching_base_num <> '';


CREATE OR REPLACE TABLE GOLD.T2_FHVHV_QUALITY_FILE_YEAR AS
WITH base AS (
    SELECT
        source_file,
        pickup_year,
        pickup_datetime,
        dropoff_datetime,
        trip_miles,
        trip_time,
        base_passenger_fare,
        driver_pay,
        pu_location_id,
        do_location_id,
        hvfhs_license_num,
        dispatching_base_num,
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01') AS file_month_start
    FROM SILVER.FHVHV_TRIPS
)
SELECT
    source_file,
    pickup_year,
    COUNT(*) AS total_rows,

    COUNT_IF(pickup_year IS NULL OR pickup_year < 2019 OR pickup_year > 2026) AS unexpected_pickup_year,

    COUNT_IF(
        pickup_datetime IS NULL
        OR file_month_start IS NULL
        OR pickup_datetime < DATEADD(day, -1, file_month_start)
        OR pickup_datetime >= DATEADD(day, 1, DATEADD(month, 1, file_month_start))
    ) AS pickup_outside_source_month,

    COUNT_IF(pickup_datetime = dropoff_datetime) AS pickup_equals_dropoff,
    COUNT_IF(dropoff_datetime < pickup_datetime) AS dropoff_before_pickup,

    COUNT_IF(trip_miles <= 0) AS nonpositive_trip_miles,
    COUNT_IF(trip_time <= 0) AS nonpositive_trip_time,

    COUNT_IF(base_passenger_fare < 0) AS negative_base_passenger_fare,
    COUNT_IF(driver_pay < 0) AS negative_driver_pay,

    COUNT_IF(pu_location_id IS NULL OR do_location_id IS NULL) AS missing_location,

    COUNT_IF(
        hvfhs_license_num IS NULL
        OR hvfhs_license_num = ''
        OR dispatching_base_num IS NULL
        OR dispatching_base_num = ''
    ) AS missing_base_info

FROM base
GROUP BY source_file, pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_FHVHV_QUALITY_YEAR AS
SELECT
    pickup_year,
    SUM(total_rows) AS total_rows,

    SUM(unexpected_pickup_year) AS unexpected_pickup_year,
    SUM(pickup_outside_source_month) AS pickup_outside_source_month,
    SUM(pickup_equals_dropoff) AS pickup_equals_dropoff,
    SUM(dropoff_before_pickup) AS dropoff_before_pickup,
    SUM(nonpositive_trip_miles) AS nonpositive_trip_miles,
    SUM(nonpositive_trip_time) AS nonpositive_trip_time,
    SUM(negative_base_passenger_fare) AS negative_base_passenger_fare,
    SUM(negative_driver_pay) AS negative_driver_pay,
    SUM(missing_location) AS missing_location,
    SUM(missing_base_info) AS missing_base_info

FROM GOLD.T2_FHVHV_QUALITY_FILE_YEAR
GROUP BY pickup_year
ORDER BY pickup_year;

CREATE OR REPLACE TABLE GOLD.T2_FHVHV_QUALITY_CHART AS
SELECT pickup_year, 'unexpected_pickup_year' AS issue_type, unexpected_pickup_year AS issue_count, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_outside_source_month', pickup_outside_source_month, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'pickup_equals_dropoff', pickup_equals_dropoff, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'dropoff_before_pickup', dropoff_before_pickup, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'nonpositive_trip_miles', nonpositive_trip_miles, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'nonpositive_trip_time', nonpositive_trip_time, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'negative_base_passenger_fare', negative_base_passenger_fare, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'negative_driver_pay', negative_driver_pay, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'missing_location', missing_location, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR
UNION ALL
SELECT pickup_year, 'missing_base_info', missing_base_info, total_rows FROM GOLD.T2_FHVHV_QUALITY_YEAR;

SELECT
    TO_VARCHAR(pickup_year) AS pickup_year_label,
    pickup_year,
    issue_type,
    ROUND(100 * issue_count / NULLIF(total_rows, 0), 4) AS issue_pct
FROM GOLD.T2_FHVHV_QUALITY_CHART
WHERE pickup_year BETWEEN 2019 AND 2026
ORDER BY pickup_year, issue_type;

CREATE OR REPLACE VIEW SILVER.FHVHV_TRIPS_CLEAN AS
SELECT *
FROM SILVER.FHVHV_TRIPS
WHERE pickup_year BETWEEN 2019 AND 2026
  AND pickup_datetime >= DATEADD(
        day, -1,
        TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01')
      )
  AND pickup_datetime < DATEADD(
        day, 1,
        DATEADD(month, 1, TO_DATE(REGEXP_SUBSTR(source_file, '[0-9]{4}-[0-9]{2}') || '-01'))
      )
  AND dropoff_datetime > pickup_datetime
  AND trip_miles > 0
  AND trip_time > 0
  AND base_passenger_fare >= 0
  AND driver_pay >= 0
  AND pu_location_id IS NOT NULL
  AND do_location_id IS NOT NULL
  AND hvfhs_license_num IS NOT NULL
  AND hvfhs_license_num <> ''BIGDATA_TAXI_MZMB.BRONZE
  AND dispatching_base_num IS NOT NULL
  AND dispatching_base_num <> '';