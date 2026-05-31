USE WAREHOUSE BIGDATA_MZMB_WH;
USE DATABASE BIGDATA_TAXI_MZMB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE GOLD.T6_STREAM_SOURCE_2021 AS
WITH combined AS (
    SELECT
        'yellow' AS dataset,
        y.pickup_datetime,
        y.dropoff_datetime,
        y.pu_location_id,
        y.do_location_id,
        pu.borough AS pickup_borough,
        pu.zone AS pickup_zone,
        du.borough AS dropoff_borough,
        du.zone AS dropoff_zone,
        y.trip_distance AS trip_distance,
        DATEDIFF('second', y.pickup_datetime, y.dropoff_datetime) AS trip_duration_sec,
        y.total_amount AS fare_amount
    FROM SILVER.YELLOW_TRIPS_CLEAN y
    LEFT JOIN EXTERNAL_DATA.TAXI_ZONES pu
        ON y.pu_location_id = pu.location_id
    LEFT JOIN EXTERNAL_DATA.TAXI_ZONES du
        ON y.do_location_id = du.location_id
    WHERE y.pickup_year = 2021

    UNION ALL

    SELECT
        'fhvhv' AS dataset,
        f.pickup_datetime,
        f.dropoff_datetime,
        f.pu_location_id,
        f.do_location_id,
        pu.borough AS pickup_borough,
        pu.zone AS pickup_zone,
        du.borough AS dropoff_borough,
        du.zone AS dropoff_zone,
        f.trip_miles AS trip_distance,
        f.trip_time AS trip_duration_sec,
        f.base_passenger_fare AS fare_amount
    FROM SILVER.FHVHV_TRIPS_CLEAN f
    LEFT JOIN EXTERNAL_DATA.TAXI_ZONES pu
        ON f.pu_location_id = pu.location_id
    LEFT JOIN EXTERNAL_DATA.TAXI_ZONES du
        ON f.do_location_id = du.location_id
    WHERE f.pickup_year = 2021
)
SELECT
    ROW_NUMBER() OVER (ORDER BY pickup_datetime, dataset) AS stream_id,
    *
FROM combined;

SELECT dataset, COUNT(*) AS "rows"
FROM GOLD.T6_STREAM_SOURCE_2021
GROUP BY dataset;

SELECT *
FROM GOLD.T6_STREAM_SOURCE_2021
ORDER BY stream_id
LIMIT 10;

CREATE OR REPLACE TABLE GOLD.T6_KAFKA_LANDING_2021 (
    RECORD_CONTENT VARIANT,
    RECORD_METADATA VARIANT
);

CREATE OR REPLACE VIEW GOLD.T6_STREAM_EVENTS_2021 AS
SELECT
    RECORD_CONTENT:stream_id::NUMBER AS stream_id,
    RECORD_CONTENT:dataset::STRING AS dataset,
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:pickup_datetime::STRING) AS pickup_datetime,
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:dropoff_datetime::STRING) AS dropoff_datetime,
    RECORD_CONTENT:pu_location_id::NUMBER AS pu_location_id,
    RECORD_CONTENT:do_location_id::NUMBER AS do_location_id,
    RECORD_CONTENT:pickup_borough::STRING AS pickup_borough,
    RECORD_CONTENT:pickup_zone::STRING AS pickup_zone,
    RECORD_CONTENT:dropoff_borough::STRING AS dropoff_borough,
    RECORD_CONTENT:dropoff_zone::STRING AS dropoff_zone,
    RECORD_CONTENT:trip_distance::FLOAT AS trip_distance,
    RECORD_CONTENT:trip_duration_sec::NUMBER AS trip_duration_sec,
    RECORD_CONTENT:fare_amount::FLOAT AS fare_amount,
    RECORD_METADATA:topic::STRING AS kafka_topic,
    RECORD_METADATA:partition::NUMBER AS kafka_partition,
    RECORD_METADATA:offset::NUMBER AS kafka_offset
FROM GOLD.T6_KAFKA_LANDING_2021;

SELECT COUNT(*) AS rows_loaded
FROM GOLD.T6_KAFKA_LANDING_2021;

SELECT
    RECORD_CONTENT:stream_id::NUMBER AS stream_id,
    RECORD_CONTENT:dataset::STRING AS dataset,
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:pickup_datetime::STRING) AS pickup_datetime,
    RECORD_CONTENT:pickup_borough::STRING AS pickup_borough,
    RECORD_CONTENT:pickup_zone::STRING AS pickup_zone,
    RECORD_CONTENT:trip_distance::FLOAT AS trip_distance
FROM GOLD.T6_KAFKA_LANDING_2021
ORDER BY stream_id
LIMIT 20;

CREATE OR REPLACE VIEW GOLD.T6_STREAM_EVENTS_2021 AS
SELECT
    RECORD_CONTENT:stream_id::NUMBER AS stream_id,
    RECORD_CONTENT:dataset::STRING AS dataset,
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:pickup_datetime::STRING) AS pickup_datetime,
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:dropoff_datetime::STRING) AS dropoff_datetime,
    RECORD_CONTENT:pu_location_id::NUMBER AS pu_location_id,
    RECORD_CONTENT:do_location_id::NUMBER AS do_location_id,
    RECORD_CONTENT:pickup_borough::STRING AS pickup_borough,
    RECORD_CONTENT:pickup_zone::STRING AS pickup_zone,
    RECORD_CONTENT:dropoff_borough::STRING AS dropoff_borough,
    RECORD_CONTENT:dropoff_zone::STRING AS dropoff_zone,
    RECORD_CONTENT:trip_distance::FLOAT AS trip_distance,
    RECORD_CONTENT:trip_duration_sec::FLOAT AS trip_duration_sec,
    RECORD_CONTENT:fare_amount::FLOAT AS fare_amount,
    RECORD_METADATA:topic::STRING AS kafka_topic,
    RECORD_METADATA:partition::NUMBER AS kafka_partition,
    RECORD_METADATA:offset::NUMBER AS kafka_offset
FROM GOLD.T6_KAFKA_LANDING_2021;

SELECT dataset, COUNT(*) AS "rows"
FROM GOLD.T6_STREAM_EVENTS_2021
GROUP BY dataset;

CREATE OR REPLACE TABLE GOLD.T6_HOURLY_BOROUGH_STATS_2021 AS
SELECT
    DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
    pickup_borough,
    dataset,

    COUNT(*) AS trip_count,

    SUM(trip_distance) AS sum_distance,
    SUM(trip_distance * trip_distance) AS sumsq_distance,

    SUM(trip_duration_sec) AS sum_duration,
    SUM(trip_duration_sec * trip_duration_sec) AS sumsq_duration,

    SUM(fare_amount) AS sum_fare,
    SUM(fare_amount * fare_amount) AS sumsq_fare

FROM GOLD.T6_STREAM_EVENTS_2021
WHERE pickup_borough IS NOT NULL
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE GOLD.T6_ROLLING_BOROUGH_STATS_2021 AS
WITH rolling AS (
    SELECT
        pickup_hour,
        pickup_borough,
        dataset,

        SUM(trip_count) OVER (
            PARTITION BY pickup_borough, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_trip_count,

        SUM(sum_distance) OVER (
            PARTITION BY pickup_borough, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sum_distance,

        SUM(sumsq_distance) OVER (
            PARTITION BY pickup_borough, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sumsq_distance,

        SUM(sum_duration) OVER (
            PARTITION BY pickup_borough, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sum_duration,

        SUM(sumsq_duration) OVER (
            PARTITION BY pickup_borough, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sumsq_duration,

        SUM(sum_fare) OVER (
            PARTITION BY pickup_borough, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sum_fare,

        SUM(sumsq_fare) OVER (
            PARTITION BY pickup_borough, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sumsq_fare

    FROM GOLD.T6_HOURLY_BOROUGH_STATS_2021
)
SELECT
    pickup_hour,
    pickup_borough,
    dataset,
    rolling_trip_count,

    rolling_sum_distance / NULLIF(rolling_trip_count, 0) AS rolling_mean_distance,
    SQRT(GREATEST(
        (rolling_sumsq_distance - POWER(rolling_sum_distance, 2) / NULLIF(rolling_trip_count, 0))
        / NULLIF(rolling_trip_count - 1, 0),
        0
    )) AS rolling_std_distance,

    rolling_sum_duration / NULLIF(rolling_trip_count, 0) AS rolling_mean_duration,
    SQRT(GREATEST(
        (rolling_sumsq_duration - POWER(rolling_sum_duration, 2) / NULLIF(rolling_trip_count, 0))
        / NULLIF(rolling_trip_count - 1, 0),
        0
    )) AS rolling_std_duration,

    rolling_sum_fare / NULLIF(rolling_trip_count, 0) AS rolling_mean_fare,
    SQRT(GREATEST(
        (rolling_sumsq_fare - POWER(rolling_sum_fare, 2) / NULLIF(rolling_trip_count, 0))
        / NULLIF(rolling_trip_count - 1, 0),
        0
    )) AS rolling_std_fare

FROM rolling;

SELECT *
FROM GOLD.T6_ROLLING_BOROUGH_STATS_2021
ORDER BY pickup_hour, pickup_borough, dataset
LIMIT 50;

CREATE OR REPLACE TABLE GOLD.T6_TOP_LOCATIONS_2021 AS
WITH locs AS (
    SELECT
        pu_location_id AS location_id,
        COUNT(*) AS pickup_count,
        0 AS dropoff_count
    FROM GOLD.T6_STREAM_EVENTS_2021
    WHERE pu_location_id NOT IN (264, 265)
    GROUP BY pu_location_id

    UNION ALL

    SELECT
        do_location_id AS location_id,
        0 AS pickup_count,
        COUNT(*) AS dropoff_count
    FROM GOLD.T6_STREAM_EVENTS_2021
    WHERE do_location_id NOT IN (264, 265)
    GROUP BY do_location_id
),
agg AS (
    SELECT
        location_id,
        SUM(pickup_count) AS pickup_count,
        SUM(dropoff_count) AS dropoff_count,
        SUM(pickup_count + dropoff_count) AS total_count
    FROM locs
    GROUP BY location_id
)
SELECT
    a.location_id,
    z.borough,
    z.zone,
    z.service_zone,
    a.pickup_count,
    a.dropoff_count,
    a.total_count
FROM agg a
LEFT JOIN EXTERNAL_DATA.TAXI_ZONES z
    ON a.location_id = z.location_id
WHERE z.borough <> 'N/A'
ORDER BY total_count DESC
LIMIT 10;

SELECT *
FROM GOLD.T6_TOP_LOCATIONS_2021
ORDER BY total_count DESC;

CREATE OR REPLACE TABLE GOLD.T6_TOP_LOCATION_EVENTS_2021 AS
SELECT
    e.stream_id,
    e.dataset,
    e.pickup_datetime,
    e.pu_location_id AS location_id,
    'pickup' AS location_role,
    e.pickup_borough AS borough,
    e.pickup_zone AS zone,
    e.trip_distance,
    e.trip_duration_sec,
    e.fare_amount
FROM GOLD.T6_STREAM_EVENTS_2021 e
JOIN GOLD.T6_TOP_LOCATIONS_2021 t
    ON e.pu_location_id = t.location_id

UNION ALL

SELECT
    e.stream_id,
    e.dataset,
    e.pickup_datetime,
    e.do_location_id AS location_id,
    'dropoff' AS location_role,
    e.dropoff_borough AS borough,
    e.dropoff_zone AS zone,
    e.trip_distance,
    e.trip_duration_sec,
    e.fare_amount
FROM GOLD.T6_STREAM_EVENTS_2021 e
JOIN GOLD.T6_TOP_LOCATIONS_2021 t
    ON e.do_location_id = t.location_id;

CREATE OR REPLACE TABLE GOLD.T6_HOURLY_TOP_LOCATION_STATS_2021 AS
SELECT
    DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
    location_id,
    borough,
    zone,
    location_role,
    dataset,

    COUNT(*) AS trip_count,

    SUM(trip_distance) AS sum_distance,
    SUM(trip_distance * trip_distance) AS sumsq_distance,

    SUM(trip_duration_sec) AS sum_duration,
    SUM(trip_duration_sec * trip_duration_sec) AS sumsq_duration,

    SUM(fare_amount) AS sum_fare,
    SUM(fare_amount * fare_amount) AS sumsq_fare

FROM GOLD.T6_TOP_LOCATION_EVENTS_2021
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE TABLE GOLD.T6_ROLLING_TOP_LOCATION_STATS_2021 AS
WITH rolling AS (
    SELECT
        pickup_hour,
        location_id,
        borough,
        zone,
        location_role,
        dataset,

        SUM(trip_count) OVER (
            PARTITION BY location_id, location_role, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_trip_count,

        SUM(sum_distance) OVER (
            PARTITION BY location_id, location_role, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sum_distance,

        SUM(sumsq_distance) OVER (
            PARTITION BY location_id, location_role, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sumsq_distance,

        SUM(sum_duration) OVER (
            PARTITION BY location_id, location_role, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sum_duration,

        SUM(sumsq_duration) OVER (
            PARTITION BY location_id, location_role, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sumsq_duration,

        SUM(sum_fare) OVER (
            PARTITION BY location_id, location_role, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sum_fare,

        SUM(sumsq_fare) OVER (
            PARTITION BY location_id, location_role, dataset
            ORDER BY pickup_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_sumsq_fare

    FROM GOLD.T6_HOURLY_TOP_LOCATION_STATS_2021
)
SELECT
    pickup_hour,
    location_id,
    borough,
    zone,
    location_role,
    dataset,
    rolling_trip_count,

    rolling_sum_distance / NULLIF(rolling_trip_count, 0) AS rolling_mean_distance,
    SQRT(GREATEST(
        (rolling_sumsq_distance - POWER(rolling_sum_distance, 2) / NULLIF(rolling_trip_count, 0))
        / NULLIF(rolling_trip_count - 1, 0),
        0
    )) AS rolling_std_distance,

    rolling_sum_duration / NULLIF(rolling_trip_count, 0) AS rolling_mean_duration,
    SQRT(GREATEST(
        (rolling_sumsq_duration - POWER(rolling_sum_duration, 2) / NULLIF(rolling_trip_count, 0))
        / NULLIF(rolling_trip_count - 1, 0),
        0
    )) AS rolling_std_duration,

    rolling_sum_fare / NULLIF(rolling_trip_count, 0) AS rolling_mean_fare,
    SQRT(GREATEST(
        (rolling_sumsq_fare - POWER(rolling_sum_fare, 2) / NULLIF(rolling_trip_count, 0))
        / NULLIF(rolling_trip_count - 1, 0),
        0
    )) AS rolling_std_fare

FROM rolling;

SELECT *
FROM GOLD.T6_ROLLING_TOP_LOCATION_STATS_2021
ORDER BY pickup_hour, location_id, location_role, dataset
LIMIT 100;

SELECT COUNT(*) AS streamed_rows
FROM GOLD.T6_STREAM_EVENTS_2021;

SELECT dataset, COUNT(*) AS "rows"
FROM GOLD.T6_STREAM_EVENTS_2021
GROUP BY dataset;

SELECT COUNT(*) AS borough_rows
FROM GOLD.T6_ROLLING_BOROUGH_STATS_2021;

SELECT COUNT(*) AS top_location_rows
FROM GOLD.T6_ROLLING_TOP_LOCATION_STATS_2021;

CREATE OR REPLACE FILE FORMAT T6_CSV_FF
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('', 'NULL', 'null');

CREATE OR REPLACE STAGE T6_RESULTS_STAGE
  FILE_FORMAT = T6_CSV_FF;

CREATE OR REPLACE TABLE GOLD.T6_BIRCH_CLUSTER_SUMMARY_2021 (
  cluster_id NUMBER,
  trip_count_sample NUMBER,
  sample_pct FLOAT,
  avg_distance FLOAT,
  avg_duration_sec FLOAT,
  avg_fare_amount FLOAT,
  dataset_distribution STRING,
  top_pickup_boroughs STRING,
  top_pickup_zones STRING
);

COPY INTO GOLD.T6_BIRCH_CLUSTER_SUMMARY_2021
FROM @GOLD.T6_RESULTS_STAGE/t6_birch_cluster_summary_2021.csv
FILE_FORMAT = GOLD.T6_CSV_FF
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE GOLD.T6_BIRCH_PROGRESS_2021 (
  messages_trained NUMBER,
  elapsed_seconds FLOAT,
  messages_per_second FLOAT,
  n_subclusters NUMBER
);

COPY INTO GOLD.T6_BIRCH_PROGRESS_2021
FROM @GOLD.T6_RESULTS_STAGE/t6_birch_progress_2021.csv
FILE_FORMAT = GOLD.T6_CSV_FF
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE GOLD.T6_BIRCH_ASSIGNMENTS_SAMPLE_2021 (
  stream_id NUMBER,
  dataset STRING,
  pickup_datetime TIMESTAMP_NTZ,
  pickup_borough STRING,
  pickup_zone STRING,
  pu_location_id NUMBER,
  do_location_id NUMBER,
  trip_distance FLOAT,
  trip_duration_sec FLOAT,
  fare_amount FLOAT,
  cluster_id NUMBER
);

COPY INTO GOLD.T6_BIRCH_ASSIGNMENTS_SAMPLE_2021
FROM @GOLD.T6_RESULTS_STAGE/t6_birch_assignments_sample_2021.csv
FILE_FORMAT = GOLD.T6_CSV_FF
ON_ERROR = 'CONTINUE';

SELECT * FROM GOLD.T6_BIRCH_CLUSTER_SUMMARY_2021 ORDER BY cluster_id;

SELECT *
FROM GOLD.T6_BIRCH_PROGRESS_2021
ORDER BY messages_trained DESC
LIMIT 10;

SELECT cluster_id, dataset, COUNT(*) AS "rows"
FROM GOLD.T6_BIRCH_ASSIGNMENTS_SAMPLE_2021
GROUP BY cluster_id, dataset
ORDER BY cluster_id, dataset;

SELECT *
FROM GOLD.T6_ROLLING_BOROUGH_STATS_2021
WHERE pickup_borough NOT IN ('N/A', 'Unknown')
ORDER BY pickup_hour, pickup_borough, dataset
LIMIT 100;

SELECT *
FROM GOLD.T6_ROLLING_TOP_LOCATION_STATS_2021
ORDER BY pickup_hour, location_id, location_role, dataset
LIMIT 100;

SELECT *
FROM GOLD.T6_BIRCH_CLUSTER_SUMMARY_2021
ORDER BY cluster_id;