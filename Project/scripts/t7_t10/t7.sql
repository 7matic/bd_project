USE WAREHOUSE BIGDATA_MZMB_WH;
USE DATABASE BIGDATA_TAXI_MZMB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE GOLD.T7_DATASET_LIMITS AS
SELECT
    'yellow' AS dataset,
    MIN(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS min_hour,
    MAX(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS max_hour
FROM SILVER.YELLOW_TRIPS_CLEAN

UNION ALL

SELECT
    'green' AS dataset,
    MIN(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS min_hour,
    MAX(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS max_hour
FROM SILVER.GREEN_TRIPS_CLEAN

UNION ALL

SELECT
    'fhv' AS dataset,
    MIN(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS min_hour,
    MAX(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS max_hour
FROM SILVER.FHV_TRIPS_CLEAN

UNION ALL

SELECT
    'fhvhv' AS dataset,
    MIN(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS min_hour,
    MAX(DATE_TRUNC('hour', pickup_datetime))::TIMESTAMP_NTZ AS max_hour
FROM SILVER.FHVHV_TRIPS_CLEAN;


CREATE OR REPLACE TABLE GOLD.T7_COMMON_RANGE AS
WITH weather_limit AS (
    SELECT
        DATEADD(
            'hour',
            23,
            MAX("DATE"::DATE)::TIMESTAMP_NTZ
        ) AS weather_end_hour
    FROM EXTERNAL_DATA.T5_WEATHER
)
SELECT
    MAX(l.min_hour) AS start_hour,
    LEAST(
        MIN(l.max_hour),
        (SELECT weather_end_hour FROM weather_limit),
        '2025-12-31 23:00:00'::TIMESTAMP_NTZ
    ) AS end_hour
FROM GOLD.T7_DATASET_LIMITS l;



CREATE OR REPLACE TABLE GOLD.T7_DATASETS AS
SELECT 'yellow' AS dataset, 0 AS dataset_id
UNION ALL SELECT 'green', 1
UNION ALL SELECT 'fhv', 2
UNION ALL SELECT 'fhvhv', 3;



CREATE OR REPLACE TABLE GOLD.T7_ZONES AS
SELECT
    location_id::NUMBER AS pickup_location_id,
    borough,
    zone,
    service_zone,

    CASE borough
        WHEN 'Bronx' THEN 0
        WHEN 'Brooklyn' THEN 1
        WHEN 'Manhattan' THEN 2
        WHEN 'Queens' THEN 3
        WHEN 'Staten Island' THEN 4
        ELSE -1
    END AS borough_id,

    CASE service_zone
        WHEN 'Yellow Zone' THEN 0
        WHEN 'Boro Zone' THEN 1
        WHEN 'Airports' THEN 2
        ELSE -1
    END AS service_zone_id

FROM EXTERNAL_DATA.TAXI_ZONES
WHERE borough IN ('Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island')
  AND location_id IS NOT NULL;






CREATE OR REPLACE TABLE GOLD.T7_HOURS AS
WITH hour_numbers AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS hour_offset
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
)
SELECT
    DATEADD('hour', h.hour_offset, r.start_hour)::TIMESTAMP_NTZ AS pickup_hour
FROM hour_numbers h
CROSS JOIN GOLD.T7_COMMON_RANGE r
WHERE DATEADD('hour', h.hour_offset, r.start_hour) <= r.end_hour;



CREATE OR REPLACE TABLE GOLD.T7_HOURLY_COUNTS
CLUSTER BY (dataset, pickup_location_id, pickup_hour)
AS
WITH all_trips AS (

    SELECT
        'yellow' AS dataset,
        DATE_TRUNC('hour', pickup_datetime)::TIMESTAMP_NTZ AS pickup_hour,
        pu_location_id::NUMBER AS pickup_location_id
    FROM SILVER.YELLOW_TRIPS_CLEAN
    WHERE pu_location_id IS NOT NULL

    UNION ALL

    SELECT
        'green' AS dataset,
        DATE_TRUNC('hour', pickup_datetime)::TIMESTAMP_NTZ AS pickup_hour,
        pu_location_id::NUMBER AS pickup_location_id
    FROM SILVER.GREEN_TRIPS_CLEAN
    WHERE pu_location_id IS NOT NULL

    UNION ALL

    SELECT
        'fhv' AS dataset,
        DATE_TRUNC('hour', pickup_datetime)::TIMESTAMP_NTZ AS pickup_hour,
        pu_location_id::NUMBER AS pickup_location_id
    FROM SILVER.FHV_TRIPS_CLEAN
    WHERE pu_location_id IS NOT NULL

    UNION ALL

    SELECT
        'fhvhv' AS dataset,
        DATE_TRUNC('hour', pickup_datetime)::TIMESTAMP_NTZ AS pickup_hour,
        pu_location_id::NUMBER AS pickup_location_id
    FROM SILVER.FHVHV_TRIPS_CLEAN
    WHERE pu_location_id IS NOT NULL
)

SELECT
    t.dataset,
    t.pickup_hour,
    t.pickup_location_id,
    COUNT(*) AS trip_count
FROM all_trips t
JOIN GOLD.T7_COMMON_RANGE r
    ON t.pickup_hour >= r.start_hour
   AND t.pickup_hour <= r.end_hour
JOIN GOLD.T7_ZONES z
    ON t.pickup_location_id = z.pickup_location_id
GROUP BY
    t.dataset,
    t.pickup_hour,
    t.pickup_location_id;




CREATE OR REPLACE TABLE GOLD.T7_DEMAND_BASE
CLUSTER BY (dataset, pickup_location_id, pickup_hour)
AS
SELECT
    d.dataset,
    d.dataset_id,

    h.pickup_hour,

    z.pickup_location_id,
    z.borough,
    z.zone,
    z.service_zone,
    z.borough_id,
    z.service_zone_id,

    COALESCE(c.trip_count, 0) AS trip_count

FROM GOLD.T7_DATASETS d
CROSS JOIN GOLD.T7_HOURS h
CROSS JOIN GOLD.T7_ZONES z
LEFT JOIN GOLD.T7_HOURLY_COUNTS c
    ON d.dataset = c.dataset
   AND h.pickup_hour = c.pickup_hour
   AND z.pickup_location_id = c.pickup_location_id;







CREATE OR REPLACE TABLE GOLD.T7_WEATHER_DAILY AS
SELECT
    "DATE"::DATE AS weather_date,
    AVG(temp_max_c) AS temp_max_c,
    AVG(temp_min_c) AS temp_min_c,
    AVG(precipitation_mm) AS precipitation_mm
FROM EXTERNAL_DATA.T5_WEATHER
GROUP BY "DATE"::DATE;


CREATE OR REPLACE TABLE GOLD.T7_SCHOOLS_BY_ZONE AS
SELECT
    location_id::NUMBER AS pickup_location_id,
    MAX(school_count) AS school_count
FROM EXTERNAL_DATA.T5_SCHOOLS_BY_ZONE
GROUP BY location_id;


CREATE OR REPLACE TABLE GOLD.T7_ATTRACTIONS_BY_ZONE AS
SELECT
    location_id::NUMBER AS pickup_location_id,
    MAX(attraction_count) AS attraction_count
FROM EXTERNAL_DATA.T5_ATTRACTIONS_BY_ZONE
GROUP BY location_id;


CREATE OR REPLACE TABLE GOLD.T7_EVENTS_HOURLY AS
SELECT
    DATE_TRUNC('hour', active_hour)::TIMESTAMP_NTZ AS pickup_hour,
    UPPER(borough) AS borough_upper,
    SUM(active_events) AS active_events
FROM EXTERNAL_DATA.T5_EVENTS_HOURLY
GROUP BY
    DATE_TRUNC('hour', active_hour)::TIMESTAMP_NTZ,
    UPPER(borough);





CREATE OR REPLACE TABLE GOLD.T7_DEMAND_ENRICHED
CLUSTER BY (dataset, pickup_location_id, pickup_hour)
AS
SELECT
    b.dataset,
    b.dataset_id,

    b.pickup_hour,
    b.pickup_hour::DATE AS pickup_date,
    YEAR(b.pickup_hour) AS pickup_year,
    MONTH(b.pickup_hour) AS pickup_month,

    b.pickup_location_id,
    b.borough,
    b.zone,
    b.service_zone,
    b.borough_id,
    b.service_zone_id,

    b.trip_count,
    LN(1 + b.trip_count) AS log_trip_count,

    /* Time features */
    HOUR(b.pickup_hour) AS hour_of_day,
    DAYOFWEEKISO(b.pickup_hour) AS day_of_week,
    IFF(DAYOFWEEKISO(b.pickup_hour) IN (6, 7), 1, 0) AS is_weekend,
    IFF(
        HOUR(b.pickup_hour) BETWEEN 7 AND 9
        OR HOUR(b.pickup_hour) BETWEEN 16 AND 19,
        1,
        0
    ) AS is_rush_hour,
    IFF(HOUR(b.pickup_hour) BETWEEN 0 AND 5, 1, 0) AS is_night,

    /* Cyclic encodings */
    SIN(2 * PI() * HOUR(b.pickup_hour) / 24.0) AS hour_sin,
    COS(2 * PI() * HOUR(b.pickup_hour) / 24.0) AS hour_cos,

    SIN(2 * PI() * (DAYOFWEEKISO(b.pickup_hour) - 1) / 7.0) AS dow_sin,
    COS(2 * PI() * (DAYOFWEEKISO(b.pickup_hour) - 1) / 7.0) AS dow_cos,

    SIN(2 * PI() * (MONTH(b.pickup_hour) - 1) / 12.0) AS month_sin,
    COS(2 * PI() * (MONTH(b.pickup_hour) - 1) / 12.0) AS month_cos,

    /* T5 augmentation features */
    COALESCE(w.temp_max_c, 0) AS temp_max_c,
    COALESCE(w.temp_min_c, 0) AS temp_min_c,
    COALESCE(w.precipitation_mm, 0) AS precipitation_mm,
    IFF(w.weather_date IS NULL, 1, 0) AS weather_missing,

    COALESCE(s.school_count, 0) AS school_count,
    COALESCE(a.attraction_count, 0) AS attraction_count,
    COALESCE(e.active_events, 0) AS active_events

FROM GOLD.T7_DEMAND_BASE b

LEFT JOIN GOLD.T7_WEATHER_DAILY w
    ON b.pickup_hour::DATE = w.weather_date

LEFT JOIN GOLD.T7_SCHOOLS_BY_ZONE s
    ON b.pickup_location_id = s.pickup_location_id

LEFT JOIN GOLD.T7_ATTRACTIONS_BY_ZONE a
    ON b.pickup_location_id = a.pickup_location_id

LEFT JOIN GOLD.T7_EVENTS_HOURLY e
    ON b.pickup_hour = e.pickup_hour
   AND UPPER(b.borough) = e.borough_upper;







CREATE OR REPLACE TABLE GOLD.T7_DEMAND_FEATURES
CLUSTER BY (split, dataset, pickup_location_id, pickup_hour)
AS
WITH lagged AS (
    SELECT
        e.*,

        LAG(trip_count, 1) OVER (
            PARTITION BY dataset, pickup_location_id
            ORDER BY pickup_hour
        ) AS lag_1h_trip_count,

        LAG(trip_count, 24) OVER (
            PARTITION BY dataset, pickup_location_id
            ORDER BY pickup_hour
        ) AS lag_24h_trip_count,

        LAG(trip_count, 168) OVER (
            PARTITION BY dataset, pickup_location_id
            ORDER BY pickup_hour
        ) AS lag_168h_trip_count,

        AVG(trip_count) OVER (
            PARTITION BY dataset, pickup_location_id
            ORDER BY pickup_hour
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS rolling_24h_avg_trip_count,

        AVG(trip_count) OVER (
            PARTITION BY dataset, pickup_location_id
            ORDER BY pickup_hour
            ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
        ) AS rolling_168h_avg_trip_count

    FROM GOLD.T7_DEMAND_ENRICHED e
)

SELECT
    *,

    COALESCE(lag_1h_trip_count, 0) AS lag_1h_trip_count_filled,
    COALESCE(lag_24h_trip_count, 0) AS lag_24h_trip_count_filled,
    COALESCE(lag_168h_trip_count, 0) AS lag_168h_trip_count_filled,
    COALESCE(rolling_24h_avg_trip_count, 0) AS rolling_24h_avg_trip_count_filled,
    COALESCE(rolling_168h_avg_trip_count, 0) AS rolling_168h_avg_trip_count_filled,

    CASE
        WHEN pickup_hour < '2024-01-01'::TIMESTAMP_NTZ THEN 'train'
        WHEN pickup_hour < '2025-01-01'::TIMESTAMP_NTZ THEN 'validation'
        ELSE 'test'
    END AS split

FROM lagged;






CREATE OR REPLACE TABLE GOLD.T7_FEATURE_PROFILE AS
SELECT
    dataset,
    split,
    COUNT(*) AS "rows",
    SUM(trip_count) AS total_trips,
    AVG(trip_count) AS avg_trip_count,
    MAX(trip_count) AS max_trip_count,
    COUNT_IF(trip_count = 0) AS zero_demand_rows,
    ROUND(100 * COUNT_IF(trip_count = 0) / COUNT(*), 2) AS zero_demand_pct,
    COUNT_IF(weather_missing = 1) AS weather_missing_rows,
    ROUND(100 * COUNT_IF(weather_missing = 1) / COUNT(*), 2) AS weather_missing_pct
FROM GOLD.T7_DEMAND_FEATURES
GROUP BY dataset, split
ORDER BY dataset, split;




SELECT *
FROM GOLD.T7_DATASET_LIMITS
ORDER BY dataset;

SELECT *
FROM GOLD.T7_COMMON_RANGE;

SELECT *
FROM GOLD.T7_FEATURE_PROFILE
ORDER BY dataset, split;

SELECT
    MIN(pickup_hour) AS min_hour,
    MAX(pickup_hour) AS max_hour,
    COUNT(*) AS "rows",
    SUM(trip_count) AS total_trips
FROM GOLD.T7_DEMAND_FEATURES;

SELECT *
FROM GOLD.T7_DEMAND_FEATURES
WHERE trip_count > 0
ORDER BY pickup_hour, dataset, pickup_location_id
LIMIT 50;