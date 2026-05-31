-- T5a: Fetch external data from APIs using Python stored procedures
-- Requires ALLOW_ALL_EAI external access integration

USE WAREHOUSE BIGDATA_MZMB_WH;
USE DATABASE BIGDATA_TAXI_MZMB;

CREATE OR REPLACE PROCEDURE EXTERNAL_DATA.FETCH_WEATHER()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_ALL_EAI)
HANDLER = 'run'
AS
$$
import requests
import pandas as pd

def run(session):
    chunks = [
        ("2012-01-01", "2014-12-31"),
        ("2015-01-01", "2017-12-31"),
        ("2018-01-01", "2020-12-31"),
        ("2021-01-01", "2023-12-31"),
        ("2024-01-01", "2025-12-31"),
    ]
    all_dfs = []
    for start, end in chunks:
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude=40.7831&longitude=-73.9712"
            f"&start_date={start}&end_date={end}"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
            f"&timezone=America%2FNew_York"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json()["daily"])
        df.rename(columns={"time": "DATE", "temperature_2m_max": "TEMP_MAX_C",
                           "temperature_2m_min": "TEMP_MIN_C", "precipitation_sum": "PRECIPITATION_MM"}, inplace=True)
        df["DATE"] = pd.to_datetime(df["DATE"])
        all_dfs.append(df)
    weather_df = pd.concat(all_dfs, ignore_index=True)
    session.create_dataframe(weather_df).write.mode("overwrite").save_as_table("EXTERNAL_DATA.T5_WEATHER")
    return f"T5_WEATHER created: {len(weather_df)} rows"
$$;

CREATE OR REPLACE PROCEDURE EXTERNAL_DATA.FETCH_EVENTS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_ALL_EAI)
HANDLER = 'run'
AS
$$
import requests
import pandas as pd

def run(session):
    base_url = "https://data.cityofnewyork.us/resource/bkfu-528j.json"
    quarters = []
    for year in [2020, 2021, 2022, 2023, 2024]:
        quarters.append((f"{year}-01-01", f"{year}-03-31"))
        quarters.append((f"{year}-04-01", f"{year}-06-30"))
        quarters.append((f"{year}-07-01", f"{year}-09-30"))
        quarters.append((f"{year}-10-01", f"{year}-12-31"))
    all_events = []
    for start, end in quarters:
        where = f"$where=start_date_time >= '{start}T00:00:00' AND start_date_time <= '{end}T23:59:59'"
        url = f"{base_url}?{where}&$limit=10000&$order=start_date_time"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        all_events.extend(resp.json())
    events_df = pd.DataFrame(all_events)
    events_clean = events_df[['event_id','event_name','start_date_time','end_date_time','event_type','event_borough']].copy()
    events_clean.columns = ['EVENT_ID','EVENT_NAME','START_DATETIME','END_DATETIME','EVENT_TYPE','EVENT_BOROUGH']
    events_clean['START_DATETIME'] = pd.to_datetime(events_clean['START_DATETIME'], errors='coerce')
    events_clean['END_DATETIME'] = pd.to_datetime(events_clean['END_DATETIME'], errors='coerce')
    events_clean['EVENT_BOROUGH'] = events_clean['EVENT_BOROUGH'].astype(str).str.strip().str.title()
    events_clean = events_clean.dropna(subset=['START_DATETIME'])
    session.create_dataframe(events_clean).write.mode("overwrite").save_as_table("EXTERNAL_DATA.T5_EVENTS")
    return f"T5_EVENTS created: {len(events_clean)} rows, {len(quarters)} quarters"
$$;

CREATE OR REPLACE PROCEDURE EXTERNAL_DATA.FETCH_SCHOOLS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_ALL_EAI)
HANDLER = 'run'
AS
$$
import requests
import pandas as pd

def run(session):
    url = "https://data.cityofnewyork.us/resource/n3p6-zve2.json?$limit=50000"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    schools_df = pd.DataFrame(resp.json())
    if 'location_1' in schools_df.columns:
        loc = schools_df['location_1'].apply(lambda x: x if isinstance(x, dict) else {})
        schools_df['LATITUDE'] = loc.apply(lambda x: float(x.get('latitude', 0)) if x.get('latitude') else None)
        schools_df['LONGITUDE'] = loc.apply(lambda x: float(x.get('longitude', 0)) if x.get('longitude') else None)
    result = schools_df[['LATITUDE', 'LONGITUDE']].copy()
    result['SCHOOL_NAME'] = schools_df['school_name'].astype(str)
    result = result.dropna(subset=['LATITUDE', 'LONGITUDE'])
    result = result[(result['LATITUDE'] > 40.4) & (result['LATITUDE'] < 41.0)]
    result = result[(result['LONGITUDE'] > -74.3) & (result['LONGITUDE'] < -73.7)]
    session.create_dataframe(result).write.mode("overwrite").save_as_table("EXTERNAL_DATA.T5_SCHOOLS_RAW")
    return f"T5_SCHOOLS_RAW created: {len(result)} rows"
$$;

CREATE OR REPLACE PROCEDURE EXTERNAL_DATA.FETCH_ATTRACTIONS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_ALL_EAI)
HANDLER = 'run'
AS
$$
import requests
import pandas as pd

def run(session):
    url = "https://data.cityofnewyork.us/resource/fn6f-htvy.json?$limit=50000"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    museums_df = pd.DataFrame(resp.json())
    def extract_coords(geom):
        if isinstance(geom, dict):
            coords = geom.get('coordinates', [None, None])
            if coords and len(coords) >= 2:
                return coords[0], coords[1]
        return None, None
    coords = museums_df['the_geom'].apply(extract_coords)
    museums_df['LONGITUDE'] = coords.apply(lambda x: x[0])
    museums_df['LATITUDE'] = coords.apply(lambda x: x[1])
    result = museums_df[['LATITUDE', 'LONGITUDE']].copy()
    result['ATTRACTION_NAME'] = museums_df['name'].astype(str)
    result['LATITUDE'] = pd.to_numeric(result['LATITUDE'], errors='coerce')
    result['LONGITUDE'] = pd.to_numeric(result['LONGITUDE'], errors='coerce')
    result = result.dropna(subset=['LATITUDE', 'LONGITUDE'])
    result = result[(result['LATITUDE'] > 40.4) & (result['LATITUDE'] < 41.0)]
    result = result[(result['LONGITUDE'] > -74.3) & (result['LONGITUDE'] < -73.7)]
    session.create_dataframe(result).write.mode("overwrite").save_as_table("EXTERNAL_DATA.T5_ATTRACTIONS_RAW")
    return f"T5_ATTRACTIONS_RAW created: {len(result)} rows"
$$;

CALL EXTERNAL_DATA.FETCH_WEATHER();
CALL EXTERNAL_DATA.FETCH_EVENTS();
CALL EXTERNAL_DATA.FETCH_SCHOOLS();
CALL EXTERNAL_DATA.FETCH_ATTRACTIONS();
