import json
import time
import getpass
from datetime import datetime, date
from decimal import Decimal

import snowflake.connector
from confluent_kafka import Producer


SNOWFLAKE_ACCOUNT = "FHB32629"
SNOWFLAKE_USER = "FERRET"
SNOWFLAKE_ROLE = "TRAINING_ROLE"
SNOWFLAKE_WAREHOUSE = "BIGDATA_MZMB_WH"
SNOWFLAKE_DATABASE = "BIGDATA_TAXI_MZMB"
SNOWFLAKE_SCHEMA = "GOLD"

KAFKA_BOOTSTRAP = "localhost:10000,localhost:10001"

LIMIT_ROWS = 1_000_000
FETCH_BATCH_SIZE = 10_000

TOPIC_YELLOW = "t6-yellow-2021"
TOPIC_FHVHV = "t6-fhvhv-2021"


QUERY = f"""
SELECT
    stream_id,
    dataset,
    pickup_datetime,
    dropoff_datetime,
    pu_location_id,
    do_location_id,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    trip_distance,
    trip_duration_sec,
    fare_amount
FROM GOLD.T6_STREAM_SOURCE_2021
ORDER BY stream_id
LIMIT {LIMIT_ROWS}
"""


def json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat(sep=" ")
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] delivery failed: {err}")


def main():
    password = getpass.getpass("Snowflake password: ")
    passcode = getpass.getpass("Snowflake MFA/TOTP code: ")

    print("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=password,
        passcode=passcode,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 100,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 100000,
    })

    cur = conn.cursor()
    cur.execute(QUERY)

    cols = [c[0].lower() for c in cur.description]

    total_sent = 0
    yellow_sent = 0
    fhvhv_sent = 0

    start = time.time()

    print(f"Producing up to {LIMIT_ROWS:,} rows...")

    try:
        while True:
            rows = cur.fetchmany(FETCH_BATCH_SIZE)

            if not rows:
                break

            for row in rows:
                record = dict(zip(cols, row))
                dataset = record["dataset"]

                if dataset == "yellow":
                    topic = TOPIC_YELLOW
                    yellow_sent += 1
                elif dataset == "fhvhv":
                    topic = TOPIC_FHVHV
                    fhvhv_sent += 1
                else:
                    continue

                producer.produce(
                    topic=topic,
                    key=str(record["stream_id"]),
                    value=json.dumps(record, default=json_default),
                    callback=delivery_report,
                )

                total_sent += 1

                # Let producer handle delivery callbacks.
                producer.poll(0)

            elapsed = time.time() - start
            rate = total_sent / elapsed if elapsed > 0 else 0

            print(
                f"sent={total_sent:,} "
                f"yellow={yellow_sent:,} "
                f"fhvhv={fhvhv_sent:,} "
                f"rate={rate:,.0f} msg/s"
            )

        print("Flushing producer...")
        producer.flush()

    finally:
        cur.close()
        conn.close()

    elapsed = time.time() - start

    print("Done.")
    print(f"Total sent: {total_sent:,}")
    print(f"Yellow sent: {yellow_sent:,}")
    print(f"FHVHV sent: {fhvhv_sent:,}")
    print(f"Elapsed seconds: {elapsed:.2f}")
    print(f"Average rate: {total_sent / elapsed:,.0f} msg/s")


if __name__ == "__main__":
    main()