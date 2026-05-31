import json
import time
import math
from datetime import datetime
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from sklearn.cluster import Birch



KAFKA_BOOTSTRAP = "localhost:10000,localhost:10001"
TOPICS = ["t6-yellow-2021", "t6-fhvhv-2021"]

MAX_MESSAGES = 1_000_000
BATCH_SIZE = 5_000

# final number of clusters after BIRCH builds the CF tree
N_CLUSTERS = 6

# BIRCH threshold controls how many subclusters are created
# lower = more fine-grained
# higher = larger subclusters
BIRCH_THRESHOLD = 0.35
BIRCH_BRANCHING_FACTOR = 50

OUTPUT_SUMMARY = "t6_birch_cluster_summary_2021.csv"
OUTPUT_ASSIGNMENTS = "t6_birch_assignments_sample_2021.csv"
OUTPUT_PROGRESS = "t6_birch_progress_2021.csv"

# store a sample
MAX_STORED_ASSIGNMENTS = 250_000


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def parse_hour_day(pickup_datetime):
    try:
        dt = datetime.fromisoformat(str(pickup_datetime))
        return dt.hour, dt.weekday()
    except Exception:
        return 0, 0


def build_features(record):
    dataset = record.get("dataset", "")
    dataset_flag = 1.0 if dataset == "fhvhv" else 0.0

    hour, dow = parse_hour_day(record.get("pickup_datetime"))

    hour_sin = math.sin(2 * math.pi * hour / 24)
    hour_cos = math.cos(2 * math.pi * hour / 24)
    dow_sin = math.sin(2 * math.pi * dow / 7)
    dow_cos = math.cos(2 * math.pi * dow / 7)

    pu_location_id = safe_float(record.get("pu_location_id")) / 265.0
    do_location_id = safe_float(record.get("do_location_id")) / 265.0

    distance = max(safe_float(record.get("trip_distance")), 0.0)
    duration = max(safe_float(record.get("trip_duration_sec")), 0.0)
    fare = max(safe_float(record.get("fare_amount")), 0.0)

    distance_scaled = min(math.log1p(distance) / math.log1p(50), 1.5)
    duration_scaled = min(math.log1p(duration) / math.log1p(7200), 1.5)
    fare_scaled = min(math.log1p(fare) / math.log1p(200), 1.5)

    return np.array([
        dataset_flag,
        hour_sin,
        hour_cos,
        dow_sin,
        dow_cos,
        pu_location_id,
        do_location_id,
        distance_scaled,
        duration_scaled,
        fare_scaled,
    ], dtype=np.float64)


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"t6-birch-consumer-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    })

    consumer.subscribe(TOPICS)

    # Use n_clusters=None while streaming
    # this lets BIRCH incrementally build the CF tree
    birch = Birch(
        n_clusters=None,
        threshold=BIRCH_THRESHOLD,
        branching_factor=BIRCH_BRANCHING_FACTOR,
    )

    batch_features = []
    batch_records = []

    stored_features = []
    stored_records = []

    progress_rows = []

    total_seen = 0
    total_trained = 0
    empty_polls = 0
    max_empty_polls = 12

    start = time.time()

    print("Starting BIRCH stream clustering consumer...")
    print(f"Topics: {TOPICS}")
    print(f"Max messages: {MAX_MESSAGES:,}")

    try:
        while total_seen < MAX_MESSAGES:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                empty_polls += 1
                print(f"No message received. empty_polls={empty_polls}/{max_empty_polls}")

                if empty_polls >= max_empty_polls:
                    print("Stopping because no more messages arrived.")
                    break

                continue

            empty_polls = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"Kafka error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            features = build_features(record)

            batch_features.append(features)
            batch_records.append(record)
            total_seen += 1

            if len(stored_features) < MAX_STORED_ASSIGNMENTS:
                stored_features.append(features)
                stored_records.append(record)

            if len(batch_features) >= BATCH_SIZE:
                X = np.vstack(batch_features)

                birch.partial_fit(X)

                total_trained += len(batch_features)
                elapsed = time.time() - start
                rate = total_trained / elapsed if elapsed > 0 else 0

                n_subclusters = len(birch.subcluster_centers_) if hasattr(birch, "subcluster_centers_") else None

                progress_rows.append({
                    "messages_trained": total_trained,
                    "elapsed_seconds": elapsed,
                    "messages_per_second": rate,
                    "n_subclusters": n_subclusters,
                })

                print(
                    f"trained={total_trained:,} "
                    f"seen={total_seen:,} "
                    f"rate={rate:,.0f} msg/s "
                    f"subclusters={n_subclusters}"
                )

                batch_features = []
                batch_records = []

        # train on final partial batch
        if batch_features:
            X = np.vstack(batch_features)
            birch.partial_fit(X)
            total_trained += len(batch_features)

    finally:
        consumer.close()

    print("Finalizing BIRCH global clustering...")

    # final global clustering over the BIRCH subclusters
    birch.set_params(n_clusters=N_CLUSTERS)
    birch.partial_fit()

    X_sample = np.vstack(stored_features)
    labels = birch.predict(X_sample)

    assignment_rows = []
    cluster_counts = Counter()
    cluster_dataset_counts = defaultdict(Counter)
    cluster_borough_counts = defaultdict(Counter)
    cluster_zone_counts = defaultdict(Counter)

    cluster_sums = defaultdict(lambda: {
        "distance": 0.0,
        "duration": 0.0,
        "fare": 0.0,
    })

    for record, label in zip(stored_records, labels):
        label = int(label)

        dataset = record.get("dataset", "unknown")
        borough = record.get("pickup_borough", "unknown")
        zone = record.get("pickup_zone", "unknown")

        distance = safe_float(record.get("trip_distance"))
        duration = safe_float(record.get("trip_duration_sec"))
        fare = safe_float(record.get("fare_amount"))

        cluster_counts[label] += 1
        cluster_dataset_counts[label][dataset] += 1
        cluster_borough_counts[label][borough] += 1
        cluster_zone_counts[label][zone] += 1

        cluster_sums[label]["distance"] += distance
        cluster_sums[label]["duration"] += duration
        cluster_sums[label]["fare"] += fare

        assignment_rows.append({
            "stream_id": record.get("stream_id"),
            "dataset": dataset,
            "pickup_datetime": record.get("pickup_datetime"),
            "pickup_borough": borough,
            "pickup_zone": zone,
            "pu_location_id": record.get("pu_location_id"),
            "do_location_id": record.get("do_location_id"),
            "trip_distance": distance,
            "trip_duration_sec": duration,
            "fare_amount": fare,
            "cluster_id": label,
        })

    summary_rows = []

    for cluster_id in range(N_CLUSTERS):
        count = cluster_counts[cluster_id]

        if count == 0:
            continue

        summary_rows.append({
            "cluster_id": cluster_id,
            "trip_count_sample": count,
            "sample_pct": 100 * count / len(labels),
            "avg_distance": cluster_sums[cluster_id]["distance"] / count,
            "avg_duration_sec": cluster_sums[cluster_id]["duration"] / count,
            "avg_fare_amount": cluster_sums[cluster_id]["fare"] / count,
            "dataset_distribution": dict(cluster_dataset_counts[cluster_id]),
            "top_pickup_boroughs": dict(cluster_borough_counts[cluster_id].most_common(5)),
            "top_pickup_zones": dict(cluster_zone_counts[cluster_id].most_common(5)),
        })

    pd.DataFrame(summary_rows).to_csv(OUTPUT_SUMMARY, index=False)
    pd.DataFrame(assignment_rows).to_csv(OUTPUT_ASSIGNMENTS, index=False)
    pd.DataFrame(progress_rows).to_csv(OUTPUT_PROGRESS, index=False)

    elapsed = time.time() - start

    print("\nDone.")
    print(f"Messages seen: {total_seen:,}")
    print(f"Messages trained: {total_trained:,}")
    print(f"Stored assignment sample: {len(assignment_rows):,}")
    print(f"Elapsed seconds: {elapsed:.2f}")
    print(f"Average training rate: {total_trained / elapsed:,.0f} msg/s")
    print(f"Wrote: {OUTPUT_SUMMARY}")
    print(f"Wrote: {OUTPUT_ASSIGNMENTS}")
    print(f"Wrote: {OUTPUT_PROGRESS}")


if __name__ == "__main__":
    main()