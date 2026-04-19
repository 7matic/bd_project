import os
import re
import uuid
import time
import shutil
import warnings
from collections import defaultdict

warnings.simplefilter("ignore", category=FutureWarning)

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from dask import delayed, compute
from dask.distributed import Client
from dask_jobqueue import SLURMCluster

PROJECT_DIR = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project"
MANIFEST_FILE = os.path.join(PROJECT_DIR, "file_manifest.txt")

OUT_ROOT = os.path.join(PROJECT_DIR, "t1_outputs")
TMP_ROOT = os.path.join(OUT_ROOT, "tmp_year_chunks")
FINAL_ROOT = os.path.join(OUT_ROOT, "processed_by_year")
LOG_FILE = os.path.join(PROJECT_DIR, "t1_repartition.log")

os.makedirs(OUT_ROOT, exist_ok=True)
os.makedirs(TMP_ROOT, exist_ok=True)
os.makedirs(FINAL_ROOT, exist_ok=True)

TS = pa.timestamp("us")
STR = pa.string()
F64 = pa.float64()
I64 = pa.int64()

VALID_YEAR_RANGE = {
    "yellow": (2012, 2026),
    "green": (2014, 2026),
    "fhv": (2015, 2026),
    "fhvhv": (2019, 2026),
}

SCHEMAS = {
    "yellow": pa.schema([
        ("VendorID", I64),
        ("tpep_pickup_datetime", TS),
        ("tpep_dropoff_datetime", TS),
        ("passenger_count", F64),
        ("trip_distance", F64),
        ("RatecodeID", F64),
        ("store_and_fwd_flag", STR),
        ("PULocationID", I64),
        ("DOLocationID", I64),
        ("payment_type", I64),
        ("fare_amount", F64),
        ("extra", F64),
        ("mta_tax", F64),
        ("tip_amount", F64),
        ("tolls_amount", F64),
        ("improvement_surcharge", F64),
        ("total_amount", F64),
        ("congestion_surcharge", F64),
        ("airport_fee", F64),
        ("cbd_congestion_fee", F64),
        ("year", I64),
    ]),
    "green": pa.schema([
        ("VendorID", I64),
        ("lpep_pickup_datetime", TS),
        ("lpep_dropoff_datetime", TS),
        ("store_and_fwd_flag", STR),
        ("RatecodeID", F64),
        ("PULocationID", I64),
        ("DOLocationID", I64),
        ("passenger_count", F64),
        ("trip_distance", F64),
        ("fare_amount", F64),
        ("extra", F64),
        ("mta_tax", F64),
        ("tip_amount", F64),
        ("tolls_amount", F64),
        ("ehail_fee", F64),
        ("improvement_surcharge", F64),
        ("total_amount", F64),
        ("payment_type", F64),
        ("trip_type", I64),
        ("congestion_surcharge", F64),
        ("cbd_congestion_fee", F64),
        ("year", I64),
    ]),
    "fhv": pa.schema([
        ("dispatching_base_num", STR),
        ("pickup_datetime", TS),
        ("dropoff_datetime", TS),
        ("PUlocationID", I64),
        ("DOlocationID", I64),
        ("SR_Flag", I64),
        ("Affiliated_base_number", STR),
        ("year", I64),
    ]),
    "fhvhv": pa.schema([
        ("hvfhs_license_num", STR),
        ("dispatching_base_num", STR),
        ("originating_base_num", STR),
        ("request_datetime", TS),
        ("on_scene_datetime", TS),
        ("pickup_datetime", TS),
        ("dropoff_datetime", TS),
        ("PULocationID", I64),
        ("DOLocationID", I64),
        ("trip_miles", F64),
        ("trip_time", I64),
        ("base_passenger_fare", F64),
        ("tolls", F64),
        ("bcf", F64),
        ("sales_tax", F64),
        ("congestion_surcharge", F64),
        ("airport_fee", F64),
        ("tips", F64),
        ("driver_pay", F64),
        ("shared_request_flag", STR),
        ("shared_match_flag", STR),
        ("access_a_ride_flag", STR),
        ("wav_request_flag", STR),
        ("wav_match_flag", STR),
        ("cbd_congestion_fee", F64),
        ("year", I64),
    ]),
}

PICKUP_COL = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
    "fhv": "pickup_datetime",
    "fhvhv": "pickup_datetime",
}

PATTERN = re.compile(r"^(yellow|green|fhv|fhvhv)_tripdata_(\d{4})-(\d{2})\.parquet$")


def log(msg: str):
    print(msg, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(msg + "\n")


def load_manifest():
    with open(MANIFEST_FILE) as f:
        files = [line.strip() for line in f if line.strip()]

    grouped = defaultdict(list)
    for path in files:
        name = os.path.basename(path)
        m = PATTERN.match(name)
        if not m:
            continue
        dataset = m.group(1)
        grouped[dataset].append(path)

    for dataset in grouped:
        grouped[dataset].sort()

    return grouped


def null_array(n, typ):
    return pa.nulls(n, type=typ)


def standardize_name_map(dataset: str):
    if dataset == "yellow":
        return {
            "Airport_fee": "airport_fee",
        }
    if dataset == "fhv":
        return {
            "DOlocationID": "DOlocationID",
            "PUlocationID": "PUlocationID",
            "dropOff_datetime": "dropoff_datetime",
        }
    return {}


def normalize_table(table: pa.Table, dataset: str) -> pa.Table:
    rename_map = standardize_name_map(dataset)

    # rename selected columns
    current_names = table.column_names
    new_names = [rename_map.get(name, name) for name in current_names]
    if new_names != current_names:
        table = table.rename_columns(new_names)

    target_schema = SCHEMAS[dataset]
    pickup_col = PICKUP_COL[dataset]

    arrays = {}
    n = table.num_rows

    for field in target_schema:
        col = field.name
        typ = field.type

        if col == "year":
            continue

        if col in table.column_names:
            arr = table[col]

            if pa.types.is_string(typ):
                arr = pc.cast(arr, STR, safe=False)
            elif pa.types.is_timestamp(typ):
                if pa.types.is_timestamp(arr.type):
                    arr = pc.cast(arr, TS, safe=False)
                elif pa.types.is_string(arr.type) or pa.types.is_large_string(arr.type):
                    arr = pc.strptime(
                        arr,
                        format="%Y-%m-%d %H:%M:%S",
                        unit="us",
                        error_is_null=True
                    )
                else:
                    arr = pc.cast(arr, TS, safe=False)
            else:
                arr = pc.cast(arr, typ, safe=False)

            arrays[col] = arr
        else:
            arrays[col] = null_array(n, typ)

    pickup_arr = arrays[pickup_col]
    year_arr = pc.year(pickup_arr)
    arrays["year"] = pc.cast(year_arr, I64, safe=False)

    ordered = [arrays[f.name] for f in target_schema]
    return pa.Table.from_arrays(ordered, schema=target_schema)


def process_one_file(path: str, dataset: str):
    name = os.path.basename(path)
    log(f"Processing {dataset}: {name}")

    pf = pq.ParquetFile(path)
    writers = {}
    written_buckets = set()

    valid_min, valid_max = VALID_YEAR_RANGE[dataset]

    try:
        for batch in pf.iter_batches(batch_size=250_000):
            table = pa.Table.from_batches([batch])
            table = normalize_table(table, dataset)

            year_arr = table["year"]

            # valid years
            valid_mask = pc.and_(
                pc.greater_equal(year_arr, pa.scalar(valid_min, type=pa.int64())),
                pc.less_equal(year_arr, pa.scalar(valid_max, type=pa.int64()))
            )

            valid_table = table.filter(valid_mask)
            invalid_table = table.filter(pc.invert(valid_mask))

            # write valid buckets by actual year
            if valid_table.num_rows > 0:
                valid_years = pc.unique(valid_table["year"]).to_pylist()

                for y in valid_years:
                    if y is None:
                        continue

                    sub = valid_table.filter(pc.equal(valid_table["year"], y))
                    if sub.num_rows == 0:
                        continue

                    bucket_name = f"year={y}"
                    year_dir = os.path.join(TMP_ROOT, dataset, bucket_name)
                    os.makedirs(year_dir, exist_ok=True)

                    writer_key = (dataset, bucket_name)

                    if writer_key not in writers:
                        out_path = os.path.join(year_dir, f"{uuid.uuid4().hex}.parquet")
                        writers[writer_key] = pq.ParquetWriter(
                            out_path,
                            schema=SCHEMAS[dataset],
                            compression="snappy",
                        )

                    writers[writer_key].write_table(sub, row_group_size=2_000_000)
                    written_buckets.add(bucket_name)

            # write invalid years separately
            if invalid_table.num_rows > 0:
                bucket_name = "year=INVALID"
                year_dir = os.path.join(TMP_ROOT, dataset, bucket_name)
                os.makedirs(year_dir, exist_ok=True)

                writer_key = (dataset, bucket_name)

                if writer_key not in writers:
                    out_path = os.path.join(year_dir, f"{uuid.uuid4().hex}.parquet")
                    writers[writer_key] = pq.ParquetWriter(
                        out_path,
                        schema=SCHEMAS[dataset],
                        compression="snappy",
                    )

                writers[writer_key].write_table(invalid_table, row_group_size=2_000_000)
                written_buckets.add(bucket_name)

    finally:
        for w in writers.values():
            w.close()

    return {"file": name, "written_buckets": len(written_buckets)}


def compact_one_year(dataset: str, year_dir_name: str):
    src_dir = os.path.join(TMP_ROOT, dataset, year_dir_name)
    if not os.path.exists(src_dir):
        return {"dataset": dataset, "year_dir": year_dir_name, "status": "missing"}

    dest_dir = os.path.join(FINAL_ROOT, dataset, year_dir_name)
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir, exist_ok=True)

    log(f"Compacting {dataset}/{year_dir_name}")

    dataset_obj = ds.dataset(src_dir, format="parquet")

    ds.write_dataset(
        dataset_obj,
        base_dir=dest_dir,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
        max_rows_per_group=2_000_000,
        min_rows_per_group=1_000_000,
        max_rows_per_file=4_000_000,
        basename_template="part-{i}.parquet",
    )

    return {"dataset": dataset, "year_dir": year_dir_name, "status": "done"}


def collect_year_dirs():
    jobs = []
    for dataset in ["yellow", "green", "fhv", "fhvhv"]:
        dataset_tmp = os.path.join(TMP_ROOT, dataset)
        if not os.path.exists(dataset_tmp):
            continue
        for year_dir_name in sorted(os.listdir(dataset_tmp)):
            full = os.path.join(dataset_tmp, year_dir_name)
            if os.path.isdir(full):
                jobs.append((dataset, year_dir_name))
    return jobs

def main():
    start = time.time()

    with open(LOG_FILE, "w") as f:
        f.write(f"T1 started at {time.ctime()}\n")

    grouped = load_manifest()

    total_files = sum(len(v) for v in grouped.values())
    log(f"Total input files: {total_files}")
    for dataset in ["yellow", "green", "fhv", "fhvhv"]:
        log(f"  {dataset}: {len(grouped.get(dataset, []))}")

    cluster = SLURMCluster(
        queue="all",
        processes=1,
        cores=2,
        memory="16GB",
        walltime="08:00:00",
        death_timeout=120,
        job_script_prologue=[
            "module load Anaconda3",
            'source "$(conda info --base)/etc/profile.d/conda.sh"',
            "conda activate bd311",
        ],
    )
    client = Client(cluster)
    cluster.scale(jobs=8)

    log("")
    log("Cluster:")
    log(str(cluster))
    log("")
    log("Job script:")
    log(cluster.job_script())
    log("")

    try:
        # normalize every file in parallel
        tasks_phase1 = []
        for dataset, files in grouped.items():
            for path in files:
                tasks_phase1.append(delayed(process_one_file)(path, dataset))

        log(f"Phase 1 tasks: {len(tasks_phase1)}")
        results1 = compute(*tasks_phase1)

        log("Phase 1 done")
        log(f"Processed files: {len(results1)}")
        log("")

        elapsed = time.time() - start
        log(f"Finished successfully in {elapsed:.2f} seconds")

    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()