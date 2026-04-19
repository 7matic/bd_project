import os
import pyarrow.dataset as ds
import pandas as pd

ROOT = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/t1_outputs/tmp_year_chunks"

DATASETS = {
    "yellow": {
        "path": os.path.join(ROOT, "yellow", "year=2026"),
        "pickup_col": "tpep_pickup_datetime",
    },
    "green": {
        "path": os.path.join(ROOT, "green", "year=2026"),
        "pickup_col": "lpep_pickup_datetime",
    },
    "fhv": {
        "path": os.path.join(ROOT, "fhv", "year=2026"),
        "pickup_col": "pickup_datetime",
    },
    "fhvhv": {
        "path": os.path.join(ROOT, "fhvhv", "year=2026"),
        "pickup_col": "pickup_datetime",
    },
}

for dataset, info in DATASETS.items():
    path = info["path"]
    pickup_col = info["pickup_col"]

    print("=" * 80)
    print(f"DATASET: {dataset}")
    print(f"PATH: {path}")

    if not os.path.exists(path):
        print("Missing year=2026 directory")
        continue

    dset = ds.dataset(path, format="parquet")
    table = dset.to_table()
    df = table.to_pandas()

    print(f"Rows: {len(df):,}")

    if len(df) == 0:
        print("No rows")
        continue

    print(f"Min {pickup_col}: {df[pickup_col].min()}")
    print(f"Max {pickup_col}: {df[pickup_col].max()}")

    print("\nSample rows:")
    print(df.head(5).to_string(index=False))
    print()