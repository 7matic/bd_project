import os
import json
from collections import defaultdict
import pyarrow.parquet as pq

ROOT = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/t1_outputs/tmp_year_chunks"
OUT_DIR = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/t1_schema_check"

os.makedirs(OUT_DIR, exist_ok=True)

EXPECTED_YEARS = {
    "yellow": set(map(str, range(2012, 2027))) | {"INVALID"},
    "green": set(map(str, range(2014, 2027))) | {"INVALID"},
    "fhv": set(map(str, range(2015, 2027))) | {"INVALID"},
    "fhvhv": set(map(str, range(2019, 2027))) | {"INVALID"},
}


def read_schema(path):
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    return {field.name: str(field.type) for field in schema}


def collect_files(dataset_dir):
    files = []
    for year_dir in sorted(os.listdir(dataset_dir)):
        year_path = os.path.join(dataset_dir, year_dir)
        if not os.path.isdir(year_path):
            continue
        for name in os.listdir(year_path):
            if name.endswith(".parquet"):
                files.append((year_dir, os.path.join(year_path, name)))
    return files


def main():
    master = {}

    for dataset in ["yellow", "green", "fhv", "fhvhv"]:
        dataset_dir = os.path.join(ROOT, dataset)
        if not os.path.exists(dataset_dir):
            print(f"Missing dataset dir: {dataset_dir}")
            continue

        files = collect_files(dataset_dir)
        schema_groups = defaultdict(list)
        type_conflicts = defaultdict(lambda: defaultdict(list))
        year_dirs = set()

        for year_dir, path in files:
            year_value = year_dir.replace("year=", "")
            year_dirs.add(year_value)

            schema = read_schema(path)
            schema_key = tuple(sorted(schema.items()))
            schema_groups[schema_key].append(path)

            for col, typ in schema.items():
                type_conflicts[col][typ].append(path)

        distinct_schemas = []
        for schema_key, paths in schema_groups.items():
            distinct_schemas.append({
                "num_files": len(paths),
                "example_files": paths[:5],
                "schema": [{"column": c, "type": t} for c, t in schema_key],
            })

        real_type_conflicts = {}
        for col, variants in type_conflicts.items():
            if len(variants) > 1:
                real_type_conflicts[col] = {
                    typ: paths[:5] for typ, paths in variants.items()
                }

        unexpected_years = sorted(year_dirs - EXPECTED_YEARS[dataset])
        missing_expected_years = sorted(EXPECTED_YEARS[dataset] - year_dirs)

        report = {
            "dataset": dataset,
            "num_files": len(files),
            "num_distinct_schemas": len(schema_groups),
            "num_type_conflicts": len(real_type_conflicts),
            "year_dirs": sorted(year_dirs),
            "unexpected_years": unexpected_years,
            "missing_expected_years": missing_expected_years,
            "type_conflicts": real_type_conflicts,
            "distinct_schemas": distinct_schemas,
        }

        master[dataset] = {
            "num_files": report["num_files"],
            "num_distinct_schemas": report["num_distinct_schemas"],
            "num_type_conflicts": report["num_type_conflicts"],
            "unexpected_years": report["unexpected_years"],
        }

        with open(os.path.join(OUT_DIR, f"{dataset}_processed_schema_report.json"), "w") as f:
            json.dump(report, f, indent=2)

    with open(os.path.join(OUT_DIR, "master_summary.json"), "w") as f:
        json.dump(master, f, indent=2)

    print("Done. Reports written to:", OUT_DIR)


if __name__ == "__main__":
    main()