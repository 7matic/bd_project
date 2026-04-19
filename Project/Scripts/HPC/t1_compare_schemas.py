import os
import re
import json
from collections import defaultdict
import pyarrow.parquet as pq

SHARED_DIR = "/d/hpc/projects/FRI/bigdata/data/Taxi"
MISSING_DIR = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/data_missing"
OUT_DIR = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/schema_report"

os.makedirs(OUT_DIR, exist_ok=True)

STARTS = {
    "yellow": (2012, 1),
    "green": (2014, 1),
    "fhv": (2015, 1),
    "fhvhv": (2019, 2),
}

PATTERN = re.compile(r"^(yellow|green|fhv|fhvhv)_tripdata_(\d{4})-(\d{2})\.parquet$")


def valid_file(dataset: str, year: int, month: int) -> bool:
    return (year, month) >= STARTS[dataset]


def build_file_list():
    files = {}
    for folder, priority in [(SHARED_DIR, 0), (MISSING_DIR, 1)]:
        for name in os.listdir(folder):
            m = PATTERN.match(name)
            if not m:
                continue

            dataset = m.group(1)
            year = int(m.group(2))
            month = int(m.group(3))

            if not valid_file(dataset, year, month):
                continue

            key = (dataset, year, month)
            path = os.path.join(folder, name)

            if key not in files or priority > files[key]["priority"]:
                files[key] = {
                    "dataset": dataset,
                    "year": year,
                    "month": month,
                    "path": path,
                    "filename": name,
                    "priority": priority,
                }

    grouped = defaultdict(list)
    for item in files.values():
        grouped[item["dataset"]].append(item)

    for dataset in grouped:
        grouped[dataset].sort(key=lambda x: (x["year"], x["month"]))

    return grouped


def read_schema(path: str):
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    result = {}
    for field in schema:
        result[field.name] = str(field.type)
    return result


def compare_dataset(dataset: str, files: list[dict]):
    schema_to_files = defaultdict(list)
    column_presence = defaultdict(list)
    column_types = defaultdict(lambda: defaultdict(list))
    per_file_schema = {}

    for item in files:
        schema = read_schema(item["path"])
        per_file_schema[item["filename"]] = schema

        schema_key = tuple(sorted(schema.items()))
        schema_to_files[schema_key].append(item["filename"])

        for col, dtype in schema.items():
            column_presence[col].append(item["filename"])
            column_types[col][dtype].append(item["filename"])

    all_files = [x["filename"] for x in files]
    all_columns = sorted(column_presence.keys())

    missing_by_file = {}
    for fname, schema in per_file_schema.items():
        present = set(schema.keys())
        missing = [c for c in all_columns if c not in present]
        if missing:
            missing_by_file[fname] = missing

    type_conflicts = {}
    for col, dtype_map in column_types.items():
        if len(dtype_map) > 1:
            type_conflicts[col] = {
                dtype: sorted(fnames) for dtype, fnames in dtype_map.items()
            }

    distinct_schemas = []
    for schema_key, fnames in schema_to_files.items():
        distinct_schemas.append({
            "num_files": len(fnames),
            "files": sorted(fnames),
            "schema": [{"column": c, "type": t} for c, t in schema_key],
        })

    distinct_schemas.sort(key=lambda x: (-x["num_files"], x["files"][0]))

    report = {
        "dataset": dataset,
        "num_files": len(files),
        "num_distinct_schemas": len(schema_to_files),
        "all_columns": all_columns,
        "distinct_schemas": distinct_schemas,
        "missing_by_file": missing_by_file,
        "type_conflicts": type_conflicts,
    }

    return report


def write_text_summary(report: dict):
    dataset = report["dataset"]
    out_path = os.path.join(OUT_DIR, f"{dataset}_schema_summary.txt")

    with open(out_path, "w") as f:
        f.write(f"DATASET: {dataset}\n")
        f.write(f"FILES: {report['num_files']}\n")
        f.write(f"DISTINCT SCHEMAS: {report['num_distinct_schemas']}\n\n")

        f.write("ALL COLUMNS:\n")
        for col in report["all_columns"]:
            f.write(f"  - {col}\n")

        f.write("\nDISTINCT SCHEMAS:\n")
        for i, s in enumerate(report["distinct_schemas"], start=1):
            f.write(f"\nSchema #{i}\n")
            f.write(f"Files: {s['num_files']}\n")
            preview = ", ".join(s["files"][:5])
            if len(s["files"]) > 5:
                preview += ", ..."
            f.write(f"Example files: {preview}\n")
            for col in s["schema"]:
                f.write(f"  {col['column']}: {col['type']}\n")

        f.write("\nTYPE CONFLICTS:\n")
        if not report["type_conflicts"]:
            f.write("  None\n")
        else:
            for col, dtype_map in report["type_conflicts"].items():
                f.write(f"\n  {col}\n")
                for dtype, fnames in dtype_map.items():
                    preview = ", ".join(fnames[:5])
                    if len(fnames) > 5:
                        preview += ", ..."
                    f.write(f"    {dtype}: {preview}\n")

        f.write("\nFILES WITH MISSING COLUMNS:\n")
        if not report["missing_by_file"]:
            f.write("  None\n")
        else:
            for fname, missing in sorted(report["missing_by_file"].items()):
                f.write(f"  {fname}: {', '.join(missing)}\n")


def main():
    grouped = build_file_list()

    master_summary = {}

    for dataset in ["yellow", "green", "fhv", "fhvhv"]:
        files = grouped.get(dataset, [])
        if not files:
            print(f"No files found for {dataset}")
            continue

        print(f"Processing {dataset}: {len(files)} files")
        report = compare_dataset(dataset, files)

        json_path = os.path.join(OUT_DIR, f"{dataset}_schema_report.json")
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2)

        write_text_summary(report)

        master_summary[dataset] = {
            "num_files": report["num_files"],
            "num_distinct_schemas": report["num_distinct_schemas"],
            "num_columns": len(report["all_columns"]),
            "num_type_conflicts": len(report["type_conflicts"]),
            "num_files_with_missing_columns": len(report["missing_by_file"]),
        }

    with open(os.path.join(OUT_DIR, "master_summary.json"), "w") as f:
        json.dump(master_summary, f, indent=2)

    print("\nDone. Reports written to:")
    print(OUT_DIR)


if __name__ == "__main__":
    main()