import os
import re

SHARED_DIR = "/d/hpc/projects/FRI/bigdata/data/Taxi"
MISSING_DIR = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/data_missing"
OUT_FILE = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/file_manifest.txt"

STARTS = {
    "yellow": (2012, 1),
    "green": (2014, 1),
    "fhv": (2015, 1),
    "fhvhv": (2019, 2),
}

PATTERN = re.compile(r"^(yellow|green|fhv|fhvhv)_tripdata_(\d{4})-(\d{2})\.parquet$")

files = {}

for folder, priority in [(SHARED_DIR, 0), (MISSING_DIR, 1)]:
    for name in os.listdir(folder):
        m = PATTERN.match(name)
        if not m:
            continue
        dataset, y, mth = m.group(1), int(m.group(2)), int(m.group(3))
        if (y, mth) < STARTS[dataset]:
            continue

        key = (dataset, y, mth)
        path = os.path.join(folder, name)

        if key not in files or priority > files[key]["priority"]:
            files[key] = {"path": path, "priority": priority}

with open(OUT_FILE, "w") as f:
    for (dataset, y, mth) in sorted(files):
        f.write(files[(dataset, y, mth)]["path"] + "\n")

print(f"Wrote {len(files)} files to {OUT_FILE}")