import os
import time
import requests
import dask.bag as db

from dask.distributed import Client
from dask_jobqueue import SLURMCluster

SHARED_DIR = "/d/hpc/projects/FRI/bigdata/data/Taxi"
PROJECT_DIR = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project"
DOWNLOAD_DIR = os.path.join(PROJECT_DIR, "data_missing")
LOG_FILE = os.path.join(PROJECT_DIR, "t0_download.log")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ranges we are interested in (based on the given instructions)
DATASET_STARTS = {
    "yellow": (2012, 1),
    "green": (2014, 1),
    "fhv": (2015, 1),
    "fhvhv": (2019, 2),
}

# we have data available only up to 2025-01 in the cluster
SHARED_AVAILABLE_UNTIL = (2025, 1)

# to where we have data available on the source (as of april 2026)
T0_TARGET_END = (2026, 2)


def log(msg: str) -> None:
    print(msg, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(msg + "\n")


def month_range(start_year: int, start_month: int, end_year: int, end_month: int):
    y, m = start_year, start_month
    out = []
    while (y, m) <= (end_year, end_month):
        out.append((y, m))
        m += 1
        if m == 13:
            y += 1
            m = 1
    return out


def build_t0_candidates():
    candidates = []

    for dataset, (start_y, start_m) in DATASET_STARTS.items():
        # T0 only needs missing files beyond 2025-01
        effective_start = max((start_y, start_m), (2025, 2))

        for year, month in month_range(
            effective_start[0],
            effective_start[1],
            T0_TARGET_END[0],
            T0_TARGET_END[1],
        ):
            fname = f"{dataset}_tripdata_{year}-{month:02d}.parquet"
            candidates.append(
                {
                    "dataset": dataset,
                    "year": year,
                    "month": month,
                    "filename": fname,
                }
            )

    return candidates


def exists_in_shared(filename: str) -> bool:
    return os.path.exists(os.path.join(SHARED_DIR, filename))


def exists_in_local(filename: str) -> bool:
    return os.path.exists(os.path.join(DOWNLOAD_DIR, filename))


def download_one(item: dict) -> dict:
    dataset = item["dataset"]
    year = item["year"]
    month = item["month"]
    filename = item["filename"]

    shared_path = os.path.join(SHARED_DIR, filename)
    local_path = os.path.join(DOWNLOAD_DIR, filename)
    temp_path = local_path + ".part"
    url = f"{BASE_URL}/{filename}"

    if os.path.exists(shared_path):
        return {
            "file": filename,
            "dataset": dataset,
            "status": "already_in_shared",
        }

    if os.path.exists(local_path):
        return {
            "file": filename,
            "dataset": dataset,
            "status": "already_downloaded",
            "size_mb": round(os.path.getsize(local_path) / (1024 ** 2), 2),
        }

    try:
        with requests.get(url, stream=True, timeout=180) as response:
            if response.status_code == 404:
                return {
                    "file": filename,
                    "dataset": dataset,
                    "status": "not_available_on_source",
                }

            response.raise_for_status()

            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)

        os.replace(temp_path, local_path)

        return {
            "file": filename,
            "dataset": dataset,
            "status": "downloaded",
            "size_mb": round(os.path.getsize(local_path) / (1024 ** 2), 2),
        }

    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)

        return {
            "file": filename,
            "dataset": dataset,
            "status": "error",
            "error": str(e),
        }


def summarize_results(results):
    groups = {}
    for r in results:
        groups.setdefault(r["status"], []).append(r)

    for status in [
        "downloaded",
        "already_in_shared",
        "already_downloaded",
        "not_available_on_source",
        "error",
    ]:
        items = groups.get(status, [])
        log(f"{status}: {len(items)}")

    log("")

    if groups.get("downloaded"):
        log("Downloaded files:")
        for r in sorted(groups["downloaded"], key=lambda x: x["file"]):
            log(f"  {r['file']} ({r['size_mb']} MB)")
        log("")

    if groups.get("not_available_on_source"):
        log("Not available on source:")
        for r in sorted(groups["not_available_on_source"], key=lambda x: x["file"]):
            log(f"  {r['file']}")
        log("")

    if groups.get("error"):
        log("Errors:")
        for r in sorted(groups["error"], key=lambda x: x["file"]):
            log(f"  {r['file']}: {r['error']}")
        log("")


def main():
    start_time = time.time()

    with open(LOG_FILE, "w") as f:
        f.write(f"T0 started at {time.ctime()}\n")

    candidates = build_t0_candidates()

    log(f"Shared directory: {SHARED_DIR}")
    log(f"Download directory: {DOWNLOAD_DIR}")
    log(f"T0 candidate files: {len(candidates)}")
    log("")

    cluster = None
    client = None

    try:
        cluster = SLURMCluster(
            queue="all",
            processes=1,
            cores=2,
            memory="4GB",
            walltime="02:00:00",
            death_timeout=120,
            job_script_prologue=[
                "module load Anaconda3",
                'source "$(conda info --base)/etc/profile.d/conda.sh"',
                "conda activate bd311",
            ],
        )

        client = Client(cluster)
        cluster.scale(jobs=8)

        log("Cluster created")
        log(str(cluster))
        log("")
        log("Job script:")
        log(cluster.job_script())
        log("")

        bag = db.from_sequence(candidates, npartitions=min(32, len(candidates)))
        results = bag.map(download_one).compute()

        summarize_results(results)

        elapsed = time.time() - start_time
        log(f"Finished in {elapsed:.2f} seconds")

    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()


if __name__ == "__main__":
    main()