import os
import re
import getpass
import snowflake.connector

SHARED_DIR = "/d/hpc/projects/FRI/bigdata/data/Taxi"
MISSING_DIR = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/data_missing"

# switch the names here between yellow, green, fhv and fhvhv to upload different datasets
DATASET = "fhvhv"
START_YEAR = 2019
START_MONTH = 2
STAGE = "@FHVHV_RAW_STAGE"
PATTERN = re.compile(r"^fhvhv_tripdata_(\d{4})-(\d{2})\.parquet$")


def collect_yellow_files():
    files = {}

    # priority 0 = shared original files
    # priority 1 = newer/missing files, prefer these if duplicate exists
    for folder, priority in [(SHARED_DIR, 0), (MISSING_DIR, 1)]:
        for name in os.listdir(folder):
            m = PATTERN.match(name)
            if not m:
                continue

            year = int(m.group(1))
            month = int(m.group(2))

            if (year, month) < (START_YEAR, START_MONTH):
                continue

            key = (year, month)
            path = os.path.join(folder, name)

            if key not in files or priority > files[key]["priority"]:
                files[key] = {
                    "path": path,
                    "filename": name,
                    "priority": priority,
                }

    return [files[k]["path"] for k in sorted(files.keys())]


def main():
    files = collect_yellow_files()

    print(f"Found {len(files)} yellow files to upload.")
    print("First 5:")
    for f in files[:5]:
        print(" ", f)
    print("Last 5:")
    for f in files[-5:]:
        print(" ", f)

    confirm = input("Upload these files to Snowflake? [y/N]: ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    mfa_code = getpass.getpass("Enter MFA code: ")

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse="BIGDATA_MZMB_WH",
        database="BIGDATA_TAXI_MZMB",
        schema="BRONZE",
        authenticator="username_password_mfa",
        passcode=mfa_code,
    )

    cur = conn.cursor()

    try:
        for i, path in enumerate(files, start=1):
            print(f"[{i}/{len(files)}] Uploading {path}")

            cur.execute(f"""
                PUT file://{path}
                {STAGE}
                AUTO_COMPRESS=FALSE
                OVERWRITE=FALSE
                PARALLEL=16
            """)

            for row in cur.fetchall():
                print("   ", row)

    finally:
        cur.close()
        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()
