import os
import getpass
from snowflake.connector import connect

MANIFEST = "/d/hpc/projects/FRI/bigdata/students/mz1034/Project/file_manifest.txt"

# export SNOWFLAKE_USER=your_username
# export SNOWFLAKE_PASSWORD=your_password
# export SNOWFLAKE_ACCOUNT=your_account

passcode = getpass.getpass("Enter MFA code: ")

conn = connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse="BIGDATA_MZMB_WH",
    database="BIGDATA_TAXI_MZMB",
    schema="BRONZE",
    passcode=passcode,
)

cursor = conn.cursor()

def dataset_from_path(path):
    name = os.path.basename(path)
    return name.split("_")[0]  # yellow / green / fhv / fhvhv


with open(MANIFEST) as f:
    files = [line.strip() for line in f]

print(f"Uploading {len(files)} files...")

for path in files:
    dataset = dataset_from_path(path)
    stage_path = f"@tlc_stage/{dataset}"

    print(f"Uploading: {path}")

    cursor.execute(f"""
        PUT file://{path}
        {stage_path}
        AUTO_COMPRESS=FALSE
        OVERWRITE=FALSE
        PARALLEL=8
    """)

cursor.close()
conn.close()

print("Done.")