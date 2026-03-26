# ABS Building Approvals ingestion (Bronze S3)
# ------------------------------------------------------------------
# Writes to:
# s3://<S3_BUCKET>/<S3_PROJECT_PREFIX>/bronze/ingestion_date=YYYY-MM-DD/part-*.parquet
# plus:
# s3://<S3_BUCKET>/<S3_PROJECT_PREFIX>/bronze/ingestion_date=YYYY-MM-DD/_MANIFEST.json

import os
import io
import tempfile
import logging
import pandas as pd

import boto3
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout, Timeout
from ingestion.functions import standardise, build_abs_url, load_sa2_codes, list_region_files, ABS_API_HEADERS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------
# CONFIG
# -----------------------------
ingestion_date = os.environ["INGESTION_DATE"]
run_id = os.environ["RUN_ID"]

START_PERIOD = os.environ["START_PERIOD"]
END_PERIOD = os.environ["END_PERIOD"]

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def build_requests_session() -> requests.Session:
    # Creates a resilient HTTP request session
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def s3_client(aws_region: str):
    # Creates an S3 client in the chosen AWS region
    return boto3.client("s3", region_name=aws_region)

def delete_s3_prefix(bucket: str, prefix: str, region: str) -> None:
    # Performs a folder wipe in S3 for existing data extraction for overwriting (idempotency) based on prefix
    s3 = s3_client(region)
    paginator = s3.get_paginator("list_objects_v2")  # Prepares a paginator in case return list of objects exceeds 1,000
    batch: list[dict] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):  # Contents of each object includes metadata including the key
            batch.append({"Key": obj["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                batch = []

    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})

def upload_to_s3(local_path: str, bucket: str, key: str, region: str) -> None:
    # Uploads file from local disk to s3
    s3 = s3_client(region)
    s3.upload_file(Filename=local_path, Bucket=bucket, Key=key)

# -----------------------------
# MAIN FUNCTION
# -----------------------------
def main():
    session = build_requests_session()
    aws_region = os.getenv("AWS_DEFAULT_REGION")
    s3_bucket = os.getenv("S3_BUCKET")
    project_prefix = (os.getenv("S3_PROJECT_PREFIX") or "").strip("/")

    if not s3_bucket:
        raise ValueError("S3_BUCKET is not set")
    if not project_prefix:
        raise ValueError("S3_PROJECT_PREFIX is not set")

    bronze_prefix = f"{project_prefix}/bronze/ingestion_date={ingestion_date}/"
    delete_s3_prefix(bucket=s3_bucket, prefix=bronze_prefix, region=aws_region)

    failed_regions: list[str] = []
    total_rows = 0
    part_idx = 0

    with tempfile.TemporaryDirectory() as tempdir:  # Creates and auto cleaned temporary folder
        region_files = list_region_files()

        for region_file in region_files:
            sa2_codes = load_sa2_codes(region_file)
            url = build_abs_url(sa2_codes, START_PERIOD, END_PERIOD)

            try:
                response = session.get(url, headers=ABS_API_HEADERS, timeout=(10, 240))
            except (ChunkedEncodingError, ConnectionError, ReadTimeout, Timeout) as error:
                logger.error(f"Network error for {region_file}: {type(error).__name__} - {error}")
                failed_regions.append(region_file)
                continue

            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code} for {region_file}")
                failed_regions.append(region_file)
                continue

            # Writing reponse to dataframe and then to temporary directory as a parquet file
            df = pd.read_csv(io.StringIO(response.text))  # Parses CSV text returned by ABS into a dataframe
            df = standardise(df, region_file, ingestion_date, run_id)

            local_part_name = f"part-{part_idx:05d}.parquet"
            local_part_path = os.path.join(tempdir, local_part_name)
            df.to_parquet(local_part_path, index=False, engine="pyarrow")

            s3_key = f"{bronze_prefix}{local_part_name}"
            upload_to_s3(local_part_path, bucket=s3_bucket, key=s3_key, region=aws_region)

            rows = len(df)
            total_rows += rows
            part_idx += 1

            logger.info(f"Wrote {local_part_name} ({rows:,} rows) -> s3://{s3_bucket}/{s3_key}")
    

    # Error checking
    if total_rows == 0:
        raise RuntimeError(f"No rows written - pipeline produced empty output")
            
    if failed_regions:
        raise RuntimeError(f"Failed region(s): {', '.join(failed_regions)}")

if __name__ == "__main__":
    main()