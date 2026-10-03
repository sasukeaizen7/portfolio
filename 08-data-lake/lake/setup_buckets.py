"""Create the lake buckets with the admin identity (the one-off "infrastructure" step; project 10
does the same with Terraform). Waits for the S3 endpoint to come up first."""

from __future__ import annotations

import os
import time

from botocore.exceptions import ClientError, EndpointConnectionError

from .s3 import CLEAN_BUCKET, RAW_BUCKET, RESULTS_BUCKET, client


def main() -> None:
    s3 = client(os.environ["ADMIN_ACCESS_KEY"], os.environ["ADMIN_SECRET_KEY"])
    for _attempt in range(60):
        try:
            existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
            break
        except (EndpointConnectionError, ClientError):
            time.sleep(2)
    else:
        raise SystemExit("S3 endpoint not reachable")
    for bucket in (RAW_BUCKET, CLEAN_BUCKET, RESULTS_BUCKET):
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
        print(f"  bucket {bucket} ready")


if __name__ == "__main__":
    main()
