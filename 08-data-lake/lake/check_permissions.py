"""Prove the access policies: the analyst can read the clean zone and write results, and nothing else.
Run against SeaweedFS (docker compose): moto doesn't enforce permissions."""

from __future__ import annotations

from botocore.exceptions import ClientError

from .query import analyst_credentials
from .s3 import CLEAN_BUCKET, RAW_BUCKET, RESULTS_BUCKET, client


def attempt(label: str, action) -> bool:
    try:
        action()
        print(f"  ALLOWED  {label}")
        return True
    except ClientError as e:
        print(f"  DENIED   {label} ({e.response['Error']['Code']})")
        return False


def main() -> None:
    analyst = client(*analyst_credentials())
    expected = {
        "analyst reads the catalog": (lambda: analyst.get_object(Bucket=CLEAN_BUCKET, Key="_catalog/catalog.json"), True),
        "analyst writes a query result": (lambda: analyst.put_object(Bucket=RESULTS_BUCKET, Key="queries/probe.txt", Body=b"x"), True),
        "analyst lists the raw zone": (lambda: analyst.list_objects_v2(Bucket=RAW_BUCKET), False),
        "analyst writes into the clean zone": (lambda: analyst.put_object(Bucket=CLEAN_BUCKET, Key="probe.txt", Body=b"x"), False),
        "analyst deletes a clean file": (lambda: analyst.delete_object(Bucket=CLEAN_BUCKET, Key="_catalog/catalog.json"), False),
    }
    wrong = [label for label, (action, allowed) in expected.items() if attempt(label, action) != allowed]
    if wrong:
        raise SystemExit(f"policy violations: {wrong}")
    print("policies behave as designed")


if __name__ == "__main__":
    main()
