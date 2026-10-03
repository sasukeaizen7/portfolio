import pytest
from moto.server import ThreadedMotoServer


@pytest.fixture(scope="session")
def s3_server():
    server = ThreadedMotoServer(port=5011)
    server.start()
    yield "http://127.0.0.1:5011"
    server.stop()


@pytest.fixture
def lake(s3_server, monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT", s3_server)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    from lake.s3 import CLEAN_BUCKET, RAW_BUCKET, RESULTS_BUCKET, client

    s3 = client()
    for bucket in (RAW_BUCKET, CLEAN_BUCKET, RESULTS_BUCKET):          # fresh buckets for every test
        try:
            for o in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=o["Key"])
        except s3.exceptions.NoSuchBucket:
            s3.create_bucket(Bucket=bucket)
    return s3
