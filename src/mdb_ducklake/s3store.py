"""Move the DuckLake catalog file to/from S3.

A DuckLake catalog is a live DuckDB file and can't be attached read-write over
S3, so when the catalog lives in the bucket we download it before a run and
upload it after. boto3 reads the same AWS_* env credentials injected via
`sops exec-env`, and endpoint_url points it at the S3-compatible provider
(e.g. DigitalOcean Spaces).
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple


def is_s3_uri(value: str) -> bool:
    return value.startswith("s3://")


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """s3://bucket/some/key -> ("bucket", "some/key")."""
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"Not a valid s3://bucket/key URI: {uri!r}")
    return bucket, key


def _client(endpoint: str | None, region: str | None):
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "boto3 is required for an s3:// catalog. It ships as a dependency; "
            "run `uv sync`."
        ) from exc
    kwargs = {}
    if endpoint:
        kwargs["endpoint_url"] = f"https://{endpoint}"
    if region:
        kwargs["region_name"] = region
    # Access key/secret are read from the environment by boto3.
    return boto3.client("s3", **kwargs)


def download(uri: str, dest: str | Path, endpoint: str | None, region: str | None) -> bool:
    """Download s3 object to dest. Returns False if the object doesn't exist."""
    from botocore.exceptions import ClientError

    bucket, key = parse_s3_uri(uri)
    client = _client(endpoint, region)
    try:
        client.download_file(bucket, key, str(dest))
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NoSuchBucket"):
            return False
        raise


def upload(src: str | Path, uri: str, endpoint: str | None, region: str | None) -> None:
    bucket, key = parse_s3_uri(uri)
    _client(endpoint, region).upload_file(str(src), bucket, key)
