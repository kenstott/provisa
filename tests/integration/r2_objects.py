# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1c4a7e-2f95-4b36-a0e8-5c7b9d3f1e62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The R2 (S3-compatible) objects an external-link test seeds, under a prefix of the run's own.

The bucket is a shared, durable fixture (REQ-254 leaves it); what a run writes into it goes under
``<stem>_<run>/`` and is removed by that prefix -- every object under it, read back empty -- so a
run never deletes another run's objects and leaves none of its own.
"""

from __future__ import annotations

import os
from typing import Any


def bucket() -> str:
    return os.environ.get("PROVISA_R2_TEST_BUCKET", "pubs")


def client() -> Any:
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_ENDPOINT_OVERRIDE"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def _keys(s3: Any, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket(), Prefix=prefix):
        keys.extend(o["Key"] for o in page.get("Contents", []))
    return keys


def remove_prefix(prefix: str) -> None:
    """Delete every object under the run's ``prefix`` (which ends in ``/``), then list it again
    and raise if anything is still there."""
    if not prefix.endswith("/"):
        raise ValueError(f"an R2 run prefix ends in '/': {prefix!r}")
    s3 = client()
    for key in _keys(s3, prefix):
        s3.delete_object(Bucket=bucket(), Key=key)
    left = _keys(s3, prefix)
    if left:
        raise RuntimeError(f"R2 objects left under {prefix!r} after their delete: {left}")
