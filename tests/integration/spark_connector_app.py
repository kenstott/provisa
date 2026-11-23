# Copyright (c) 2026 Kenneth Stott
# Canary: 8e4d1b70-3c95-4fa2-b6d8-0a7f2e9c13d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ASGI entry point identical to ``main:app``, with the Spark connector's block and its image's
labels stood in for (REQ-2225, REQ-2270).

A connector is offered by a platform block a release writes, and held to the labels of an image in
a registry; neither is what tests/integration/test_spark_connector_e2e.py tests, so the server it
runs reads the block row from ``PROVISA_TEST_SPARK_CONNECTOR`` and the labels from
``PROVISA_TEST_SPARK_LABELS`` (JSON each), as the in-process tests of that file stand them in."""

# Requirements: REQ-2225, REQ-2270

from __future__ import annotations

import json
import os
from typing import Any

from provisa.codesource import remote_sources

_ROW = json.loads(os.environ["PROVISA_TEST_SPARK_CONNECTOR"])
_LABELS = json.loads(os.environ["PROVISA_TEST_SPARK_LABELS"])


async def _connector(_name: str) -> dict:
    return _ROW


async def _image_labels(_state: Any, _org_id: str, _name: str, _row: dict) -> dict:
    return _LABELS


remote_sources._connector = _connector  # pyright: ignore[reportPrivateUsage]  # the stand-in
remote_sources._image_labels = _image_labels  # pyright: ignore[reportPrivateUsage]  # the stand-in

from main import app  # noqa: E402  # after the stand-ins: the app is the one the server runs

__all__ = ["app"]
