# Copyright (c) 2026 Kenneth Stott
# Canary: 7b56e90f-292d-4a78-8763-2665f8fea449
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark connector's own code (REQ-2225).

The host runs one annotated Spark script given when the connector is set up: its @table and
@command functions are the source's. This package is what that script imports. :func:`session`
is a SparkSession against the cluster named at setup; the script never names the cluster itself.
"""

from __future__ import annotations

from typing import Any

import provisa_services

__all__ = ["declared", "session"]


def session() -> Any:  # a pyspark.sql.SparkSession
    """A Spark Connect session to the configured cluster, created once and shared by the script."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.remote(provisa_services.setup_value("cluster")).getOrCreate()


def declared(values: dict) -> None:
    """Names the values this connector needs for the setup form. The host runs the script's own
    lifecycle functions and never calls this one.

    @on-start
    @requires script
    @requires cluster
    """
