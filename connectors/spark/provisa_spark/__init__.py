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

A Spark source is a connection to a cluster through Spark Connect; it has no script (REQ-2225,
amended 2026-11-25). The host serves no tables of its own: what the cluster holds is served from
its catalog (REQ-2272). Logic the cluster does not hold is an ordinary Python code source, which
opens this source's session with ``provisa_services.spark.session(<source>)``.
"""

from __future__ import annotations

__all__ = ["declared"]


def declared(values: dict) -> None:
    """Names the values this connector needs for the setup form.

    @on-start
    @requires cluster
    """
