# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7e4a91-5f0c-4b36-8e2a-c9b1f6d30a75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The server's open-file limit, raised at start (#338).

A process starts with the soft ``RLIMIT_NOFILE`` its parent gave it: 256 on macOS by default. The
embedded DuckDB engine opens many files, a read of a Parquet folder one per file, and a server
holds sockets and its own files besides. As a database server does, the server raises its soft
limit to the hard limit once, at start, and logs both values. The hard limit is the operator's (or
the platform's) to set. A POSIX-only step: the ``resource`` module does not exist elsewhere, and
the app runs this only where it does (``provisa.api.app.lifespan``)."""

from __future__ import annotations

import logging
import resource


def _named(limit: int) -> str:
    return "unlimited" if limit == resource.RLIM_INFINITY else str(limit)


def raise_open_file_limit(log: logging.Logger) -> None:
    """Raise this process's soft open-file limit to its hard limit, logging the old and new."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    log.info("Open-file limit: soft %s -> %s (hard %s)", _named(soft), _named(hard), _named(hard))
