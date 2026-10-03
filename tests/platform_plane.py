# Copyright (c) 2026 Kenneth Stott
# Canary: 92aee53f-953e-40ed-8745-f78e3061cb97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A real platform plane for unit tests, on a throwaway SQLite file.

The simple provider keeps each user's id there (provisa/auth/simple_user_ids.py), so a test that
signs a simple user in needs one. The registry tables are created from the product's own
metadata, the way init_registry_schema creates them.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url


def platform_db(directory: str | Path | None = None) -> Database:
    """A fresh platform-plane Database. ``directory`` defaults to a new temporary directory."""
    root = Path(directory) if directory is not None else Path(tempfile.mkdtemp(prefix="platform-"))
    engine = create_engine_from_url(f"sqlite+pysqlite:///{root / 'platform.db'}")
    schema_admin.metadata.create_all(engine, tables=schema_admin.REGISTRY_TABLES)
    return Database(engine, "test")


def simple_user_ids(directory: str | Path | None = None):
    """The simple provider's id store over a fresh platform plane."""
    from provisa.auth.simple_user_ids import SimpleUserIds

    return SimpleUserIds(platform_db(directory))
