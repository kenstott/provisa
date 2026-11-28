# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b1c9e-7a24-4d58-b0e3-5c8a92d41e07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reflecting PostgreSQL's system tables raises no SAWarning: every system type their columns
carry (xid, aclitem, int2vector, oidvector, ...) is registered with the dialect's type map."""

# Requirements: REQ-2190

from __future__ import annotations

import warnings

import sqlalchemy as sa
from sqlalchemy.engine.reflection import ObjectKind

import provisa.core.database  # noqa: F401  (registers the dialect types, as the product does)
import provisa.core.pgvector  # noqa: F401


def test_reflecting_pg_catalog_raises_no_sa_warning(embedded_postgres) -> None:
    engine = sa.create_engine(embedded_postgres.url())
    try:
        insp = sa.inspect(engine)
        with warnings.catch_warnings():
            warnings.simplefilter("error", sa.exc.SAWarning)
            columns = insp.get_multi_columns(schema="pg_catalog", kind=ObjectKind.ANY)
    finally:
        engine.dispose()
    named = {c["name"]: str(c["type"]).lower() for cols in columns.values() for c in cols}
    attacl = next(
        c["type"] for c in columns[("pg_catalog", "pg_attribute")] if c["name"] == "attacl"
    )
    assert str(attacl.item_type).lower() == "aclitem"
    assert named["relfrozenxid"] == "xid"
    assert named["indkey"] == "int2vector"
    assert named["indclass"] == "oidvector"
