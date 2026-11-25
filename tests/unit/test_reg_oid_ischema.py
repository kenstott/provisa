# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3d8f21-b5e4-4c16-9e02-6d4a1b7c5f83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2190: PostgreSQL's reg* OID-alias types are registered in SQLAlchemy's
``ischema_names`` so that reflecting any relation with such a column (e.g. the
meta schema's pg_catalog-shaped views) raises no SAWarning and records the
column's type as the right name.

The test calls ``PGDialect._reflect_type`` directly — the same code path
``get_multi_columns`` takes when it encounters an unknown type name — so
no live database is needed.
"""

# Requirements: REQ-2190

from __future__ import annotations

import warnings

import pytest
import sqlalchemy.exc
from sqlalchemy.dialects.postgresql.base import PGDialect, ischema_names

# Importing pgvector triggers the ischema_names registrations.
import provisa.core.pgvector as _pgvec  # noqa: F401


# ---------------------------------------------------------------------------
# The complete list of reg* types the brief requires (regclass is SQLAlchemy's
# own and was already registered; it is included here as a coverage check).
# ---------------------------------------------------------------------------
_REG_TYPES = [
    "regproc",
    "regprocedure",
    "regoper",
    "regoperator",
    "regclass",
    "regtype",
    "regrole",
    "regnamespace",
    "regconfig",
    "regdictionary",
    "regcollation",
]


@pytest.mark.parametrize("type_name", _REG_TYPES)
def test_reg_oid_type_is_in_ischema_names(type_name: str) -> None:
    """Every reg* type is registered so SQLAlchemy's reflection finds it."""
    assert type_name in ischema_names, (
        f"{type_name!r} is missing from ischema_names; "
        "reflecting a relation with this column will emit SAWarning"
    )


def _reflect(type_name: str):
    """Call ``PGDialect._reflect_type`` with the right arity for this SQLAlchemy version."""
    import inspect

    dialect = PGDialect()
    sig = inspect.signature(dialect._reflect_type)
    kwargs = {
        "format_type": type_name,
        "domains": {},
        "enums": {},
        "type_description": f"column {type_name} of table pg_aggregate",
    }
    if "collation" in sig.parameters:
        kwargs["collation"] = None
    return dialect._reflect_type(**kwargs)


@pytest.mark.parametrize("type_name", _REG_TYPES)
def test_reflecting_a_reg_oid_column_raises_no_sa_warning(type_name: str) -> None:
    """``PGDialect._reflect_type`` for a reg* column emits no SAWarning (REQ-2190)."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", sqlalchemy.exc.SAWarning)
        # Does not raise — the type is now known.
        _reflect(type_name)


@pytest.mark.parametrize("type_name", _REG_TYPES)
def test_reflecting_a_reg_oid_column_gives_right_data_type(type_name: str) -> None:
    """``_column_records`` records the type as its lowercase pg name (REQ-2190).

    ``_column_records`` does ``str(c["type"]).split("(")[0].strip().lower()``,
    so ``str(type_instance)`` must start with the uppercase type name.
    """
    type_obj = _reflect(type_name)
    recorded = str(type_obj).split("(")[0].strip().lower()
    assert recorded == type_name, (
        f"_column_records would record {recorded!r} instead of {type_name!r}"
    )
