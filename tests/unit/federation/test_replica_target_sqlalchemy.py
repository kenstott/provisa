# Copyright (c) 2026 Kenneth Stott
# Canary: 32033f4e-5783-4c7e-87b0-080a9146430d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a SQLAlchemy store declares an atomic swap only on a dialect where one is proven;
on any other, no method builds a replica in it."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from provisa.federation.data_replicator import (
    EngineCaps,
    NoReplicationMethod,
    SourceCaps,
    SourceRead,
    choose_method,
)
from provisa.federation.replica_target import SqlAlchemyStoreTarget


def _target(url: str) -> SqlAlchemyStoreTarget:
    # create_engine opens no connection.
    return SqlAlchemyStoreTarget(
        create_engine(url),
        schema="org_a_replicas",
        table="s__public__t",
        columns=[("id", "integer")],
        pk_columns=["id"],
    )


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg2://u:p@localhost:1/d",
        "mysql+pymysql://u:p@localhost:1/d",
        "mssql+pyodbc://u:p@localhost:1/d?driver=ODBC+Driver+18+for+SQL+Server",
    ],
)
def test_a_dialect_with_a_proven_swap_declares_it(url):
    assert _target(url).caps.atomic_swap


def test_a_dialect_with_no_proven_swap_declares_none_and_is_refused():
    target = _target("oracle+oracledb://u:p@localhost:1/?service_name=x")
    assert not target.caps.atomic_swap
    with pytest.raises(NoReplicationMethod, match="cannot swap a finished table in atomically"):
        choose_method(
            SourceCaps(frozenset({SourceRead.CURSOR})),
            target.caps,
            EngineCaps(reaches_source=False, runs=frozenset()),
        )
