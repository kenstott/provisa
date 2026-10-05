# Copyright (c) 2026 Kenneth Stott
# Canary: 74525ec0-f1ae-4d1c-af8e-9f4206057ca5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: what a SQLAlchemy store declares per dialect and driver: how a batch is loaded, how
a finished build replaces the replica, and which stores have no atomic replace at all."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from provisa.federation.data_replicator import (
    EngineCaps,
    NoReplicationMethod,
    SourceCaps,
    SourceRead,
    TargetCaps,
    TargetLoad,
    TargetWrite,
    choose_method,
)
from provisa.federation.replica_target import (
    LOAD_INSERT,
    LOAD_ODBC_ARRAY,
    LOAD_ORACLE_DIRECT_PATH,
    LOAD_SINGLESTORE_INFILE,
    RENAME_IN_TRANSACTION,
    RENAME_PAIR,
    ROWS_IN_TRANSACTION,
    SA_NO_ATOMIC_REPLACE,
    PostgresStoreTarget,
    SqlAlchemyStoreTarget,
    sa_replace_method,
    sqlalchemy_store_target,
)

_ARGS = {
    "schema": "org_a_replicas",
    "table": "s__public__t",
    "columns": [("id", "integer")],
    "pk_columns": ["id"],
}


def _target(url: str, **overrides) -> SqlAlchemyStoreTarget:
    # create_engine opens no connection.
    return SqlAlchemyStoreTarget(create_engine(url), **_ARGS, **overrides)


@pytest.mark.parametrize(
    ("url", "load", "replace"),
    [
        (
            "mssql+pyodbc://u:p@localhost:1/d?driver=ODBC+Driver+18+for+SQL+Server",
            LOAD_ODBC_ARRAY,
            RENAME_IN_TRANSACTION,
        ),
        ("mysql+pymysql://u:p@localhost:1/d", LOAD_INSERT, RENAME_PAIR),
        (
            "oracle+oracledb://u:p@localhost:1/?service_name=x",
            LOAD_ORACLE_DIRECT_PATH,
            ROWS_IN_TRANSACTION,
        ),
        # REQ-990: a SingleStore store streams LOAD DATA LOCAL INFILE, never executemany.
        ("singlestoredb://u:p@localhost:1/d", LOAD_SINGLESTORE_INFILE, ROWS_IN_TRANSACTION),
    ],
)
def test_each_dialect_declares_its_load_and_its_atomic_replace(url, load, replace):
    target = _target(url)
    assert (target.load_method, target.replace_method) == (load, replace)
    assert target.caps.atomic_swap
    # The named capability the store page and the docs state: only a bulk path is a bulk stream.
    assert target.caps.load is (
        TargetLoad.BULK_STREAM
        if load in (LOAD_ORACLE_DIRECT_PATH, LOAD_SINGLESTORE_INFILE)
        else TargetLoad.ROW_COPY
    )


def test_the_overrides_force_the_floor_load_and_the_row_replace():
    target = _target(
        "mssql+pyodbc://u:p@localhost:1/d?driver=ODBC+Driver+18+for+SQL+Server",
        load=LOAD_INSERT,
        rename=False,
    )
    assert (target.load_method, target.replace_method) == (LOAD_INSERT, ROWS_IN_TRANSACTION)


def test_a_postgresql_store_has_one_write_face_whichever_driver_the_url_names():
    for url in ("postgresql+psycopg2://u:p@h:1/d", "postgresql+psycopg://u:p@h:1/d"):
        assert isinstance(sqlalchemy_store_target(create_engine(url), **_ARGS), PostgresStoreTarget)


def test_any_transactional_dialect_replaces_rows_in_one_transaction():
    for dialect in ("oracle", "db2", "firebird", "hana", "cockroachdb"):
        assert sa_replace_method(dialect) == ROWS_IN_TRANSACTION


@pytest.mark.parametrize("dialect", sorted(SA_NO_ATOMIC_REPLACE))
def test_a_store_with_no_atomic_replace_is_refused_by_name(dialect):
    assert sa_replace_method(dialect) is None
    with pytest.raises(NoReplicationMethod, match="cannot swap a finished table in atomically"):
        choose_method(
            SourceCaps(frozenset({SourceRead.CURSOR})),
            TargetCaps(frozenset({TargetWrite.BULK_BATCH}), False, TargetLoad.ROW_COPY),
            EngineCaps(reaches_source=False, runs=frozenset()),
        )
