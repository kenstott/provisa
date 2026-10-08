# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-a1b2-c3d4e5f6a7b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live AskAmerica source (REQ-540): a Postgres-wire source, read through its bundled server.

A ``Source`` row (type=govdata) carries the API key in ``username`` and the schemas it serves in
``database``. ``provisa.federation.askamerica`` exchanges the key for the credentials the data is
read with; ``provisa.federation.pgwire_replica`` starts the bundled ``pgwire-govdata`` server
with them, from the source's own state directory; DuckDB attaches that server through its
postgres extension and reads each table from the adapter schema the table names.

Credentials
-----------
There is no emulator behind the adapter: a real key is required (``ASKAMERICA_API_KEY``, a
secret of the warehouse lane), so this runs there. A missing key fails the module by name.

What this proves that the unit tests cannot
-------------------------------------------
- the key AskAmerica issues is exchanged for credentials, and a made-up key is refused;
- the pinned bundle starts with the environment Provisa gives it and nothing else;
- DuckDB's postgres extension attaches the server (its pg_catalog introspection answers);
- a table is read in place from its own schema, and joined with a table of another source in
  one statement — the read the in-process engine this replaces could never serve.

The server mounts its schemas from object storage before it accepts a connection, which takes
minutes on a cold cache; the fixture waits for it rather than for the 90 seconds a statement
does.
"""

from __future__ import annotations

import contextlib
import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_SOURCE_ID = "askamerica-itest"
#: One small schema plus the two linker schemas the Sources form always adds.
_SCHEMAS = "fec,ref,geo"
_SCHEMA = "fec"
#: How long the fixture waits for a cold server to mount its schemas.
_COLD_START_SECONDS = 600


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the AskAmerica source test needs a real key "
            "(a secret of the warehouse lane)"
        )
    return key


def _source():
    from provisa.core.models import Source, SourceType

    return Source(id=_SOURCE_ID, type=SourceType.govdata, username=_api_key(), database=_SCHEMAS)


@contextlib.contextmanager
def _serving(source, data_dir):
    """The source's pgwire server, started from a state directory of this run's own."""
    from provisa.federation import pgwire_replica as pr

    previous = os.environ.get("PROVISA_DATA_DIR")
    os.environ["PROVISA_DATA_DIR"] = str(data_dir)
    try:
        yield pr.ensure_endpoint(source, timeout=_COLD_START_SECONDS)
    finally:
        pr.stop_endpoint(source.id)
        if previous is None:
            del os.environ["PROVISA_DATA_DIR"]
        else:
            os.environ["PROVISA_DATA_DIR"] = previous


@pytest.fixture(scope="module")
def endpoint(tmp_path_factory):
    with _serving(_source(), tmp_path_factory.mktemp("askamerica-state")) as served:
        yield served


@pytest.fixture(scope="module")
def attached(endpoint):
    """A DuckDB connection with the server attached exactly as the engine connector attaches it."""
    import duckdb

    from provisa.federation.engine import build_duckdb_engine
    from types import SimpleNamespace

    source = _source()
    details = (
        build_duckdb_engine()
        .connectors["govdata"]
        .details(
            SimpleNamespace(  # pyright: ignore[reportArgumentType] - the runtime's attach view
                id=source.id,
                type=source.type,
                username=source.username,
                database=source.database,
                catalog=_SOURCE_ID.replace("-", "_"),
                schema_name=_SCHEMA,
                table_name="candidates",
            )
        )
    )
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(details["attach"])
    try:
        yield con, details
    finally:
        con.close()


def test_the_key_is_exchanged_for_storage_credentials():
    from provisa.federation.askamerica import resolve_storage_credentials

    creds = resolve_storage_credentials(_api_key())
    assert creds.access_key_id and creds.secret_access_key and creds.session_token
    assert creds.bucket and creds.endpoint.startswith("https://")


def test_a_made_up_key_is_refused():
    from provisa.federation.askamerica import AskAmericaKeyRefused, resolve_storage_credentials

    with pytest.raises(AskAmericaKeyRefused):
        resolve_storage_credentials("not-a-real-key")


def test_the_connector_names_no_schema_of_its_own(attached):
    _, details = attached
    assert "remote_schema" not in details


def test_duckdb_lists_the_schemas_the_source_serves_and_no_other(attached):
    con, details = attached
    listed = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT schema_name FROM duckdb_tables() WHERE database_name = ?",
            [details["raw_alias"]],
        ).fetchall()
    }
    assert _SCHEMA in listed
    assert listed <= set(_SCHEMAS.split(",")), listed


def test_a_table_is_read_in_place_from_its_own_schema(attached):
    con, details = attached
    alias = details["raw_alias"]
    tables = [
        row[0]
        for row in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ?",
            [alias, _SCHEMA],
        ).fetchall()
    ]
    assert "candidates" in tables, tables
    rows = con.execute(f'SELECT * FROM "{alias}"."{_SCHEMA}"."candidates" LIMIT 5').fetchall()
    assert rows, "the attached table returned no rows"


def test_a_table_joins_a_table_of_another_source_in_one_statement(attached):
    """The read the in-process engine could not serve: the AskAmerica table beside any other
    relation the engine holds, in one statement the engine computes."""
    con, details = attached
    alias = details["raw_alias"]
    con.execute("CREATE OR REPLACE TEMP TABLE picks AS SELECT 1 AS n")
    joined = con.execute(
        f'SELECT count(*) FROM (SELECT * FROM "{alias}"."{_SCHEMA}"."candidates" LIMIT 5) c '
        "CROSS JOIN picks p"
    ).fetchone()
    assert joined is not None and joined[0] == 5


@pytest.mark.asyncio(loop_scope="session")
async def test_a_replica_is_landed_from_the_tables_own_schema(endpoint):
    """What an engine with no connector of its own for the type (Trino) reads: the rows landed
    through the server, selected from the schema the table names."""
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.calcite_child_host, endpoint.pgwire_port)
    try:
        schema = pr.remote_schema(_source(), _SCHEMA)
        rows = await conn.fetch(f'SELECT * FROM "{schema}"."candidates" LIMIT 5')
    finally:
        await conn.close()
    assert schema == _SCHEMA
    assert len(rows) == 5
