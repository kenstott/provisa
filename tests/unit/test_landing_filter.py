# Copyright (c) 2026 Kenneth Stott
# Canary: e7c0ddcb-afd7-493e-b620-657595de0370
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2062: a table's landing filter -- a SQL predicate over its own columns -- is applied when
its replica lands, by the SQL read that builds the replica, in that read's dialect."""

# Requirements: REQ-2062

from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest

import provisa.api.app  # noqa: F401 -- the API layer says whether the deployment holds many orgs, which reading a source's secrets asks
from provisa.core.landing_filter import check, where
from provisa.core.models import Column, Table

_COLS = ["cve_id", "cvss", "published"]


def _table(landing_filter: str | None) -> Table:
    return Table(
        source_id="govdata",
        domain_id="d",
        schema="cyber_vuln",
        table="vulnerabilities",
        landing_filter=landing_filter,
        columns=[Column(name=c, data_type="text", visible_to=["admin"]) for c in _COLS],
    )


def test_a_predicate_over_the_tables_own_columns_is_taken():
    check("t", "cve_id IN ('CVE-2021-44228', 'CVE-2022-22965') AND cvss >= 7", _COLS)
    assert _table("cve_id LIKE 'CVE-2021-%'").landing_filter == "cve_id LIKE 'CVE-2021-%'"


@pytest.mark.parametrize(
    ("predicate", "refused"),
    [
        ("cve_id IN (SELECT cve_id FROM kev)", "no subquery and name no table"),
        ("vendor = 'apache'", "names vendor, not columns of the table"),
        ("1 = 1", "names none of the table's columns"),
        ("cve_id IN (", "not a SQL predicate"),
    ],
)
def test_anything_else_is_refused_by_name(predicate, refused):
    with pytest.raises(ValueError, match=refused):
        _table(predicate)


def test_it_is_read_in_the_dialect_of_the_read_that_builds_the_replica():
    table = _table("cve_id ILIKE 'cve-2021-%'")
    assert where(table, lambda: "postgres") == " WHERE \"cve_id\" ILIKE 'cve-2021-%'"
    assert where(table, lambda: "duckdb") == " WHERE \"cve_id\" ILIKE 'cve-2021-%'"
    assert where(table, lambda: "trino") == " WHERE LOWER(\"cve_id\") LIKE LOWER('cve-2021-%')"
    assert where(_table(None), lambda: "duckdb") == ""


def test_a_column_named_as_a_keyword_is_quoted():
    """The govdata server reads the filter as Calcite SQL, where YEAR, MONTH and DATE are
    keywords: ``WHERE year = 2023`` failed to parse (Encountered "year =")."""
    table = Table(
        source_id="govdata",
        domain_id="d",
        schema="health",
        table="cdc_county_overdose_deaths",
        landing_filter="st_abbrev = 'OH' AND year BETWEEN 2010 AND 2017 AND "
        "SUBSTRING(date FROM 6 FOR 2) IN ('05', '06') AND month = '12'",
        columns=[
            Column(name=c, data_type="text", visible_to=["admin"])
            for c in ("st_abbrev", "year", "date", "month")
        ],
    )
    assert where(table, lambda: "postgres") == (
        ' WHERE "st_abbrev" = \'OH\' AND "year" BETWEEN 2010 AND 2017 AND '
        "SUBSTRING(\"date\" FROM 6 FOR 2) IN ('05', '06') AND \"month\" = '12'"
    )


async def test_the_engine_reads_only_the_rows_that_pass_it():
    from provisa.federation.replica_source import EngineTableSource

    asked: list[str] = []

    class _Stream(list):
        def close(self) -> None:
            pass

    def execute_engine_stream(sql, authorization):
        asked.append(sql)
        assert authorization.expected_sql == sql
        return None, _Stream([pa.RecordBatch.from_pydict({"cve_id": ["CVE-2021-44228"]})])

    engine = SimpleNamespace(execute_engine_stream=execute_engine_stream)
    source = EngineTableSource(
        engine, '"govdata"."cyber_vuln"."vulnerabilities"', in_place=True,
        where=where(_table("cve_id = 'CVE-2021-44228'"), lambda: "duckdb"),
    )  # fmt: skip
    batches = [b async for b in source.batches(100)]
    assert asked == [
        'SELECT * FROM "govdata"."cyber_vuln"."vulnerabilities" WHERE "cve_id" = \'CVE-2021-44228\''
    ]
    assert batches[0].num_rows == 1


def test_postgres_copies_only_the_rows_that_pass_it(monkeypatch):
    from provisa.federation import replica_guard
    from provisa.federation.replica_target import pg_statement_copy

    # The store's own check that the target is an ordinary table; a fake cursor has no catalog.
    monkeypatch.setattr(replica_guard, "require_pg_replica_table", lambda *a, **k: None)
    monkeypatch.setattr(replica_guard, "require_replicas_schema", lambda *a, **k: None)

    statements: list[str] = []

    class _Cursor:
        def execute(self, sql, *_args):
            statements.append(sql)

        rowcount = 1

        def fetchone(self):
            return (1, 0, 0)

    pg_statement_copy(
        _Cursor(),
        source_relation='"govdata_fdw"."vulnerabilities"',
        schema="org_x_replicas",
        table="govdata__cyber_vuln__vulnerabilities",
        columns=[(c, "text") for c in _COLS],
        prior_hash=None,
        where=where(_table("cvss >= 9"), lambda: "postgres"),
    )
    (insert,) = [s for s in statements if s.startswith("INSERT INTO")]
    assert insert.endswith('FROM "govdata_fdw"."vulnerabilities" WHERE "cvss" >= 9')


async def test_a_table_read_by_its_adapter_refuses_a_landing_filter():
    from provisa.events.source_loader import SourceRowLoader, UnsupportedSourceFetch

    async def adapter(source, table):
        return []

    loader = SourceRowLoader(engine=None, adapter_loaders={"openapi": adapter})
    table = SimpleNamespace(
        table_name="alerts", schema_name="openapi", landing_filter="id > 1", columns=[]
    )
    with pytest.raises(UnsupportedSourceFetch, match="applied by a SQL read of its source"):
        loader.replica_source(
            SimpleNamespace(source_pools=None), SimpleNamespace(id="gh", type="openapi"), table, []
        )


def test_a_changed_filter_asks_for_a_rebuild_and_none_keeps_the_digest():
    from provisa.federation.replica_converge import definition_hash

    src = SimpleNamespace(type="govdata", host=None, port=None, database=None, path=None)
    address = SimpleNamespace(schema="s", table="t")
    columns, key = [("cve_id", "text")], ["cve_id"]
    plain = definition_hash(src, address, columns, key, landing_filter=None)
    a = definition_hash(src, address, columns, key, landing_filter="cvss >= 7")
    b = definition_hash(src, address, columns, key, landing_filter="cvss >= 9")
    assert len({plain, a, b}) == 3
    import hashlib
    import json

    identity = {"type": "govdata", "host": None, "port": None, "database": None, "path": None}
    before = {
        "address": ["s", "t"],
        "columns": [["cve_id", "text"]],
        "key": key,
        "source": identity,
    }
    assert plain == hashlib.sha256(json.dumps(before, sort_keys=True).encode()).hexdigest()


async def test_the_filter_is_stored_with_the_table_and_read_back_by_the_build(control_plane):
    from sqlalchemy import insert

    from provisa.api.admin.db_queries import fetch_tables
    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import domains, sources
    from provisa.federation.registry_view import _build_registered_tables

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="d"))
        await conn.execute_core(insert(sources).values(id="govdata", type="govdata"))
        await table_repo.upsert(conn, _table("cvss >= 9"))
        rows = await fetch_tables(conn)
    assert rows[0]["landing_filter"] == "cvss >= 9"
    (registered,) = _build_registered_tables(rows)
    assert registered.landing_filter == "cvss >= 9"


async def test_the_table_form_sets_both_and_an_edit_saves_what_it_sends(control_plane):
    """The admin table form carries the landing filter and the fixed arguments, so an edit
    through it saves the values it sends rather than keeping the stored ones (REQ-1919)."""
    from sqlalchemy import insert

    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import domains, sources

    assert "landing_filter" not in table_repo.KEPT_ON_FORM_EDIT
    assert "arguments" not in table_repo.KEPT_ON_FORM_EDIT
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="d"))
        await conn.execute_core(insert(sources).values(id="govdata", type="govdata"))
        await table_repo.upsert(conn, _table("cvss >= 7"))
        edited = await table_repo.keep_unedited(conn, _table("cvss >= 9"))
    assert edited.landing_filter == "cvss >= 9"


async def test_the_admin_refuses_fixed_arguments_on_a_source_that_takes_none(control_plane):
    from sqlalchemy import insert

    from provisa.api.admin._import_set import table_arguments_refusal
    from provisa.core.schema_org import sources

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="gh", type="openapi"))
        on_pg = SimpleNamespace(source_id="pg", table_name="t", arguments={"x": 1})
        on_api = SimpleNamespace(source_id="gh", table_name="t", arguments={"org": "acme"})
        none = SimpleNamespace(source_id="pg", table_name="t", arguments={})
        refused = await table_arguments_refusal(conn, on_pg)
        assert await table_arguments_refusal(conn, on_api) is None
        assert await table_arguments_refusal(conn, none) is None
    assert refused is not None and refused.code == "schema.arguments_not_an_api"
