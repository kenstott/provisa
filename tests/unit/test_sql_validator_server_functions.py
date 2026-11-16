# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4e2b70-1f5d-4a96-b3c8-6e0d9a7f2145
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed statement calls no function that reads or acts on its server (REQ-001).

What a statement returns is governed by the tables it names. ``ts_stat('SELECT ...')``,
``pg_read_file``, ``lo_import``, ``dblink``, ``set_config`` and their kin do what no table rule
sees, as the login of the source or the engine; ``provisa_*`` are the product's own. The validator
refuses each by name wherever the statement calls it, however the name is written.
"""

# Requirements: REQ-001

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.pgwire.catalog_pg17_acl import REVOKED_FUNCTIONS
from provisa.transpiler import metadata_functions, server_functions
from provisa.transpiler.server_functions import (
    ACTING_PREFIXES,
    ACTS_ON_THE_SERVER,
    READS_THE_SERVER,
    RUNS_SQL_AS_TEXT,
    WRITTEN_BEFORE_GOVERNANCE,
    refused_in_a_governed_statement,
)
from provisa.transpiler.xml_syntax import MAPPINGS

_ROLE = {"id": "teller", "capabilities": [], "domain_access": ["*"]}
_CUSTOMER = [("id", "integer"), ("name", "varchar"), ("score", "double"), ("doc", "jsonb")]

#: One function of each refused family, beside every listed name.
_OF_A_PREFIX = (
    "dblink_exec",
    "dblink_connect",
    "pg_stat_get_activity",
    "pg_stat_reset_shared",
    "pg_replication_origin_create",
    "binary_upgrade_set_next_pg_type_oid",
)
_SERVER = sorted({*READS_THE_SERVER, *ACTS_ON_THE_SERVER, *_OF_A_PREFIX})
_OWN = (
    "provisa_fake_method",
    "provisa_digest",
    "provisa_seed",
    "provisa_digest_tag",
    "provisa_stable_fake",
    "provisa_xml_value",
    "provisa_type",
    "provisa_anything_else",
)


def _gov() -> GovernanceContext:
    return GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={1: frozenset({"id", "name", "score", "doc"})},
        table_map={"bank.customer": 1},
        all_columns={1: list(_CUSTOMER)},
        limit_ceiling=None,
        sample_size=None,
    )


def _violations(sql: str) -> list[str]:
    return [
        f"{v.code} {v.message}"
        for v in validate_sql(
            sql, CompilationContext(), _gov(), _ROLE, [], bypass_relationship_guard=True
        )
    ]


def _spellings(name: str) -> dict[str, str]:
    """The ways a statement writes a call's name."""
    mixed = "".join(c.upper() if i % 2 else c for i, c in enumerate(name))
    return {
        "plain": name,
        "qualified": f"pg_catalog.{name}",
        "quoted": f'"{name}"',
        "qualified_quoted": f'pg_catalog."{name}"',
        "upper": name.upper(),
        "mixed": mixed,
    }


#: Where a statement calls a function, ``{call}`` being the call's name.
_PLACES = {
    "select_list": "SELECT {call}('a') AS v FROM bank.customer c",
    "where": "SELECT c.id FROM bank.customer c WHERE {call}('a') IS NOT NULL",
    "subquery": "SELECT c.id FROM bank.customer c WHERE c.id IN (SELECT {call}('a'))",
    "derived": "SELECT t.v FROM (SELECT {call}('a') AS v FROM bank.customer c) t",
    "cte": "WITH x AS (SELECT {call}('a') AS v) SELECT c.id FROM bank.customer c, x",
    "lateral": "SELECT c.id FROM bank.customer c, LATERAL {call}(c.name) AS f",
    "from": "SELECT * FROM {call}('a') AS f",
    "no_table": "SELECT {call}('a') AS v",
    "argument": "SELECT lower({call}('a')) AS v FROM bank.customer c",
    "order_by": "SELECT c.id FROM bank.customer c ORDER BY {call}('a')",
    "join_on": "SELECT c.id FROM bank.customer c JOIN bank.customer d ON {call}('a') IS NULL",
}


#: The places that name no table: the statement is the engine's.
_NO_TABLE = frozenset({"from", "no_table"})
#: Refused in a statement naming a table alone (server_functions.RUNS_SQL_AS_TEXT says why).
_WHERE_A_TABLE_IS = READS_THE_SERVER - RUNS_SQL_AS_TEXT


def _refused(name: str, said: list[str]) -> bool:
    if name in MAPPINGS:  # refused by the rule of the XML mappings, which says the same
        return any(v.startswith("V006") and name in v for v in said)
    return any(v.startswith("V007") and f"'{name}'" in v for v in said)


@pytest.mark.parametrize("place", sorted(_PLACES))
@pytest.mark.parametrize("name", _SERVER)
def test_a_function_that_reads_or_acts_on_the_server_is_refused_wherever_it_is_called(name, place):
    said = _violations(_PLACES[place].format(call=name))
    if place in _NO_TABLE and name in _WHERE_A_TABLE_IS:
        assert not [v for v in said if v.startswith("V007")], said  # the engine's, as before
    else:
        assert _refused(name, said), said


@pytest.mark.parametrize("name", sorted(RUNS_SQL_AS_TEXT))
def test_a_function_that_runs_sql_given_as_text_is_refused_with_no_table(name):
    for call in _spellings(name).values():
        said = _violations(f"SELECT * FROM {call}('SELECT secret FROM other.table')")
        assert _refused(name, said), said
        said = _violations(f"SELECT {call}('SELECT secret FROM other.table') AS v")
        assert _refused(name, said), said


@pytest.mark.parametrize(
    "sql",
    [
        "WITH c AS (SELECT 1 AS id) SELECT lastval() FROM c",
        "SELECT pg_my_temp_schema() FROM generate_series(1, 2) AS g",
        "SELECT nextval('s') FROM (SELECT 1 AS a) t",
    ],
)
def test_a_cte_a_row_generator_and_a_derived_table_name_no_table(sql):
    assert not [v for v in _violations(sql) if v.startswith("V007")], _violations(sql)


def test_a_cte_named_as_a_table_does_not_hide_a_table():
    said = _violations("WITH x AS (SELECT c.id FROM bank.customer c) SELECT pg_sleep(1) FROM x")
    assert _refused("pg_sleep", said), said


@pytest.mark.parametrize("spelling", ["qualified", "quoted", "qualified_quoted", "upper", "mixed"])
@pytest.mark.parametrize("name", _SERVER)
def test_a_function_that_reads_or_acts_on_the_server_is_refused_however_it_is_written(
    name, spelling
):
    call = _spellings(name)[spelling]
    for place in ("select_list", "where", "lateral"):
        said = _violations(_PLACES[place].format(call=call))
        assert _refused(name, said), (place, said)


@pytest.mark.parametrize("place", sorted(_PLACES))
@pytest.mark.parametrize("name", _OWN)
def test_a_function_of_the_products_own_is_refused_in_a_statement(name, place):
    said = _violations(_PLACES[place].format(call=name))
    assert any(v.startswith("V007") and f"'{name}'" in v for v in said), said


@pytest.mark.parametrize("spelling", ["quoted", "upper", "mixed"])
@pytest.mark.parametrize("name", _OWN)
def test_a_function_of_the_products_own_is_refused_however_it_is_written(name, spelling):
    said = _violations(_PLACES["select_list"].format(call=_spellings(name)[spelling]))
    assert any(v.startswith("V007") and f"'{name}'" in v for v in said), said


@pytest.mark.parametrize(
    "sql",
    [
        # S17: a setting set on a pooled connection is its next caller's.
        "SELECT set_config('search_path', 'other', false) FROM bank.customer c",
        "SELECT c.id FROM bank.customer c WHERE set_config('role', 'postgres', false) <> ''",
        "SELECT pg_catalog.set_config('statement_timeout', '0', false)",
        "SELECT pg_read_file('/etc/passwd')",
        "SELECT lo_export(1, '/tmp/x')",
        # H7: SQL as text, files, large objects, other servers, time.
        "SELECT * FROM ts_stat('SELECT usename::text::tsvector FROM pg_shadow')",
        "SELECT ts_rewrite('a'::tsquery, 'SELECT t, s FROM aliases') FROM bank.customer c",
        "SELECT pg_read_file('/etc/passwd') FROM bank.customer c",
        "SELECT lo_import('/etc/passwd') FROM bank.customer c",
        "SELECT * FROM dblink('host=10.0.0.1', 'SELECT 1') AS t(a int)",
        "SELECT c.id FROM bank.customer c WHERE pg_sleep(3600) IS NULL",
        # T3: any Faker method, with a client's arguments.
        "SELECT provisa_fake_method('file_path', '{}', 1, 1) FROM bank.customer c",
    ],
)
def test_the_reported_statements_are_refused(sql):
    assert any(v.startswith("V007") for v in _violations(sql)), _violations(sql)


def test_the_refusal_names_the_function_and_no_requirement():
    (said,) = _violations("SELECT PG_Read_File('/etc/passwd') FROM bank.customer c")
    assert said == (
        "V007 Function 'pg_read_file' is not served: it reads or acts on its server, "
        "outside the tables a statement is governed by"
    )
    (own,) = _violations("SELECT provisa_digest('k', c.name) FROM bank.customer c")
    assert own == "V007 Function 'provisa_digest' is not served: it is the product's own"
    assert "REQ" not in said + own


def test_a_call_is_refused_once_however_often_it_is_read():
    said = _violations("SELECT pg_sleep(1), pg_sleep(2) FROM bank.customer c")
    assert said == [
        "V007 Function 'pg_sleep' is not served: it reads or acts on its server, "
        "outside the tables a statement is governed by"
    ]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT lower(c.name), upper(c.name), length(c.name) FROM bank.customer c",
        "SELECT date_part('year', now()), date_trunc('day', now()) FROM bank.customer c",
        "SELECT extract(epoch FROM now()), age(now()) FROM bank.customer c",
        "SELECT array_position(ARRAY[1, 2, 3], c.id) FROM bank.customer c",
        "SELECT to_char(c.score, '999D99'), to_char(now(), 'YYYY-MM-DD') FROM bank.customer c",
        "SELECT jsonb_extract_path_text(c.doc, 'a'), jsonb_typeof(c.doc) FROM bank.customer c",
        "SELECT jsonb_build_object('id', c.id), jsonb_array_length(c.doc) FROM bank.customer c",
        'SELECT pg_catalog.lower(c.name), pg_catalog."upper"(c.name) FROM bank.customer c',
        "SELECT coalesce(c.name, ''), nullif(c.id, 0), abs(c.score) FROM bank.customer c",
        "SELECT count(*), sum(c.score), string_agg(c.name, ',') FROM bank.customer c",
        "SELECT row_number() OVER (ORDER BY c.id), md5(c.name) FROM bank.customer c",
        "SELECT regexp_replace(c.name, 'a', 'b'), split_part(c.name, ' ', 1) FROM bank.customer c",
        "SELECT current_setting('timezone'), pg_typeof(c.id), version() FROM bank.customer c",
        "SELECT pg_backend_pid(), txid_current(), random() FROM bank.customer c",
        "SELECT to_regclass('bank.customer'), has_table_privilege('x', 'SELECT')",
        "SELECT ts_rank(to_tsvector(c.name), to_tsquery('a')) FROM bank.customer c",
        "SELECT * FROM generate_series(1, 3) AS g",
    ],
)
def test_an_ordinary_function_is_served_as_before(sql):
    assert not [v for v in _violations(sql) if v.startswith("V007")], _violations(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'pg_read_file(''/etc/passwd'')' AS v FROM bank.customer c",
        "SELECT c.name AS pg_sleep FROM bank.customer c",
        'SELECT c.name AS "provisa_digest" FROM bank.customer c',
        "SELECT x.set_config FROM (SELECT c.name AS set_config FROM bank.customer c) x",
    ],
)
def test_a_statement_that_only_holds_the_word_is_not_refused(sql):
    assert not [v for v in _violations(sql) if v.startswith("V007")], _violations(sql)


# --- one list, read by the meta path and the validator -------------------------------------------


def test_the_meta_path_reads_the_same_list():
    assert metadata_functions.READS_THE_SERVER is server_functions.READS_THE_SERVER
    from provisa.pgwire import catalog

    assert catalog.READS_THE_SERVER is server_functions.READS_THE_SERVER


def test_the_meta_paths_list_is_what_it_was():
    # The 65 names the meta path refused by name before the list moved: none added, none dropped.
    assert len(READS_THE_SERVER) == 65
    assert {"ts_stat", "ts_rewrite", "dblink", "pg_sleep", "nextval", "query_to_xml"} <= (
        READS_THE_SERVER
    )
    assert not READS_THE_SERVER & {"pg_read_file", "lo_import", "set_config"}  # they have forms


def test_no_name_is_listed_twice():
    assert not READS_THE_SERVER & ACTS_ON_THE_SERVER
    listed = READS_THE_SERVER | ACTS_ON_THE_SERVER
    assert not {name for name in listed if name.startswith(ACTING_PREFIXES)} - {
        # Listed by name on the meta path, whose list is kept as it was.
        "binary_upgrade_create_empty_extension",
        "pg_replication_origin_session_reset",
    }


def test_every_function_postgresql_revokes_from_public_is_refused():
    # PostgreSQL's own list of what an ordinary login may not run (catalog_pg17_acl).
    for _, signature, _ in REVOKED_FUNCTIONS:
        name = signature.partition("(")[0]
        assert refused_in_a_governed_statement(name, True) == "server", name


def test_every_form_of_the_meta_path_that_acts_is_refused():
    # The meta path answers these as a server with no files, large objects or slots; a source or
    # an engine sent the name would run it.
    for name in ("pg_read_file", "pg_ls_dir", "lo_import", "lo_export", "pg_reload_conf"):
        assert any(form == name for form, _ in metadata_functions.SCALAR_FORMS), name
        assert refused_in_a_governed_statement(name, False) == "server", name


def test_the_reason_a_function_is_refused():
    assert refused_in_a_governed_statement("TS_STAT", False) == "server"
    assert refused_in_a_governed_statement("Provisa_Fake_Method", False) == "own"
    assert refused_in_a_governed_statement("lower", True) is None
    assert refused_in_a_governed_statement("pg_stat_get_activity", False) == "server"
    assert refused_in_a_governed_statement("lastval", True) == "server"
    assert refused_in_a_governed_statement("lastval", False) is None


def test_the_functions_that_run_sql_as_text_are_of_the_meta_paths_list():
    assert RUNS_SQL_AS_TEXT <= READS_THE_SERVER
    assert MAPPINGS <= RUNS_SQL_AS_TEXT


@pytest.mark.parametrize("name", sorted(WRITTEN_BEFORE_GOVERNANCE))
def test_what_the_product_writes_before_governance_is_not_refused(name):
    # A catalog function's form and an answered XML value are in the statement the validator
    # reads (pgwire.catalog_route.address, compiler.xml_mapping.fold).
    assert not [v for v in _violations(f"SELECT {name}('a') AS v") if v.startswith("V007")]
    assert name.startswith("provisa_")


def test_the_forms_of_the_meta_path_call_nothing_else_of_the_products_own():
    import sqlglot
    import sqlglot.expressions as exp

    for form in (*metadata_functions.FORMS.values(), *metadata_functions.RELATION_FORMS.values()):
        for call in sqlglot.parse_one(form.sql, read="postgres").find_all(exp.Anonymous):
            assert refused_in_a_governed_statement(call.name, True) != "own", (form.name, call.name)
