# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9a41-5c3b-4f86-a0e1-9b4c6f2d8e53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column registered at a path in its MongoDB document, read through its governed relation and
every level of the chain below it: the faked projection (REQ-1494), the kept-mutation overlay
(REQ-1942) and the member projection (REQ-1620) (REQ-039).

Trino's MongoDB connector reads a document as it is: a nested document is a ROW column, and a
column registered at ``customer_profile.tier`` is no column of the table there. The table here is
that shape, in DuckDB (a STRUCT). The governed statement reads a path column by its own name, as
every other engine's relation of the table exposes it; on Trino the routed statement reads it at
its path (``sql_rewrite.read_document_paths``). So no level may read the table with ``*`` (Trino's
``*`` holds the document, not the column), and a level carries only what the level above reads: a
path column the statement does not read is not read off the table at all."""

# Requirements: REQ-039, REQ-1494, REQ-1620, REQ-1942

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.compiler import stage2
from provisa.compiler.rls import HIDDEN, PLAIN, MemberReads, as_rule
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.fakes import digest as digest_mod
from provisa.fakes.duckdb_functions import register
from provisa.security.masking import MaskingRule, MaskType

_TID = 3
_COLUMNS = [
    ("order_id", "integer"),
    ("status", "varchar"),
    ("channel", "varchar"),
    ("customer_tier", "varchar"),
    ("city", "varchar"),
]
# The perf bench's order_docs: customer_tier at customer_profile.tier; city two keys down.
_PATHS = {
    ("mongo", "db", "order_docs"): {
        "customer_tier": "customer_profile.tier",
        "city": "customer_profile.address.city",
    }
}
_LOG = ("mongo", "db", "order_docs_log")


@pytest.fixture
def con(monkeypatch):
    """The table as Trino's MongoDB catalog holds it: the document's nested fields in a ROW."""
    monkeypatch.setattr(digest_mod, "_key", b"k" * 32)
    c = duckdb.connect()
    register(c)
    c.execute("ATTACH ':memory:' AS mongo")
    c.execute("CREATE SCHEMA mongo.db")
    c.execute(
        "CREATE TABLE mongo.db.order_docs (order_id INTEGER, status VARCHAR, channel VARCHAR, "
        "customer_profile STRUCT(tier VARCHAR, address STRUCT(city VARCHAR)))"
    )
    c.execute(
        "INSERT INTO mongo.db.order_docs VALUES "
        "(1, 'open', 'web', {'tier': 'gold', 'address': {'city': 'Oslo'}}), "
        "(2, 'closed', 'store', {'tier': 'silver', 'address': {'city': 'Rome'}}), "
        "(3, 'open', 'store', {'tier': 'gold', 'address': {'city': 'Lima'}})"
    )
    # A Reversible environment's change log keeps a written row by its registered columns.
    c.execute(
        'CREATE TABLE mongo.db.order_docs_log ("__seq" BIGINT, "__op" VARCHAR, order_id INTEGER, '
        "status VARCHAR, channel VARCHAR, customer_tier VARCHAR, city VARCHAR)"
    )
    c.execute(
        "INSERT INTO mongo.db.order_docs_log VALUES (1, 'upsert', 2, 'closed', 'web', 'platinum', 'Bern')"
    )
    return c


def _gov(
    *, fakes: dict[str, str] | None = None, kept: bool = False, members=None
) -> GovernanceContext:
    gov = GovernanceContext(role_id="analyst")
    gov.table_map = {"mongo.db.order_docs": _TID, "db.order_docs": _TID}
    gov.all_columns = {_TID: list(_COLUMNS)}
    gov.visible_columns = {_TID: None}
    for name, decl in (fakes or {}).items():
        gov.masking_rules[(_TID, name)] = (MaskingRule(MaskType.fake, fake=decl), "varchar")
    stage2._bind_fakes(gov)
    if kept:
        gov.kept = {_TID: (_LOG, ["order_id"])}
    if members is not None:
        gov.member_reads = {_TID: members}
        gov.rls_rules = {_TID: as_rule("status = 'open' OR status = 'closed'")}
    return gov


def _members(column: str) -> MemberReads:
    """Two members: the first admits the open orders and reads ``column``; the second admits the
    closed ones and is not served it."""
    return MemberReads(
        flags=(as_rule("status = 'open'"), as_rule("status = 'closed'")),
        columns={column: ((PLAIN, None, "varchar"), (HIDDEN, None, "varchar"))},
    )


def _governed(sql: str, gov: GovernanceContext) -> str:
    return apply_governance(sql, gov)


def _run(con, governed: str) -> list[tuple]:
    from provisa.compiler.sql_rewrite import read_document_paths

    routed = read_document_paths(governed, "postgres", _PATHS)
    return con.execute(sqlglot.transpile(routed, read="postgres", write="duckdb")[0]).fetchall()


def _no_star_read_of_the_table(governed: str) -> bool:
    from provisa.compiler.sql_rewrite import read_document_paths

    tree = sqlglot.parse_one(read_document_paths(governed, "postgres", _PATHS), read="postgres")
    for table in tree.find_all(sqlglot.exp.Table):
        if table.name != "order_docs":
            continue
        select = table.find_ancestor(sqlglot.exp.Select)
        assert select is not None
        if any(isinstance(p, sqlglot.exp.Star) or p.is_star for p in select.expressions):
            return False
    return True


_READ = "SELECT d.order_id, d.customer_tier, d.city FROM mongo.db.order_docs d ORDER BY d.order_id"
_UNREAD = "SELECT d.order_id, d.status FROM mongo.db.order_docs d ORDER BY d.order_id"


# -- the governed relation alone ---------------------------------------------------------------


def test_a_read_path_column_is_read_at_its_path(con):
    governed = _governed(_READ, _gov())
    assert _run(con, governed) == [(1, "gold", "Oslo"), (2, "silver", "Rome"), (3, "gold", "Lima")]


def test_a_path_column_filters_at_its_path(con):
    sql = "SELECT d.order_id FROM mongo.db.order_docs d WHERE d.customer_tier = 'gold' ORDER BY 1"
    assert _run(con, _governed(sql, _gov())) == [(1,), (3,)]


def test_an_unread_path_column_is_not_read(con):
    governed = _governed(_UNREAD, _gov())
    assert "customer_tier" not in governed and "city" not in governed
    assert _run(con, governed) == [(1, "open"), (2, "closed"), (3, "open")]


# -- under the faked projection (REQ-1494) -----------------------------------------------------


def test_a_read_path_column_under_fakes_is_read_at_its_path(con):
    governed = _governed(_READ, _gov(fakes={"channel": "categories((web, store))"}))
    assert _no_star_read_of_the_table(governed)
    assert _run(con, governed) == [(1, "gold", "Oslo"), (2, "silver", "Rome"), (3, "gold", "Lima")]


def test_an_unread_path_column_under_fakes_is_read_at_no_level(con):
    sql = "SELECT d.order_id, d.channel FROM mongo.db.order_docs d ORDER BY d.order_id"
    governed = _governed(sql, _gov(fakes={"channel": "categories((web, store))"}))
    assert "customer_tier" not in governed and "city" not in governed
    rows = _run(con, governed)
    assert [r[0] for r in rows] == [1, 2, 3] and {r[1] for r in rows} <= {"web", "store"}


def test_a_faked_path_column_fakes_the_value_at_its_path(con):
    governed = _governed(_READ, _gov(fakes={"customer_tier": "categories((bronze, platinum))"}))
    rows = _run(con, governed)
    assert [(r[0], r[2]) for r in rows] == [(1, "Oslo"), (2, "Rome"), (3, "Lima")]
    assert {r[1] for r in rows} <= {"bronze", "platinum"}
    assert rows[0][1] == rows[2][1]  # one real value, one fake


# -- under the kept-mutation overlay (REQ-1942) -------------------------------------------------


def test_a_read_path_column_under_kept_mutations_is_read_at_its_path(con):
    governed = _governed(_READ, _gov(kept=True))
    assert _no_star_read_of_the_table(governed)
    assert _run(con, governed) == [
        (1, "gold", "Oslo"),
        (2, "platinum", "Bern"),
        (3, "gold", "Lima"),
    ]


def test_an_unread_path_column_under_kept_mutations_is_read_at_no_level(con):
    governed = _governed(_UNREAD, _gov(kept=True))
    assert "customer_tier" not in governed and "city" not in governed
    assert _run(con, governed) == [(1, "open"), (2, "closed"), (3, "open")]


def test_a_read_path_column_under_fakes_and_kept_mutations_is_read_at_its_path(con):
    governed = _governed(_READ, _gov(fakes={"channel": "categories((web, store))"}, kept=True))
    assert _no_star_read_of_the_table(governed)
    assert _run(con, governed) == [
        (1, "gold", "Oslo"),
        (2, "platinum", "Bern"),
        (3, "gold", "Lima"),
    ]


# -- under the member projection (REQ-1620) -----------------------------------------------------


def test_a_read_path_column_under_members_is_read_at_its_path(con):
    sql = "SELECT d.order_id, d.customer_tier, d.channel FROM mongo.db.order_docs d ORDER BY 1"
    governed = _governed(sql, _gov(members=_members("channel")))
    assert _no_star_read_of_the_table(governed)
    assert _run(con, governed) == [(1, "gold", "web"), (2, "silver", None), (3, "gold", "store")]


def test_a_path_column_the_members_read_differently_is_read_at_its_path(con):
    governed = _governed(_READ, _gov(members=_members("customer_tier")))
    assert _run(con, governed) == [(1, "gold", "Oslo"), (2, None, "Rome"), (3, "gold", "Lima")]


def test_an_unread_path_column_under_members_is_read_at_no_level(con):
    sql = "SELECT d.order_id, d.channel FROM mongo.db.order_docs d ORDER BY d.order_id"
    governed = _governed(sql, _gov(members=_members("channel")))
    assert "customer_tier" not in governed and "city" not in governed
    assert _run(con, governed) == [(1, "web"), (2, None), (3, "store")]


def test_a_read_path_column_under_every_level_is_read_at_its_path(con):
    gov = _gov(fakes={"status": "categories((a, b))"}, kept=True, members=_members("channel"))
    sql = "SELECT d.order_id, d.customer_tier, d.channel FROM mongo.db.order_docs d ORDER BY 1"
    governed = _governed(sql, gov)
    assert _no_star_read_of_the_table(governed)
    rows = _run(con, governed)
    assert [(r[0], r[1]) for r in rows] == [(1, "gold"), (2, "platinum"), (3, "gold")]


def test_an_unread_path_column_under_every_level_is_read_at_no_level(con):
    gov = _gov(fakes={"status": "categories((a, b))"}, kept=True, members=_members("channel"))
    sql = "SELECT d.order_id, d.channel FROM mongo.db.order_docs d ORDER BY d.order_id"
    governed = _governed(sql, gov)
    assert "customer_tier" not in governed and "city" not in governed
    assert [r[0] for r in _run(con, governed)] == [1, 2, 3]


# -- the routed statement on Trino (sql_rewrite.read_document_paths) ---------------------------


def _paths(sql: str) -> str:
    from provisa.compiler.sql_rewrite import read_document_paths

    return read_document_paths(sql, "postgres", _PATHS)


def test_a_path_column_in_a_select_list_keeps_its_name():
    assert _paths('SELECT "d"."customer_tier" FROM "mongo"."db"."order_docs" AS "d"') == (
        'SELECT "d"."customer_profile"."tier" AS "customer_tier" '
        'FROM "mongo"."db"."order_docs" AS "d"'
    )


def test_a_path_column_in_an_expression_is_its_path():
    sql = 'SELECT upper("d"."city") AS c FROM "mongo"."db"."order_docs" AS "d" WHERE "d"."customer_tier" = \'gold\''
    assert _paths(sql) == (
        'SELECT UPPER("d"."customer_profile"."address"."city") AS c '
        'FROM "mongo"."db"."order_docs" AS "d" '
        'WHERE "d"."customer_profile"."tier" = \'gold\''
    )


def test_a_one_key_path_names_a_top_level_field():
    paths = {("mongo", "db", "products"): {"product_id": "_id"}}
    from provisa.compiler.sql_rewrite import read_document_paths

    sql = 'SELECT "p"."product_id" FROM "mongo"."db"."products" AS "p"'
    assert read_document_paths(sql, "postgres", paths) == (
        'SELECT "p"."_id" AS "product_id" FROM "mongo"."db"."products" AS "p"'
    )


def test_a_column_of_a_derived_table_or_another_catalog_is_left_as_it_is():
    derived = 'SELECT "d"."customer_tier" FROM (SELECT 1 AS "customer_tier") AS "d"'
    replica = 'SELECT "d"."customer_tier" FROM "mat_store"."db"."order_docs" AS "d"'
    assert _paths(derived) == derived
    assert _paths(replica) == replica
