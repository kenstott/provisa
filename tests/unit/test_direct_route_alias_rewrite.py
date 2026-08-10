# Copyright (c) 2026 Kenneth Stott
# Canary: 5c9a1b74-2e6f-4d18-9a03-7be5d2f81c4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1411: the DIRECT lowering keeps the semantic name as the ref's alias.

An ops/meta table is registered under a physical name that differs from the name clients see
(``query_audit_log`` is the view ``query_audit_log_ops``), and the query's column qualifiers bind
to the semantic name. Substituting the physical name before pinning that alias stranded every
qualifier — Postgres answered "missing FROM-clause entry for table query_audit_log" on the
provisa-admin route, which is how the admin Reports viewer reads its tables.
"""

import pytest

from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical
from provisa.compiler.sql_types import CompilationContext, TableMeta


@pytest.fixture
def ops_ctx():
    meta = TableMeta(
        table_id=1,
        field_name="query_audit_log",
        type_name="QueryAuditLog",
        source_id="provisa-admin",
        catalog_name="provisa_admin",
        schema_name="org_kstott",
        table_name="query_audit_log_ops",
        domain_id="ops",
        original_table_name="query_audit_log",
    )
    return CompilationContext(tables={"query_audit_log": meta})


def test_aliased_physical_table_keeps_semantic_alias(ops_ctx):
    sql = (
        'SELECT "query_audit_log"."id", "query_audit_log"."user_id" '
        'FROM "ops"."query_audit_log" ORDER BY "id" ASC LIMIT 101 OFFSET 0'
    )
    out = rewrite_semantic_to_physical(sql, ops_ctx)
    assert '"org_kstott"."query_audit_log_ops"' in out
    assert "AS query_audit_log" in out
    assert "AS query_audit_log_ops" not in out
    assert '"ops"."query_audit_log"' not in out


def test_column_qualifiers_resolve_against_the_emitted_alias(ops_ctx):
    import sqlglot
    import sqlglot.expressions as exp

    sql = 'SELECT "query_audit_log"."id" FROM "ops"."query_audit_log"'
    out = rewrite_semantic_to_physical(sql, ops_ctx)
    tree = sqlglot.parse_one(out, read="postgres")
    aliases = {t.alias_or_name for t in tree.find_all(exp.Table)}
    qualifiers = {c.table for c in tree.find_all(exp.Column) if c.table}
    assert qualifiers <= aliases, f"stranded qualifiers {qualifiers - aliases} in {out}"
