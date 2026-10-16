# Copyright (c) 2026 Kenneth Stott
# Canary: 9740aaad-4932-4bed-a762-38974c7b08a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""AST-transform invariant tests (REQ-913).

Every governance/compilation transform must operate on a parsed AST, not on the
SQL text. These tests encode that invariant by exercising inputs that defeat the
current *string/regex* transforms:

  - a text scan for the projection's end takes the FIRST `FROM` → a scalar subquery in
    the projection moves the boundary, so masking silently skips the real columns (leak).
  - rls._has_alias tests literal `'"t0"' in sql` → any other alias convention
    (Cypher-derived SQL, hand-written aliases) skips qualification, emitting a bare,
    scope-ambiguous predicate.
  - stage2._apply_limit_ceiling bails on a parameterized LIMIT → the row-cap ceiling
    is not enforced.

They FAIL against the string implementations and are the acceptance criteria for the
move to AST-based transforms. Marked xfail(strict) so the suite stays green while red
and flips to a hard failure (forcing marker removal) once the restructure lands.
"""

from provisa.compiler.mask_inject import MaskingRules
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.security.masking import MaskType, MaskingRule
from tests.helpers import governed_for_role


def _meta(table_id=2, field_name="customers", table_name="customers", source_id="pg"):
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=field_name.capitalize(),
        source_id=source_id,
        catalog_name=source_id,
        schema_name="public",
        table_name=table_name,
    )


def _ctx(tables=None):
    ctx = CompilationContext()
    ctx.tables = tables or {"customers": _meta()}
    ctx.joins = {}
    return ctx


def test_masking_survives_scalar_subquery_in_projection():
    """A scalar subquery in the SELECT list must not move the masking boundary.

    The first `FROM` belongs to the inner subquery, so the string transform never
    reaches "ssn" and the sensitive column leaks unmasked.
    """
    sql = (
        'SELECT (SELECT MAX("total") FROM "public"."orders") AS "cnt", '
        '"ssn" FROM "public"."customers"'
    )
    rules: MaskingRules = {
        (2, "analyst"): {
            "ssn": (MaskingRule(mask_type=MaskType.constant, value="HIDDEN"), "varchar"),
        }
    }
    result = governed_for_role(sql, _ctx(), "analyst", masking_rules=rules)
    assert result == (
        'SELECT (SELECT MAX("total") FROM "public"."orders") AS "cnt", '
        '\'HIDDEN\' AS "ssn" FROM "public"."customers"'
    ), "ssn leaked unmasked"


def test_rls_predicate_is_scoped_to_root_alias_regardless_of_alias_name():
    """RLS filter must bind to the root table's alias for any alias convention.

    The query aliases the root as "c" (not "t0"); the injected predicate must be
    qualified `"c"."region"`, not a bare `region` that binds ambiguously under joins.
    """
    rls = RLSContext(rules={2: "region = 'us'"})
    result = governed_for_role(
        'SELECT "c"."id" FROM "public"."customers" "c"', _ctx(), "analyst", rls=rls
    )
    # The predicate column must be qualified with the root alias, not emitted bare.
    assert result == (
        'SELECT "c"."id" FROM "public"."customers" AS "c" WHERE ("c"."region" = \'us\')'
    ), "RLS predicate not scoped to root alias"


def test_row_cap_ceiling_enforced_over_parameterized_limit():
    """A role row-cap ceiling must bound even a parameterized LIMIT.

    `_apply_limit_ceiling` returns the SQL unchanged when the LIMIT is `$1`, so a
    caller-supplied bound escapes the governance ceiling.
    """
    from provisa.compiler.stage2 import GovernanceContext, apply_governance

    gov = GovernanceContext(limit_ceiling=100)
    governed = apply_governance('SELECT "x" FROM "public"."t" LIMIT $1', gov)
    assert "100" in governed, "row-cap ceiling not imposed over parameterized LIMIT"
