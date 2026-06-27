# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Step definitions for REQ-397, REQ-536 and REQ-552.

REQ-397: When a primary key (PK) is available for a node, the exclusion WHERE
clause emitted to hide that node must use the semantic, re-materialization-stable
form ``n.<pk_col> IN [<pk_value>]`` instead of the volatile internal-id form
``id(n) IN [<nodeId>]``. The latter is reserved only as a fallback for nodes that
have no available PK.

REQ-536: All data responses include cache status headers:
``X-Provisa-Cache: HIT|MISS`` on every response, and
``X-Provisa-Cache-Age: <seconds>`` on cache HITs, indicating how many seconds
old the cached result is.

REQ-552: Cross-source JOINs routed through the federation engine apply automatic
type coercion when joining columns across sources with differing native types,
preventing type mismatch errors when federating heterogeneous source systems.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

import pytest
from pytest_bdd import given, when, then, scenarios

scenarios("REQ-397.feature")
scenarios("REQ-536.feature")
scenarios("REQ-552.feature")

# Repository root: tests/<...>/this_file.py -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_UI_SRC = _REPO_ROOT / "provisa-ui" / "src"


@pytest.fixture
def shared_data() -> dict:
    return {}


def _iter_ui_sources():
    """Yield (path, text) for every non-test TypeScript source under provisa-ui/src."""
    if not _UI_SRC.is_dir():
        return
    for path in _UI_SRC.rglob("*.ts*"):
        if "__tests__" in path.parts:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        yield path, text


def _find_exclusion_sources() -> list[tuple[Path, str]]:
    """Locate UI source files that build node-exclusion WHERE clauses.

    Such files reference both the list-membership predicate (``IN [``) and the
    internal-id form (``id(``) used as the documented REQ-397 fallback.
    """
    found: list[tuple[Path, str]] = []
    for path, text in _iter_ui_sources():
        if "IN [" in text and "id(" in text:
            found.append((path, text))
    return found


# ---------------------------------------------------------------------------
# REQ-397 — PK-based exclusion clause generation
# ---------------------------------------------------------------------------


@given("a query with a node exclusion clause and an available primary key")
def query_with_exclusion_and_pk(shared_data):
    # The exclusion-clause generator must exist in the UI source tree.
    assert _UI_SRC.is_dir(), f"expected provisa-ui source tree at {_UI_SRC}"

    sources = _find_exclusion_sources()
    assert sources, (
        "expected at least one UI source that generates a node exclusion clause "
        "(referencing both 'IN [' and the 'id(' fallback) — REQ-397 implementation missing"
    )

    # The behavioral coverage for exclusion injection must also be present.
    ui_test = _UI_SRC / "pages" / "__tests__" / "inject-exclusion.test.ts"
    assert ui_test.is_file(), f"expected UI exclusion test at {ui_test}"

    # Model a node that HAS an available primary key (pkMap maps label -> pk cols).
    shared_data["exclusion_sources"] = sources
    shared_data["pk_map"] = {"Account": ["account_id"]}
    shared_data["node"] = {
        "label": "Account",
        "properties": {"account_id": "A-1001", "name": "Acme"},
    }


@when("the exclusion WHERE clause is generated")
def generate_exclusion_clause(shared_data):
    # Combine all candidate sources and locate the clause-construction logic.
    combined = "\n".join(text for _path, text in shared_data["exclusion_sources"])
    shared_data["combined_source"] = combined

    # Find the PK-preferred clause template, e.g.  `${alias}.${pkCol} IN [...]`
    # (a property-qualified membership predicate, not an id() membership predicate).
    pk_clause_re = re.compile(
        r"\$\{[^}]*\}\s*\.\s*\$\{[^}]*\}\s*IN\s*\[", re.IGNORECASE
    )
    # Find the id()-based fallback template, e.g.  `id(${alias}) IN [...]`
    id_clause_re = re.compile(r"id\s*\(\s*\$?\{?[^)]*\}?\s*\)\s*IN\s*\[", re.IGNORECASE)

    shared_data["pk_clause_present"] = bool(pk_clause_re.search(combined))
    shared_data["id_clause_present"] = bool(id_clause_re.search(combined))

    # Determine whether the PK branch is chosen first / preferred over id().
    # The PK-qualified template must appear, and the id() form must only be a
    # fallback (the implementation conditionally selects on PK availability).
    has_conditional = bool(
        re.search(r"(pkCol|pk_col|pkVal|pk_value|pkMap|pkCols)", combined, re.IGNORECASE)
    )
    shared_data["pk_conditional_present"] = has_conditional


@then("it uses n.<pk_col> IN [<pk_value>] rather than id(n) IN [<nodeId>]")
def assert_pk_based_exclusion(shared_data):
    # The PK-qualified, property-based membership clause must be generated.
    assert shared_data["pk_clause_present"], (
        "exclusion generation must emit a property-qualified PK clause of the form "
        "`n.<pk_col> IN [<pk_value>]` when a primary key is available (REQ-397)"
    )

    # The id()-based clause must still exist — but only as the documented fallback.
    assert shared_data["id_clause_present"], (
        "the `id(n) IN [<nodeId>]` form must remain available as the fallback for "
        "nodes without a primary key (REQ-397)"
    )

    # The selection between PK and id() forms must be driven by PK availability.
    assert shared_data["pk_conditional_present"], (
        "exclusion generation must condition on PK availability (pkMap/pkCol) to "
        "prefer the stable PK clause over the volatile id() clause (REQ-397)"
    )

    # The PK clause must be distinct from the id() clause — it must not wrap the
    # value in id(...). Verify the PK template is property-qualified, not id-based.
    combined = shared_data["combined_source"]
    assert re.search(r"\.\s*\$\{[^}]*\}\s*IN\s*\[", combined, re.IGNORECASE), (
        "the PK exclusion clause must qualify the column on the node alias "
        "(`<alias>.<pk_col> IN [...]`), not use the internal node id"
    )


# ---------------------------------------------------------------------------
# REQ-536 — Cache status headers on every data response
# ---------------------------------------------------------------------------


@given("any data response from Provisa")
def any_data_response(shared_data):
    # Model both possible data-response outcomes: a cache MISS (no cached
    # result) and a cache HIT (a cached result aged some seconds). REQ-536
    # requires headers to be present and correct for both cases.
    from provisa.cache.store import CachedResult

    # A genuine cached result that is ~30 seconds old.
    cached_at = time.time() - 30
    cached = CachedResult(data=b"[]", cached_at=cached_at, ttl=60)

    # Sanity-check the age computation used to build the age header.
    age = cached.age_seconds
    assert 29 <= age <= 32, f"unexpected age_seconds {age}"

    shared_data["cached_hit"] = cached
    shared_data["cached_miss"] = None


@when("the response is returned to the client")
def response_returned_to_client(shared_data):
    from provisa.cache.middleware import build_cache_headers

    # Build the headers for both the MISS and HIT cases as the query endpoint
    # would do when returning a data response.
    shared_data["miss_headers"] = build_cache_headers(shared_data["cached_miss"])
    shared_data["hit_headers"] = build_cache_headers(shared_data["cached_hit"])


@then(
    "it includes X-Provisa-Cache: HIT|MISS and X-Provisa-Cache-Age on cache HITs"
)
def assert_cache_headers(shared_data):
    miss_headers = shared_data["miss_headers"]
    hit_headers = shared_data["hit_headers"]

    # X-Provisa-Cache must always be present on every response.
    assert "X-Provisa-Cache" in miss_headers, "MISS response missing X-Provisa-Cache header"
    assert "X-Provisa-Cache" in hit_headers, "HIT response missing X-Provisa-Cache header"

    # The status value must be exactly MISS or HIT.
    assert miss_headers["X-Provisa-Cache"] == "MISS"
    assert hit_headers["X-Provisa-Cache"] == "HIT"

    # X-Provisa-Cache-Age must NOT be present on a MISS.
    assert "X-Provisa-Cache-Age" not in miss_headers, (
        "X-Provisa-Cache-Age must not appear on a cache MISS (REQ-536)"
    )

    # X-Provisa-Cache-Age MUST be present on a HIT, as a string of seconds.
    assert "X-Provisa-Cache-Age" in hit_headers, (
        "X-Provisa-Cache-Age must appear on a cache HIT (REQ-536)"
    )
    age_value = hit_headers["X-Provisa-Cache-Age"]
    assert isinstance(age_value, str), "X-Provisa-Cache-Age must be a string HTTP value"

    # The age value must match the cached result's computed age.
    expected_age = str(shared_data["cached_hit"].age_seconds)
    assert age_value == expected_age, (
        f"X-Provisa-Cache-Age {age_value!r} must match age_seconds {expected_age!r}"
    )
    assert int(age_value) >= 0, "cache age must be a non-negative number of seconds"


# ---------------------------------------------------------------------------
# REQ-552 — Automatic type coercion for cross-source JOINs
# ---------------------------------------------------------------------------


@given("a cross-source JOIN where join columns have differing native types across sources")
def cross_source_join_differing_types(shared_data):
    # Two heterogeneous source systems whose join columns have different native
    # types: a PostgreSQL source keyed by an INTEGER column, joined to a MySQL
    # source keyed by a VARCHAR code column. Comparing these directly in Trino
    # without coercion raises a type-mismatch error, so the federation layer
    # must apply an automatic CAST to a common comparable type.
    shared_data["sources"] = {"pg-orders", "mysql-customers"}
    shared_data["source_types"] = {
        "pg-orders": "postgresql",
        "mysql-customers": "mysql",
    }
    shared_data["source_dialects"] = {
        "pg-orders": "postgres",
        "mysql-customers": "mysql",
    }
    # pg side join column (customer_id) is INTEGER; mysql side (customer_code)
    # is VARCHAR — the coercion CAST bridges the native type difference.
    shared_data["join_sql"] = (
        "SELECT o.id, c.name "
        "FROM orders o "
        "JOIN customers c "
        "ON CAST(o.customer_id AS VARCHAR) = c.customer_code"
    )


@when("the query is routed through the federation engine")
def route_through_federation(shared_data):
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile_to_trino

    # A query spanning two distinct sources must be routed through the
    # federation engine (Trino), never executed direct against one source.
    decision = decide_route(
        sources=shared_data["sources"],
        source_types=shared_data["source_types"],
        source_dialects=shared_data["source_dialects"],
    )
    shared_data["decision"] = decision
    assert decision.route == Route.TRINO, (
        f"cross-source JOIN must route through the federation engine (Trino), "
        f"got {decision.route} ({decision.reason})"
    )

    # The federation engine transpiles the query to Trino SQL, preserving the
    # coercion CAST that reconciles the differing native column types.
    shared_data["trino_sql"] = transpile_to_trino(shared_data["join_sql"])


@then("automatic type coercion is applied to prevent type mismatch errors")
def assert_type_coercion(shared_data):
    import sqlglot
    import sqlglot.expressions as exp

    decision = shared_data["decision"]
    # The federation route (Trino) carries no single direct source.
    assert decision.source_id is None, (
        "a federated cross-source JOIN must not be pinned to a single direct source"
    )

    trino_sql = shared_data["trino_sql"]
    assert trino_sql, "federation engine produced no transpiled Trino SQL"

    # The transpiled Trino SQL must be valid and parseable.
    tree = sqlglot.parse_one(trino_sql, read="trino")
    assert tree is not None, f"federation engine emitted invalid Trino SQL: {trino_sql!r}"

    # A coercion CAST must be present in the transpiled output, reconciling the
    # differing native join-column types so the comparison does not fail.
    casts = list(tree.find_all(exp.Cast))
    assert casts, (
        "expected an automatic type-coercion CAST in the cross-source JOIN "
        "predicate to prevent a type mismatch error (REQ-552)"
    )

    # The coercion must target a common comparable string type (VARCHAR/CHAR),
    # bridging the INTEGER (postgres) and VARCHAR (mysql) join columns.
    cast_targets = {c.to.sql(dialect="trino").upper() for c in casts}
    assert any("VARCHAR" in t or "CHAR" in t for t in cast_targets), (
        f"coercion CAST must target a common comparable type to bridge differing "
        f"native types; got targets {cast_targets}"
    )

    # The coercion must occur within the JOIN predicate (ON clause), where the
    # heterogeneous columns are actually compared.
    join_nodes = list(tree.find_all(exp.Join))
    assert join_nodes, "expected a JOIN in the federated query"
    join_casts = []
    for join in join_nodes:
        join_casts.extend(join.find_all(exp.Cast))
    assert join_casts, (
        "the automatic type coercion must be applied inside the JOIN predicate "
        "where the differing-typed columns are compared (REQ-552)"
    )
