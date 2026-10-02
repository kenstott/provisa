# Copyright (c) 2026 Kenneth Stott
# Canary: 166d371b-c4d7-4d86-a187-444fa2498949
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prepared-statement-style cache for the raw-SQL pipeline's pure pre-governance stage.

REQ-1866: a raw-SQL surface (pgwire today; Bolt/HTTP-Cypher/Flight SQL are follow-ups) re-parses
and re-normalizes caller-authored SQL text on every single call, even a byte-identical repeat,
while GraphQL's resolver never pays this cost at all (it emits already-semantic SQL directly from
its schema, compiled once at boot). This module caches exactly that gap and nothing else.

SCOPE, deliberately narrow: this cache covers ONLY the localize-check (REQ-1159) and
metric-expansion (REQ-1317) structural work `_govern_and_route_planned`
(provisa/pgwire/_pipeline.py) does before it ever calls into governance. It does NOT touch, cache,
or shortcut `build_governance_context`/`validate_sql`/`apply_governance`/`decide_route` (masking,
RLS, relationship-guard, or routing) — those already run once per call for every surface,
including GraphQL, and are confirmed cheap (GraphQL's own ~17ms total includes them); there is no
performance case for touching them, only risk. They keep running in full, on every call,
unconditionally, whether this cache hits or misses.

REQ-1886 (this revision): the key is now `(role_id, schema generation, SQL SHAPE)` — the same
literal-blanked shape hash `compiled_query_cache.sql_shape_digest` uses (see below for why this
module computes an equivalent digest itself rather than calling that function) — not the raw text.
Before this change a cache HIT required byte-identical raw SQL, so any traffic shape whose only
per-call variation is a literal (a point lookup with a different id every call — the dominant
OLTP-style pattern over pgwire) NEVER hit this cache at all; every call was a miss, and even a hit
still re-parsed the cached SQL text from scratch (see git history), so the cache saved nothing for
that traffic shape and very little in general.

THE CORRECTNESS CONSTRAINT THIS REVISION IS BUILT AROUND: a shape-keyed cache can be HIT by a call
whose literal values differ from the call that populated the entry. `PreparedFrontEnd.parsed` and
`.normalized_sql` (and `.metric_semantic_sql`) MUST always reflect the CURRENT call's actual
literals — serving a prior call's baked-in literal values on a hit would be a silent
wrong-answer bug (querying `order_id = 999` and silently getting the plan/SQL for `order_id = 1`
because it hit the same shape). Concretely, this rules out ever storing `normalized_sql` or
`metric_semantic_sql` verbatim in the cache and replaying them on a hit — those strings embed
literal values (`expand_metric_query` passes the caller's WHERE/LIMIT/ORDER clauses through into
its output, unlike the rest of the SELECT list, which is built purely from schema-configured
metric/dimension definitions — see `_TEMPLATE_LITERAL_ARGS` below and `metric_expand.py`).

WHAT IS ACTUALLY CACHED, and why each piece is safe:
  - Whether a shape requires the REQ-1159 localize step at all. `find_command_calls`
    (provisa/executor/command_localize.py) detects a composed command purely by the callee NAME in
    an `exp.Anonymous` node — never by argument values — so "does this SQL SHAPE compose a
    registered command" is literal-independent and stable for the shape under a fixed
    `tracked_functions` registry (which is itself schema-generation-scoped: a tracked-function
    registration/removal bumps `schema_version`, per REQ-1866's existing invalidation note below).
    A shape that ANY call previously localized a command for is NEVER entered into this cache at
    all (unchanged from before this revision, see "NOT CACHEABLE" below) — so a cache HIT, by
    construction, means "the localize step is a structural no-op for this shape", and the call
    below skip it entirely rather than re-running `localize_inline_commands`.
  - For a shape that needs REQ-1317 metric expansion (`expand_metric_query` returns non-None): the
    EXPENSIVE, LITERAL-INDEPENDENT part of that call's output — the resolved dimension/join plan
    and the metric-expression SELECT list, both built purely from schema config
    (`state.metrics`/`state.tables`/`state.relationships`, all generation-scoped) — is cached as a
    template tree. The only literal-bearing pieces `expand_metric_query` passes through unchanged
    are the caller's WHERE/LIMIT/ORDER clauses (verified by reading `expand_metric_query`: it
    `.copy()`s `tree`'s `limit`/`order` nodes directly, and rebuilds WHERE from a `.copy()` of the
    caller's own condition tree with only column table-qualifiers stripped, never literals
    touched). A later call of the SAME shape has the SAME WHERE/LIMIT/ORDER structure by
    definition (shape-identical means literal-blanked-identical) with only literal values
    differing, in the SAME left-to-right occurrence order — confirmed by direct test: expanding a
    metric query and diffing its WHERE/LIMIT literal nodes against the pre-expansion tree's,
    position-for-position, in-order. So on a hit, the join-plan/SELECT-list rebuild is skipped
    entirely; only the WHERE/LIMIT/ORDER literal nodes are swapped into a fresh COPY of the cached
    template (never the shared cached template itself) for THIS call's actual values. A literal
    COUNT mismatch between the template and the current call (which should be structurally
    impossible for a genuine shape match) raises loudly rather than ever substituting a
    best-effort/partial/wrong splice — see `_splice_current_literals`.
  - One narrower case is excluded from templating even though it IS metric expansion: the
    "`SELECT * FROM (<inner>) _sample LIMIT n`" UI-sampling wrapper (`expand_metric_query`'s own
    one-level subquery recursion). Its outer LIMIT and the inner query's WHERE live at two
    different tree depths reached by two different recursive calls, so the simple
    "walk WHERE/LIMIT/ORDER of the top-level tree, in order" correspondence this module relies on
    does not hold cleanly for it. Rather than risk a subtly wrong splice for a case that's cheap to
    just recompute, this shape is cached with a `"rerun"` marker: `expand_metric_query` runs fresh
    on every call (no join-plan reuse), same as before this revision — still correct, just without
    the extra speedup the direct case gets.

NOT CACHEABLE, and deliberately excluded (unchanged from before this revision): a statement that
composes a registered command inline (`_localize_inline_commands` returns True) invokes that
command with THIS call's literal arguments and bakes the result into the rewritten tree — the
output is per-call by construction, never reusable across calls even for the identical SQL shape.
Such a statement is simply never cached; the caller falls through to running the original,
uncached code path exactly as before this module existed.

ONE PARSE PER CALL: `raw_sql` is parsed on every call, cache hit or not, to learn THIS call's own
literal values. (Amended 2026-09-30, REQ-589:) pgwire no longer inlines bind values — a prepared
statement reaches this stage with its `$1`/`$2` placeholders and the values travel separately as
bound parameters, so for a pgwire prepared statement the text is identical across values and its
parse result could be looked up by text. That text-keyed lookup is not implemented here; a client
that inlines its own literals still needs the parse to learn them. This module makes sure the
parse is the ONLY parse: the SQL-shape
digest below is computed directly from the tree that parse already produced (never a second
`sqlglot.parse_one` of the same text purely to hash its shape, which an earlier draft of this
change did and which `compiled_query_cache.sql_shape_digest` would also do if called here — hence
the small local `_shape_digest_from_tree`, deliberately duplicating that function's
literal-blanking transform rather than its parse).

Invalidation: keyed on `(state.schema_boot_id, state.schema_version)`, the same generation pair
`_rebuild_schemas_impl` already bumps on every schema/masking/RLS/relationship/tracked-function
change (see REQ-1866's own note on the pre-existing RLS/role-mutation rebuild gap, fixed
separately) — a schema/config change invalidates every cached entry for every role at once.
"""

# Requirements: REQ-1866, REQ-1886

from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal as TypingLiteral

import sqlglot
from sqlglot import expressions as exp

from provisa.compiler.definitions import refuse_definition

# Bounded so a pathological caller issuing endless distinct one-off SQL shapes cannot grow this
# without limit — an ordinary LRU-by-insertion-order eviction (not a strict LRU-by-access) is
# enough here: this is a pure speed optimization, never a correctness dependency, so evicting the
# "wrong" entry only costs a future cache miss, never a wrong answer.
_MAX_ENTRIES = 4096

_CacheMode = TypingLiteral["plain", "template", "rerun"]

_lock = threading.Lock()
# key -> (generation, mode, template)
#   mode == "plain":    no metric expansion applies to this shape; `template` is None.
#   mode == "template":  `template` is the cached metric-expanded tree with this build's own
#                         WHERE/LIMIT/ORDER literals still in it — a hit splices in the CURRENT
#                         call's own literals into a fresh copy before ever handing it out (see
#                         `_splice_current_literals`); the stored tree itself is never mutated or
#                         returned directly.
#   mode == "rerun":     metric expansion applies via the subquery-sampling-wrapper path, where
#                         template-splicing is not attempted (see module docstring); `template` is
#                         None and `expand_metric_query` runs again on every hit.
_cache: dict[str, tuple[tuple[str, int], _CacheMode, Any]] = {}  # Any: template tree (see below)

# The clauses `expand_metric_query` passes through from the caller's own tree essentially
# unchanged (a `.copy()`, or a rebuild-from-`.copy()` for WHERE) — the ONLY places a metric-expanded
# output can carry a call-specific literal. Never the SELECT list: that's built purely from
# schema-configured metric/dimension definitions (see module docstring).
_TEMPLATE_LITERAL_ARGS = ("where", "limit", "order")


@dataclass(frozen=True)
class PreparedFrontEnd:
    """The result of the cacheable pre-governance stage: an already-parsed, already-localized,
    already-metric-expanded statement, ready to hand to governance exactly as
    `_govern_and_route_planned` would have produced it uncached."""

    normalized_sql: str
    parsed: Any  # a sqlglot exp.Expression; typed loosely to sidestep sqlglot's own Expr/
    # Expression stub mismatch (isinstance-true at runtime; pyright disagrees statically)
    cache_hit: bool
    # REQ-1322: None unless a metrics.<name> reference was expanded this call — carried through
    # a cache hit too, so the explain surface reports the same form it would have uncached.
    metric_semantic_sql: str | None = None
    # REQ-1877: True iff this call localized an inline tracked-function command
    # (`_localize_inline_commands` returned True) — the statement invoked a live command with
    # THIS call's arguments and baked its (possibly side-effecting, possibly non-deterministic)
    # result into the tree. A downstream cache (the compiled-query-outcome cache) MUST treat such
    # a call as uncacheable even when the raw SQL shape is byte-identical to a previous call.
    localized: bool = False


def _blank_literal(node: Any) -> Any:
    if isinstance(node, exp.Literal):
        return exp.Placeholder()
    return node


def _shape_digest_from_tree(tree: Any) -> str:
    """SHA-256 of `tree` with every literal blanked, computed straight from an already-parsed
    tree. Same algorithm as `compiled_query_cache.sql_shape_digest` (parse, blank every
    `exp.Literal`, re-render, hash) but taking the tree instead of raw text: this module always
    has a tree in hand already (it must parse `raw_sql` regardless, to learn this call's own
    literal values — see module docstring), so hashing from it avoids a second, redundant parse of
    the same caller-authored SQL purely to compute a shape key."""
    blanked = tree.copy().transform(_blank_literal)
    return hashlib.sha256(blanked.sql(dialect="postgres").encode()).hexdigest()


def _cache_key(shape_digest: str, role_id: str, generation: tuple[str, int]) -> str:
    return f"{generation[0]}\x00{generation[1]}\x00{role_id}\x00{shape_digest}"


def _clause_literals(node: Any) -> list[exp.Literal]:
    """Literal nodes within one WHERE/LIMIT/ORDER clause, in document order."""
    if node is None:
        return []
    return list(node.find_all(exp.Literal))


def _template_literals(tree: Any) -> list[exp.Literal]:
    lits: list[exp.Literal] = []
    for arg in _TEMPLATE_LITERAL_ARGS:
        lits.extend(_clause_literals(tree.args.get(arg)))
    return lits


def _splice_current_literals(template: Any, current_tree: Any, *, cache_key: str) -> Any:
    """A fresh copy of `template` with its WHERE/LIMIT/ORDER literal nodes replaced, in order, by
    `current_tree`'s own — never mutates the shared cached `template` itself (concurrent/future
    hits must see the original, unmodified template).

    A literal-count mismatch between the two means the shape-digest match does not actually imply
    the structural correspondence this splice depends on (a shape-digest collision, or a bug in
    that correspondence) — refusing loudly here, rather than substituting a partial/best-guess
    splice, is the only acceptable behavior: a wrong splice would silently run this call against a
    stale literal value.
    """
    current_literals = _template_literals(current_tree)
    working = template.copy()
    template_literals = _template_literals(working)
    if len(current_literals) != len(template_literals):
        raise RuntimeError(
            f"prepare_front_end: WHERE/LIMIT/ORDER literal count drifted for cache key "
            f"{cache_key!r} — cached template has {len(template_literals)}, this call's SQL has "
            f"{len(current_literals)}. Refusing to splice: this would risk silently executing "
            "against a stale literal value."
        )
    for template_node, current_node in zip(template_literals, current_literals):
        template_node.replace(current_node.copy())
    return working


def _is_wrapped_sampling_query(tree: Any) -> bool:
    """True for the UI sampling wrapper `SELECT * FROM (<inner>) _sample LIMIT n` that
    `expand_metric_query` recurses into one level for — see module docstring for why this shape is
    excluded from template-splicing."""
    if not isinstance(tree, exp.Select):
        return False
    from_clause = tree.args.get("from_") or tree.find(exp.From)
    if from_clause is None:
        return False
    return isinstance(from_clause.this, exp.Subquery)


def _metric_tables_from_state(state: object) -> Mapping[str, Any]:
    return {
        t["table_name"]: {
            "id": t["id"],
            "columns": [c["column_name"] for c in t.get("columns", [])],
        }
        for t in getattr(state, "tables", [])
    }


def clear() -> None:
    """Drop every cached entry. Exposed for tests; production invalidation is generation-keyed
    (a stale generation's entries simply stop matching, see module docstring) so this is never
    called from request-serving code."""
    with _lock:
        _cache.clear()


async def _run_metric_expand(parsed_input: Any, state: object) -> tuple[Any, str | None]:
    """Run REQ-1317 metric expansion fresh against `parsed_input` and the current `state`. Returns
    `(tree, metric_semantic_sql)` — `tree` is `parsed_input` unchanged when no expansion applies."""
    metric_registry = getattr(state, "metrics", {})
    if not metric_registry:
        return parsed_input, None

    from provisa.compiler.metric_expand import expand_metric_query

    metric_tables = _metric_tables_from_state(state)
    expanded = expand_metric_query(
        parsed_input, metric_registry, metric_tables, getattr(state, "relationships", [])
    )
    if expanded is None:
        return parsed_input, None

    normalized_sql = expanded.sql(dialect="postgres")
    from provisa.observability.stage_trace import trace_stage

    trace_stage("metric.expand", normalized_sql)
    return expanded, normalized_sql


async def prepare_front_end(
    raw_sql: str,
    role_id: str,
    state: object,  # object-ok: AppState lives in provisa.api, which this compiler-layer module
    # must not import (the exact layering violation this parameter's injection avoids below) —
    # only ever duck-typed via getattr, never a concrete attribute access on `state` itself.
    localize_inline_commands: Callable[[Any, str, object], Awaitable[bool]],
) -> PreparedFrontEnd:
    """Parse + localize-check + metric-expand `raw_sql` for `role_id`, using the cache when the
    same SQL SHAPE for the same role has already gone through this stage under the current schema
    generation and never found an inline command to localize.

    `raw_sql` is always parsed exactly once per call, hit or miss (see module docstring for why
    that parse cannot itself be avoided). What a hit skips is the STRUCTURAL work after it: the
    localize-check re-walk, and — for a metric-expanded shape — `expand_metric_query`'s
    dimension-resolution/join-plan rebuild, whose result is spliced with this call's own
    WHERE/LIMIT/ORDER literals instead of being recomputed.

    `localize_inline_commands` is injected (REQ-1159's `_localize_inline_commands`, always) rather
    than imported directly: `provisa/compiler/` is a lower layer `pgwire`/`executor`/`api` depend
    on, never the reverse (enforced by this repo's import-linter contracts) — `_pipeline.py` lives
    above this module, so this module cannot import it, only accept it as a parameter from a
    caller that already can.
    """
    generation = (
        getattr(state, "schema_boot_id", ""),
        getattr(state, "schema_version", 0),
    )
    loop = asyncio.get_running_loop()
    # REQ-1882: sqlglot parsing is synchronous CPU work — a live py-spy dump caught it running
    # in-line on the shared governance event loop under concurrent load, blocking every other
    # concurrent request's governance step for its duration. run_in_executor moves it to the
    # default thread pool so the loop stays free to dispatch other work while this parse runs.
    # This is the one mandatory parse every call pays (see module docstring); it also supplies the
    # tree the shape digest below is hashed from, so no second parse is needed to key the cache.
    parsed_input = await loop.run_in_executor(
        None, lambda: sqlglot.parse_one(raw_sql, read="postgres")
    )
    # Nothing is defined through a query protocol (provisa/compiler/definitions.py): a statement
    # that creates, alters or drops a relation is refused here, as soon as its kind is known and
    # before any of the pipeline's work on it.
    refuse_definition(parsed_input)
    shape_digest = _shape_digest_from_tree(parsed_input)
    key = _cache_key(shape_digest, role_id, generation)

    with _lock:
        cached = _cache.get(key)

    if cached is not None and cached[0] == generation:
        _gen, mode, template = cached
        if mode == "plain":
            return PreparedFrontEnd(normalized_sql=raw_sql, parsed=parsed_input, cache_hit=True)
        if mode == "rerun":
            expanded, metric_semantic_sql = await _run_metric_expand(parsed_input, state)
            if metric_semantic_sql is None:
                # The shape was observed to expand under this exact generation before; a live
                # rerun that now finds nothing to expand means the shape/generation correspondence
                # this cache depends on no longer holds — loud failure, not a silent stale-shaped
                # reuse of `parsed_input` as if it were the (never-computed) expansion.
                raise RuntimeError(
                    f"prepare_front_end: cached mode 'rerun' for key {key!r} but metric expansion "
                    "found nothing to expand this call — invariant violated."
                )
            return PreparedFrontEnd(
                normalized_sql=metric_semantic_sql,
                parsed=expanded,
                cache_hit=True,
                metric_semantic_sql=metric_semantic_sql,
            )
        assert mode == "template" and template is not None
        spliced = _splice_current_literals(template, parsed_input, cache_key=key)
        normalized_sql = spliced.sql(dialect="postgres")
        from provisa.observability.stage_trace import trace_stage

        trace_stage("metric.expand", normalized_sql)
        return PreparedFrontEnd(
            normalized_sql=normalized_sql,
            parsed=spliced,
            cache_hit=True,
            metric_semantic_sql=normalized_sql,
        )

    localized = await localize_inline_commands(parsed_input, role_id, state)
    if localized:
        # Per-call by construction (baked-in literal command-invocation result) — never cache
        # this statement, this call or any future one. Behave exactly as the uncached path would.
        normalized_sql = parsed_input.sql(dialect="postgres")
        return PreparedFrontEnd(
            normalized_sql=normalized_sql, parsed=parsed_input, cache_hit=False, localized=True
        )

    expanded, metric_semantic_sql = await _run_metric_expand(parsed_input, state)
    if metric_semantic_sql is None:
        with _lock:
            if len(_cache) >= _MAX_ENTRIES:
                _cache.pop(next(iter(_cache)))
            _cache[key] = (generation, "plain", None)
        return PreparedFrontEnd(normalized_sql=raw_sql, parsed=parsed_input, cache_hit=False)

    mode: _CacheMode = "rerun" if _is_wrapped_sampling_query(parsed_input) else "template"
    template = None if mode == "rerun" else expanded.copy()
    with _lock:
        if len(_cache) >= _MAX_ENTRIES:
            _cache.pop(next(iter(_cache)))
        _cache[key] = (generation, mode, template)

    return PreparedFrontEnd(
        normalized_sql=metric_semantic_sql,
        parsed=expanded,
        cache_hit=False,
        metric_semantic_sql=metric_semantic_sql,
    )
