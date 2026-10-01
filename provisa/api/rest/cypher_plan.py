# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9f5a71-e3b8-4d06-8a4f-6b1d0e7c3f95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Cypher label map and a Cypher statement's translation, kept so a repeated request does not
rebuild them (REQ-1877).

Both are pure functions of the registry. The label map (``CypherLabelMap.from_schema``) names
every registered table and column; the translation (parse, Cypher -> SQL, graph rewrites, semantic
SQL) depends on the statement text and that map, never on a parameter's value — bound values
travel separately, in the order the translation names.

A translation is a plan: it is kept through ``provisa.pgwire.governed_plan``, the one mechanism
every surface keeps a plan with — the org's own size-bounded store, a key carrying the schema
generation and the role, and an entry that answers only while the registry objects it was built
from are still the same objects. A label map follows the same identity rule but is not an entry
of that store: it is as large as the registry, so it is held per org, role and domain-access
scope for exactly as long as the schema generation lasts, and never waits to be evicted
(:func:`kept_label_map`).

Every Cypher surface (HTTP, Bolt, Arrow Flight) reads both here. A label map is keyed by the
inputs it is built from, so two surfaces whose inputs agree share one map. A translation is keyed
by the surface as well, because each surface admits a statement by its own checks before it
translates it, and only an admitted statement is kept.

What is never kept: a write, a statement a surface answers without translating (procedures,
registered commands, catalog probes), and a statement that failed any check."""

# Requirements: REQ-1877

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from provisa.pgwire.governed_plan import PlanSlot

if TYPE_CHECKING:
    from provisa.cypher.label_map import CypherLabelMap


def _registry_anchors(state: Any) -> tuple[Any, ...]:
    """The registry objects a label map is built from, beyond the role's governance objects."""
    registry = state.schema_build_cache
    return (
        registry.get("tables"),
        registry.get("relationships"),
        registry.get("column_types"),
        state.source_catalogs,
    )


def _access_key(domain_access: list[str] | None) -> list[str] | None:
    return None if domain_access is None else sorted(domain_access)


@dataclass(frozen=True)
class _KeptLabelMap:
    generation: tuple[Any, Any]
    anchors: tuple[Any, ...]
    label_map: CypherLabelMap


def label_map_key(
    role_id: str, domain_access: list[str] | None, cross_domain: bool, business_view: bool
) -> tuple[Any, ...]:
    """What a label map depends on besides the registry itself: the role (whose compilation
    context it is built from) and the scope it is built for. Not the person — every person acting
    as the role under the same domain access reads the same map."""
    access = None if domain_access is None else tuple(sorted(domain_access))
    return (role_id, access, cross_domain, business_view)


def kept_label_map(
    state: Any,
    role_id: str,
    *,
    domain_access: list[str] | None,
    cross_domain: bool,
    business_view: bool,
    build: Callable[[], CypherLabelMap],
) -> CypherLabelMap:
    """The role's label map for these inputs, built by ``build`` only when none is kept.

    ``domain_access`` is the access the map is scoped to, ``cross_domain`` whether nodes reachable
    through another domain's relationships are included, ``business_view`` whether system-domain
    nodes are dropped. The map is shared by every request it answers: callers read it, never
    mutate it.

    Kept in the org's ``cypher_label_maps`` under the plan store's own rule
    (``pgwire.governed_plan``): a map answers only for the schema generation it was built in and
    only while the registry objects it was built from are still the same objects, and nothing is
    kept while a rebuild is in progress or when the generation moved during the build. Unlike a
    plan it does not sit in the size-bounded store until evicted — it is as large as the registry
    and has one entry per role scope, so the generation is its whole lifetime: keeping a new
    generation's map drops every older one."""
    from provisa.pgwire.governed_plan import _rebuild_in_progress, governance_anchors

    maps = state.cypher_label_maps
    key = label_map_key(role_id, domain_access, cross_domain, business_view)
    generation = (state.schema_boot_id, state.schema_version)
    anchors = (*governance_anchors(state, role_id), *_registry_anchors(state))
    kept = maps.get(key)
    if (
        isinstance(kept, _KeptLabelMap)
        and kept.generation == generation
        and len(kept.anchors) == len(anchors)
        and all(a is b for a, b in zip(kept.anchors, anchors))
    ):
        return kept.label_map
    rebuilding = _rebuild_in_progress()
    label_map = build()
    if rebuilding or _rebuild_in_progress():
        return label_map
    if (state.schema_boot_id, state.schema_version) != generation:
        return label_map
    for stale in [k for k, v in list(maps.items()) if v.generation != generation]:
        maps.pop(stale, None)
    maps[key] = _KeptLabelMap(generation, anchors, label_map)
    return label_map


@dataclass(frozen=True)
class CypherTranslation:
    """One admitted Cypher read, translated: everything a request needs short of its own values."""

    semantic_sql: str
    # $name parameters in the positional order the SQL binds them.
    ordered_params: tuple[str, ...]
    # Every $name the statement references: each request must supply all of them.
    param_names: tuple[str, ...]
    # RETURN alias -> graph kind, read by row assembly. Shared: never mutated.
    graph_vars: dict[str, Any]
    span_attrs: dict[str, str] | None = None

    def bind(self, params: dict[str, Any]) -> list[Any]:
        """This request's values in binding order; an unsupplied parameter is refused."""
        from provisa.cypher.params import bind_params

        bind_params(list(self.param_names), params)
        return [params.get(name) for name in self.ordered_params]


class TranslationRequest:
    """One request's view of the plan store: look the translation up, or record the one it builds."""

    def __init__(
        self,
        state: Any,
        role_id: str,
        *,
        surface: str,
        domain_access: list[str] | None,
        cypher: str,
        params: dict[str, Any],
    ) -> None:
        self._slot = PlanSlot(
            state,
            "cypher",
            role_id,
            surface,
            _access_key(domain_access),
            cypher,
            # The shape, not the values: which parameters are supplied and as what type.
            sorted((name, type(value).__name__) for name, value in params.items()),
            extra_anchors=(*_registry_anchors(state), state.relationships),
        )

    def cached(self) -> CypherTranslation | None:
        return self._slot.cached()

    def record(self, translation: CypherTranslation) -> None:
        self._slot.keep(translation)
