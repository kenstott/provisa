# Copyright (c) 2026 Kenneth Stott
# Canary: 45a7a653-85ec-4cc4-8c80-cf9f9ed6507c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL name generation: shortest unique within domain → regex rules → alias.

Names must be valid GraphQL identifiers: [_A-Za-z][_0-9A-Za-z]*.
"""

# Requirements: REQ-154, REQ-155, REQ-156, REQ-157, REQ-194, REQ-195, REQ-411, REQ-412, REQ-416

import functools
import re
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from types import SimpleNamespace

    from inflect import Word
    from inflect import engine as _Engine

    from provisa.core.models import Source

# inflect's import + engine() build costs ~0.8s. Lazy-load it so that cost is paid ONCE on first
# pluralization (schema generation), never at module import — keeps the desktop/demo cold start
# instant (import is otherwise ~1.6s, dominated by inflect). Behaviour is identical; just deferred.
_inflect: "_Engine | None" = None


def _engine() -> "_Engine":
    global _inflect
    if _inflect is None:
        import inflect

        _inflect = inflect.engine()
    return _inflect


# inflect applies the generic consonant+y -> ies rule to any word, but these are actor-reference
# particles (treated_by, created_by, owned_by), not countable nouns — "by" pluralizes to "bys",
# never "bies".
_PLURAL_OVERRIDES = {"by": "bys"}


_VERB_PREFIXES = frozenset(
    {
        "find",
        "get",
        "list",
        "create",
        "add",
        "update",
        "delete",
        "remove",
        "search",
        "fetch",
        "query",
        "retrieve",
        "read",
    }
)


def rel_field_name(target_field_name: str, cardinality: str) -> str:  # REQ-194, REQ-415
    """Build {noun}_{modifiers} relationship field name with library-based pluralization.

    Handles camelCase (OpenAPI operation IDs) and strips leading verb prefixes
    so findPetsByStatus → pet_by_status (many-to-one).
    """
    return apply_gql_name(_rel_words(target_field_name, cardinality))


def openapi_operation_alias(operation_id: str) -> str:  # REQ-601, REQ-194
    """The alias an OpenAPI virtual table is suggested under, from its ``operationId``: the
    leading verb stripped and the noun singular, in snake case (``findPetsByStatus`` is
    ``pet_by_status``). The words are :func:`rel_field_name`'s for a many-to-one edge."""
    return _rel_words(operation_id, "many-to-one")


def _rel_words(target_field_name: str, cardinality: str) -> str:
    """:func:`rel_field_name`'s words, snake-joined, before any convention is applied."""
    base = target_field_name.split("__", 1)[-1]
    # Normalise camelCase/PascalCase → snake_case before splitting
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", base)
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake).lower()
    parts = [p for p in snake.split("_") if p]
    # Strip leading verb prefixes (common in OpenAPI operation IDs)
    original_len = len(parts)
    while len(parts) > 1 and parts[0] in _VERB_PREFIXES:
        parts = parts[1:]
    verb_was_stripped = len(parts) < original_len
    # Compound noun (no verb stripped, multiple parts): last word is head noun
    if not verb_was_stripped and len(parts) > 1:
        noun, modifiers = parts[-1], parts[:-1]
    else:
        noun, modifiers = parts[0], parts[1:]
    eng = _engine()
    if cardinality == "one-to-many":
        if noun in _PLURAL_OVERRIDES:
            noun = _PLURAL_OVERRIDES[noun]
        else:
            singular = eng.singular_noun(cast("Word", noun))
            if singular is False:
                # noun is singular — pluralize it
                noun = eng.plural_noun(cast("Word", noun)) or noun
            elif singular.endswith("s") and not noun.endswith("ies"):
                # inflect returned a false singular ending in 's' (e.g. address→addres) — force plural
                noun = eng.plural_noun(cast("Word", noun)) or noun
            # else: genuinely plural (e.g. inquiries, orders) — leave as-is
    else:
        singular = eng.singular_noun(cast("Word", noun))
        if singular:
            noun = singular
    if not verb_was_stripped and len(parts) > 1:
        words = [*modifiers, noun]
    else:
        words = [noun, *modifiers]
    return "_".join(words)


def junction_field_name(  # REQ-1586
    label_source: str,
    cardinality: str,
    *,
    type_value: str | None = None,
    via_table_name: str | None = None,
    cypher_alias: str | None = None,
) -> str:
    """The relationship field name a junction-backed edge takes.

    One junction table backs several edges between the same pair of tables — the demo's pets are
    bonded pairs, littermates and enclosure-mates through one ``pet_companions`` row set. Named
    after the target table the way an FK edge is, all three become ``pets``: the GraphQL type keeps
    only the last field of that name, ``ctx.joins`` resolves ``?include=pets`` to whichever edge
    registered last, and a client rendering the list sees one name three times. The nomination that
    already distinguishes these edges on the Cypher side names the field here too.

    The three nominations are the ones the registry's CHECK constraints admit, so a missing or
    unknown one is a declaration that never should have been stored, not something to name around.
    """
    if label_source == "column":
        if not type_value:
            raise ValueError("Junction nominates a column but carries no via_type_value")
        label = type_value
    elif label_source == "table":
        if not via_table_name:
            raise ValueError("Junction nominates its table name, which is absent")
        label = via_table_name
    elif label_source == "fixed":
        if not cypher_alias:
            raise ValueError("Junction nominates a fixed label, which is absent")
        label = cypher_alias
    else:
        raise ValueError(f"Junction has no label source nomination: {label_source!r}")
    # A nominated label is prose ("bonded pair") or a Cypher type (BONDED_PAIR); rel_field_name
    # splits on underscores, so give it the words separated the way it expects.
    return rel_field_name(re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_"), cardinality)


def ontology_name(term_name: str) -> str | None:  # REQ-2163
    """The name a term gives its Class table or its Property column in the ontology schema: its
    words, lower-cased and joined by underscores ("customer account" is ``customer_account``).
    None when the term's name gives no identifier (no letters, or a digit first), since it can name
    no table or column."""
    name = re.sub(r"[^A-Za-z0-9]+", "_", term_name).strip("_").lower()
    return name if re.fullmatch(r"[a-z][a-z0-9_]*", name) else None


# Pure string -> string, and asked for every column of every table each role's schema and context
# name (a schema rebuild and a role's compile): a repeated name is answered from the memo.
@functools.lru_cache(maxsize=65536)
def _to_pascal_case(name: str) -> str:
    """Convert snake_case or kebab-case to PascalCase.

    A word that starts with a digit keeps the underscore before it: case cannot mark a boundary
    before a digit, so dropping it made ``foo_123`` and ``foo123`` one name and lost the boundary
    for every name converted back (``_to_snake_case``) -- a table registered as ``foo_123`` was
    published to SQL as ``foo123``.
    """
    parts = re.split(r"[_\-]+", name)
    result = []
    for p in parts:
        if not p:
            continue
        if result and p[0].isdigit():
            result.append("_")
        # If part already has internal uppercase (camelCase), just capitalize first letter
        if any(c.isupper() for c in p[1:]):
            result.append(p[0].upper() + p[1:])
        else:
            result.append(p.capitalize())
    return "".join(result)


def to_pascal_case(name: str) -> str:  # REQ-194
    """A name in pascal case (``active_users`` and ``activeUsers`` are ``ActiveUsers``), for a
    client-facing name outside the convention presets: a gRPC RPC, a GraphQL type name."""
    return _to_pascal_case(name)


@functools.lru_cache(maxsize=65536)
def _to_camel_case(name: str) -> str:
    """Convert snake_case or kebab-case to camelCase."""
    pascal = _to_pascal_case(name)
    if not pascal:
        return pascal
    return pascal[0].lower() + pascal[1:]


def _to_field_name(name: str) -> str:
    """Convert to valid GraphQL field name (snake_case, no hyphens)."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")


# Pure string -> string, and on every statement's path: governance builds a name for every
# registered table per request (REQ-1877), so a repeated name is answered from the memo.
@functools.lru_cache(maxsize=16384)
def domain_to_sql_name(domain_id: str) -> str:  # REQ-471
    """Normalize a domain ID to a valid SQL identifier (non-alphanumeric → underscore)."""
    return re.sub(r"[^a-zA-Z0-9]", "_", domain_id).strip("_")


def semantic_relation_names(domain_id: str, semantic: str) -> tuple[str, str]:  # REQ-641
    """The names a statement gives a registered table through its semantic name: qualified by
    its domain's SQL name, and bare."""
    return f"{domain_to_sql_name(domain_id)}.{semantic}", semantic


def stored_relation_names(domain_id: str, schema_name: str, table_name: str) -> list[str]:
    """The qualified names a statement gives a registered table through its stored table name:
    ``domain.table`` and ``schema.table`` (e.g. ``meta.registered_tables``,
    ``public.registered_tables``). Served and draft tables alike are named by this one rule —
    governance resolves a served table under these names (``build_governance_context``) and a
    draft is refused as draft under the same ones (REQ-1921)."""
    if not table_name:
        return []
    names = []
    if domain_id:
        names.append(f"{domain_to_sql_name(domain_id)}.{table_name}")
    if schema_name:
        names.append(f"{schema_name}.{table_name}")
    return names


def domain_gql_alias(domain_id: str, stored: str | None = None) -> str:  # REQ-471
    """Return stored alias, or compute first-letter acronym from domain id.

    Stored alias is used as-is (lowercase for ops, UPPER for type prefix).
    Computed default: first letter of each word segment, lowercase.
    e.g. 'sales_analytics' → 'sa', 'human-resources' → 'hr'.
    Returns '' for empty domain_id (no prefix).
    """
    if stored:
        return stored.lower()
    if not domain_id:
        return ""
    parts = re.split(r"[^a-zA-Z0-9]+", domain_id)
    acronym = "".join(p[0] for p in parts if p and p[0].isalpha())
    return acronym.lower() if acronym else domain_id[0].lower()


def domain_alias_map(domains: list[dict]) -> dict[str, str]:  # REQ-471
    """Each domain's alias (:func:`domain_gql_alias`), every one its own: a computed alias another
    domain's alias also is is the domain's own name instead, so no domain's tables are named, typed
    or grouped as another's. A stored alias is the domain's, as written. A system schema
    (ontology, ops) is named in the SDL by its own prefix (``SYSTEM_SCHEMA_GQL_PREFIX``, REQ-2163),
    never by an alias, so its computed alias is its own name and takes no model domain's: opioids
    keeps "o" beside ops."""
    aliases = {d["id"]: domain_gql_alias(d["id"], d.get("graphql_alias")) for d in domains}
    system = {
        d["id"]
        for d in domains
        if d["id"] in SYSTEM_SCHEMA_GQL_PREFIX and not d.get("graphql_alias")
    }
    taken = [a for domain_id, a in aliases.items() if a and domain_id not in system]
    return {
        d["id"]: (
            domain_to_sql_name(d["id"])
            if d["id"] in system
            or (not d.get("graphql_alias") and taken.count(aliases[d["id"]]) > 1)
            else aliases[d["id"]]
        )
        for d in domains
        if aliases[d["id"]]
    }


@functools.lru_cache(maxsize=16384)
def _to_snake_case(name: str) -> str:
    """Convert camelCase or PascalCase to snake_case."""
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def source_to_catalog(source_id: str) -> str:  # REQ-471
    """Convert a source ID to the engine catalog name (hyphens → underscores)."""
    return source_id.replace("-", "_")


#: The kinds of object an engine keeps to reach a source live, each named ``<kind>_<catalog>``:
#: a pg engine's foreign servers and staging schemas (postgres_fdw and the other FDWs, the
#: connector-pgwire bridge, file_fdw) and a ClickHouse engine's databases.
ATTACH_KINDS: tuple[str, ...] = (
    "fdw",
    "fdw_pgwire",
    "fdw_file",
    "fdw_codehost",  # REQ-2100: a built code host
    "ch",
    "ch_pgwire",
)


def engine_attach_name(kind: str, catalog: str) -> str:  # REQ-1266, REQ-1529
    """The name of an object an engine database keeps to reach a source live — its foreign
    server (and so its user mapping), and the schema its foreign tables are imported into —
    from the source's catalog name (:func:`org_prefixed_catalog`, the name the compiler emits).
    It carries the org and the environment as that catalog does, so two orgs, or two
    environments, keeping one engine database never share one. ``kind`` is the connector's
    prefix (one of :data:`ATTACH_KINDS`)."""
    if kind not in ATTACH_KINDS:
        raise ValueError(f"{kind!r} is not an attach kind ({', '.join(ATTACH_KINDS)})")
    return f"{kind}_{catalog}"


def attach_catalog(source: "Source | SimpleNamespace") -> str:  # REQ-1266, REQ-1529
    """The catalog name an engine attaches ``source`` under: carried by the attach view of a
    source its caller builds (``native_backend._walk_registry``, ``pg_backend``) from the org's
    ``source_catalogs``. A source that was not handed over that way has none (KeyError)."""
    return vars(source)["catalog"]


def physical_relation_name(catalog: str, schema: str, table: str) -> str:
    """The key a table's catalog-physical reference (``catalog.schema.table``, as a statement is
    lowered to, ``sql_rewrite._table_ref``) is resolved by in a governance table map."""
    return f"{catalog}.{schema}.{table}"


def table_id_of(table: Any, table_map: Mapping[str, int]) -> int | None:
    """The registered table a statement's reference names: by its catalog-physical name when it
    carries a catalog, else its ``schema.table`` or ``domain.table`` name, else its bare name.
    A view's body is lowered to catalog-physical names before it is read into a statement, and
    two sources may hold tables of one name (an EU and a US ``public.customers``): only the
    catalog tells them apart, and resolved without it the one reference names the other table."""
    if table.catalog and table.db:
        found = table_map.get(physical_relation_name(table.catalog, table.db, table.name))
        if found is not None:
            return found
    if table.db:
        found = table_map.get(f"{table.db}.{table.name}")
        if found is not None:
            return found
        return table_map.get(table.name)
    from provisa.compiler import temp_tables

    if table.name in temp_tables.names():
        # REQ-615: the session's own temporary table, ahead of a model table of its name -- as
        # PostgreSQL's pg_temp comes first, and as the validator reads it.
        return None
    return table_map.get(table.name)


def region_read_catalog(
    org_id: str, region_id: str, *, default_org: str, env: str | None
) -> str:  # REQ-1922, REQ-1266, REQ-1529
    """The name an engine reads another region's replicas store by — a Trino catalog, a DuckDB
    ATTACH alias, the stem of a pg engine's foreign server — carrying the org and the environment
    as a source's catalog name does (:func:`org_prefixed_catalog`)."""
    return org_prefixed_catalog(org_id, f"region_{region_id}", default_org=default_org, env=env)


def live_view_schema(catalog: str, schema: str) -> str:  # REQ-1730
    """The one schema segment a catalog-incapable engine addresses ``catalog.schema`` under
    (:func:`provisa.compiler.sql_rewrite.fold_catalog_into_schema`), and so the schema the engine
    keeps a source's live view in."""
    return f"{catalog}_{schema}"


def org_prefixed_catalog(
    org_id: str, base_catalog: str, *, default_org: str, env: str | None = None
) -> str:  # REQ-1266, REQ-1529
    """Namespace an org-scoped source's engine catalog so two orgs seeded with
    identical source ids don't collide in the one global engine catalog namespace.

    The bootstrap/default org keeps bare names, so single-org deployments and every
    existing ``source_to_catalog`` call site stay byte-identical; any other org gets
    an ``org_<id>__`` prefix. System catalogs (provisa-admin/otel) and fixed-warehouse
    catalogs (BigQuery/Fabric/Synapse — one physical catalog shared across orgs) are
    NOT org-scoped and must never be passed here.

    AN ENVIRONMENT NAMESPACES IT TOO (REQ-1529), for the same reason an org does and with more
    force: a branch resolves its own bindings, so the same source id in a branch may point at a
    different host than it does in the base. The catalog namespace is the one shared coordinator's,
    so a branch registering under the base's name would not shadow the base's catalog for itself —
    it would REPLACE it, and the base would afterwards be querying the branch's database. prod and
    ``None`` give the pre-environment name, which is what keeps an org holding only prod
    byte-identical; anything else is prefixed even in the default org, because a branch of the
    default org collides with the default org's own bare name.

    The ``_env_`` infix matches ``org_schema`` deliberately: REQ-1309 forbids an underscore in an
    org id, so the first one after ``org_`` splits the name into exactly one org and one
    environment however either is named.
    """
    from provisa.core.environments import PROD

    if env is not None and env != PROD:
        return f"org_{org_id}_env_{env}__{base_catalog}"
    if org_id == default_org:
        return base_catalog
    return f"org_{org_id}__{base_catalog}"


def is_own_catalog(catalog: str, source_id: str) -> bool:  # REQ-194
    """Whether ``catalog`` is the one named after ``source_id`` (:func:`source_to_catalog`, in the
    forms :func:`org_prefixed_catalog` gives it), so the source holds that address alone. Otherwise
    the source is pinned to a catalog it shares by design: the control plane's, a materialize
    store's or a fixed warehouse's."""
    base = source_to_catalog(source_id)
    return catalog == base or (catalog.startswith("org_") and catalog.endswith(f"__{base}"))


# REQ-195: Hasura v2 / DDN literals map to internal presets.
#   hasura-default  → snake_case   graphql-default → camelCase
#   graphql (DDN namingConvention) → camelCase
_CONVENTION_ALIASES = {
    "hasura-default": "hasura_graphql",
    "graphql-default": "apollo_graphql",
    "graphql": "apollo_graphql",
}


def normalize_convention(convention: str) -> str:  # REQ-195, REQ-416
    """Resolve a Hasura/DDN literal convention name to its internal preset."""
    return _CONVENTION_ALIASES.get(convention, convention)


#: REQ-416: the naming conventions a table, a source or the deployment may name, in the order the
#: admin offers them; VALID_CONVENTIONS adds the Hasura/DDN literals that resolve to them.
PRESET_CONVENTIONS: tuple[str, ...] = ("snake", "hasura_graphql", "apollo_graphql")

VALID_CONVENTIONS = frozenset(set(PRESET_CONVENTIONS) | set(_CONVENTION_ALIASES))


def stored_convention(convention: str | None) -> str | None:  # REQ-195, REQ-416
    """The preset ``convention`` is kept as (None: inherit): a Hasura/DDN spelling is taken as
    input and stored as the preset it names, so every stored convention is one of
    PRESET_CONVENTIONS. Raises ValueError for any other name."""
    return None if convention is None else preset_convention(convention)


def preset_convention(convention: str) -> str:  # REQ-195, REQ-416
    """:func:`stored_convention` of a named convention."""
    preset = normalize_convention(convention)
    if preset not in PRESET_CONVENTIONS:
        raise ValueError(
            f"unknown naming convention {convention!r}; expected one of {sorted(VALID_CONVENTIONS)}"
        )
    return preset


def validation_error_for_convention(convention: str) -> str | None:  # REQ-416
    """REQ-416: return an error message if `convention` is not a valid preset, else None.

    Only the presets (and their literal aliases in VALID_CONVENTIONS) are accepted; free-form
    strings must be rejected before `configure` is called on the naming-update path.
    """
    if convention in VALID_CONVENTIONS:
        return None
    valid = ", ".join(sorted(VALID_CONVENTIONS))
    return f"Invalid naming convention {convention!r}. Valid conventions: {valid}"


def _canonical_convention(convention: str) -> str:
    """Resolve preset convention to field/column naming form."""
    convention = normalize_convention(convention)
    # A name outside the presets is a caller bug, not a case to guess at: the old `else camelCase`
    # turned a mistyped convention into a silent no-op rename in whichever direction happened to
    # match, which is how `apply_convention(name, "snake_case")` (the *output form*, not a preset)
    # read as apollo and left camelCase names untouched on the SQL plane.
    if convention not in VALID_CONVENTIONS:
        raise ValueError(
            f"unknown naming convention {convention!r}; expected one of {sorted(VALID_CONVENTIONS)}"
        )
    # REQ-194: hasura_graphql is snake_case; apollo_graphql is camelCase.
    if convention in ("snake", "hasura_graphql"):
        return "snake_case"
    return "camelCase"  # apollo_graphql


def mutation_style(convention: str) -> str:  # REQ-411, REQ-412
    """Return 'snake' or 'camel' mutation prefix style for a given convention."""
    if normalize_convention(convention) in ("snake", "hasura_graphql"):
        return "snake"
    return "camel"  # apollo_graphql


_gql_convention: str = "apollo_graphql"
_sql_convention: str = "snake"


def configure(gql: str = "apollo_graphql", sql: str = "snake") -> None:  # REQ-471
    global _gql_convention, _sql_convention
    _gql_convention = normalize_convention(gql)
    _sql_convention = normalize_convention(sql)


def active_gql_convention() -> str:  # REQ-471
    return _gql_convention


def active_sql_form() -> str:  # REQ-2190
    """The output form of the SQL convention in force, as :func:`_canonical_convention` names it."""
    return _canonical_convention(_sql_convention)


_GQL_NAME_RE = re.compile(r"^[_A-Za-z][_0-9A-Za-z]*$")


def apply_gql_name(name: str, override: str | None = None) -> str:  # REQ-194, REQ-411, REQ-412
    return _gql_name(name, override or _gql_convention)


@functools.lru_cache(maxsize=65536)
def _gql_name(name: str, convention: str) -> str:
    """:func:`apply_gql_name` under ``convention``: pure, so a repeated name is answered from the
    memo (see :func:`_to_pascal_case`)."""
    converted = apply_convention(name, convention)
    if _GQL_NAME_RE.match(converted):
        return converted
    # apply_convention only re-cases an already-valid identifier — a raw external name (a CSV/API
    # column header verbatim, e.g. "1844YearsDosesAdministered") can still violate the GraphQL
    # Name grammar after conversion: any non [_0-9A-Za-z] character, or (unchanged by casing) a
    # leading digit. This module is the sole naming authority (REQ-471) — every caller building a
    # GraphQL field/enum-value/type name from a physical name goes through here, so the guarantee
    # belongs here, not at each call site. Confirmed live: schema_gen.py's distinct_on enum builder
    # crashed GraphQLEnumType's own name assertion on exactly this input, unrecovered, taking the
    # whole schema (and therefore every org's whole app) down at startup.
    sanitized = re.sub(r"[^0-9A-Za-z_]", "_", converted)
    if not sanitized or sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def apply_sql_name(name: str, override: str | None = None) -> str:  # REQ-194
    return apply_convention(name, override or _sql_convention)


def apply_cql_label(name: str) -> str:  # REQ-471
    """Cypher node/relationship label — always PascalCase."""
    return _to_pascal_case(name)


def apply_cql_property(name: str) -> str:  # REQ-471
    """Cypher property key — follows the configured GQL convention."""
    return apply_gql_name(name)


@functools.lru_cache(maxsize=65536)
def apply_convention(name: str, convention: str) -> str:  # REQ-471
    """Apply a naming convention preset to a name. Returns name unchanged when already correct."""
    canon = _canonical_convention(convention)
    if canon == "snake_case":
        return _to_snake_case(name)
    if canon == "camelCase":
        return _to_camel_case(name)
    return name


def _apply_naming_rules(name: str, rules: list[dict]) -> str:
    """Apply regex naming rules in order."""
    for rule in rules:
        name = re.sub(rule["pattern"], rule["replacement"], name)
    return name


def _shortest_unique(name: str, all_names: list[str], qualifiers: list[str]) -> str:
    """Find shortest unique name within a set.

    If `name` is unique in `all_names`, return it. Otherwise, prepend qualifiers
    one at a time until unique.

    Args:
        name: base table name (e.g., "orders")
        all_names: all table names in the domain
        qualifiers: ordered prefixes to try (e.g., [schema_name, source_id])
    """
    if all_names.count(name) <= 1:
        return name
    for qualifier in qualifiers:
        candidate = f"{qualifier}_{name}"
        if candidate not in all_names:
            return candidate
    raise ValueError(
        f"Cannot generate unique name for {name!r} within domain. "
        f"All qualifier combinations exhausted."
    )


def _apply_table_convention(name: str, convention: str) -> str:
    """Apply a naming convention preset to a table/field name."""
    safe = _to_field_name(name)
    canon = _canonical_convention(convention)
    if canon == "camelCase":
        return _to_camel_case(safe)
    return safe  # snake_case


class SqlAddressTaken(ValueError):  # REQ-1933
    """A table's SQL address (``domain.table``) is already another registered table's. Two
    tables never share one: nothing would decide which of them a statement reads."""

    def __init__(self, domain_id: str, address: str, holder: str, newcomer: str) -> None:
        self.domain_id = domain_id
        self.address = address
        self.holder = holder
        self.newcomer = newcomer
        super().__init__(
            f"{newcomer} would be {domain_to_sql_name(domain_id)}.{address}, which is already "
            f"{holder}'s address in domain {domain_id!r}. Give it a different name or alias, or "
            "register it in another domain."
        )


def table_sql_address(
    table_name: str, alias: str | None, naming_rules: list[dict], convention: str | None = None
) -> str:  # REQ-1933
    """The name a table is addressed by within its domain on the SQL plane: its alias, else its
    name under the naming rules, spelled as the compiler spells it
    (``sql_rewrite.semantic_table_name``)."""
    if alias:
        return apply_sql_name(alias)
    named = _apply_naming_rules(table_name, naming_rules)
    return apply_sql_name(_apply_table_convention(named, convention or active_gql_convention()))


def refuse_taken_sql_addresses(tables: "list[dict]", naming_rules: list[dict]) -> None:  # REQ-1933
    """Raise :class:`SqlAddressTaken` for the first table whose SQL address an earlier one in
    ``tables`` already has in the same domain. Each entry carries ``domain_id``, ``source_id``,
    ``schema_name``, ``table_name`` and ``alias`` (and may carry ``gql_naming_convention``)."""
    held: dict[tuple[str, str], str] = {}
    for t in tables:
        address = table_sql_address(
            t["table_name"], t.get("alias"), naming_rules, t.get("gql_naming_convention")
        )
        who = f"{t['source_id']}.{t['schema_name']}.{t['table_name']}"
        key = (t["domain_id"], address)
        if key in held and held[key] != who:
            raise SqlAddressTaken(t["domain_id"], address, held[key], who)
        held.setdefault(key, who)


def generate_name(  # REQ-154, REQ-155, REQ-157, REQ-194, REQ-1933
    table_name: str,
    naming_rules: list[dict],
    alias: str | None = None,
    convention: str = "apollo_graphql",
) -> str:
    """The GraphQL-safe name of a table: its alias, else its name under the naming rules, in the
    convention's casing. A name is never qualified to make it unique: two tables of one address in
    a domain are refused (:func:`refuse_taken_sql_addresses`, REQ-1933)."""
    if alias:
        return _apply_table_convention(alias, convention)
    result = _apply_table_convention(_apply_naming_rules(table_name, naming_rules), convention)
    if not result:
        raise ValueError(
            f"Naming rules produced empty name for table {table_name!r}. "
            f"Check naming rule configuration."
        )
    return result


class LabelClash(ValueError):  # REQ-572
    """A table's Cypher label is a domain's. A label never means both: a statement naming it
    could be reading the table or the domain's tables."""

    def __init__(self, label: str, domain_id: str, table: str) -> None:
        self.label = label
        self.domain_id = domain_id
        self.table = table
        super().__init__(
            f"Table {table} and domain {domain_id!r} would both have the Cypher label {label}. "
            "A label never names both a domain and a table: give the table an alias, or name the "
            "domain differently."
        )


def refuse_label_clash(
    tables: "list[dict]", domain_ids: "list[str]", naming_rules: list[dict]
) -> None:  # REQ-572
    """Raise :class:`LabelClash` for the first table whose Cypher label is one of the domains'.
    Each table carries ``source_id``, ``schema_name``, ``table_name`` and ``alias`` (and may carry
    ``gql_naming_convention``)."""
    by_label = {domain_cypher_label(d): d for d in domain_ids if d}
    for t in tables:
        label = table_cypher_label(
            t["table_name"], t.get("alias"), naming_rules, t.get("gql_naming_convention")
        )
        if label in by_label:
            raise LabelClash(
                label, by_label[label], f"{t['source_id']}.{t['schema_name']}.{t['table_name']}"
            )


def table_cypher_label(
    table_name: str, alias: str | None, naming_rules: list[dict], convention: str | None = None
) -> str:  # REQ-572
    """The Cypher label a table answers to, without its domain: its GraphQL field name
    (:func:`generate_name`) in the label convention, the part after a domain qualifier when the
    name carries one -- as the label map derives it (``gql.label_map._split_cypher_labels``)."""
    field = generate_name(
        table_name, naming_rules, alias=alias, convention=convention or active_gql_convention()
    )
    return apply_cql_label(field.split("__", 1)[1] if "__" in field else field)


def domain_cypher_label(domain_id: str) -> str:  # REQ-572
    """The Cypher label a domain answers to (``gql.label_map``: the domain part of a label)."""
    return apply_cql_label(domain_id)


#: REQ-2163 (amended 2026-10-27): the prefix every root field and type a generated system schema
#: gives carries in the GraphQL SDL, whether or not domains prefix their fields, so neither schema
#: ever shares a name with a model table's field or type. Its Cypher labels and SQL relations are
#: unprefixed.
SYSTEM_SCHEMA_GQL_PREFIX: dict[str, str] = {"ontology": "on", "ops": "op"}


def system_schema_names(domain_id: str, field_name: str) -> tuple[str, str]:  # REQ-2163
    """The root field and type names of a ``domain_id`` system schema's table whose own field name
    is ``field_name``: ``on_terms`` and ``ON_Terms``, its prefix upper-cased in the type as a
    domain's is (``M__RegisteredTables``)."""
    prefix = SYSTEM_SCHEMA_GQL_PREFIX[domain_id]
    return f"{prefix}_{field_name}", f"{prefix.upper()}_{to_type_name(field_name)}"


def unprefixed_field_name(field_name: str, domain_id: str) -> str:  # REQ-2163
    """A table's field name without its system schema's SDL prefix (``on_terms`` is ``terms``);
    any other domain's field name as it is."""
    prefix = SYSTEM_SCHEMA_GQL_PREFIX.get(domain_id)
    if prefix is None:
        return field_name
    if not field_name.startswith(f"{prefix}_"):
        raise ValueError(f"the {domain_id} table field {field_name!r} lacks its prefix {prefix}_")
    return field_name[len(prefix) + 1 :]


def bare_field_name(field_name: str, domain_id: str) -> str:  # REQ-471, REQ-2163
    """A table's root field name without its domain's prefix (``sa__orders`` is ``orders``) or its
    system schema's (``on_terms`` is ``terms``)."""
    if domain_id in SYSTEM_SCHEMA_GQL_PREFIX:
        return unprefixed_field_name(field_name, domain_id)
    return field_name.split("__", 1)[1] if "__" in field_name else field_name


def table_type_name(field_name: str, domain_id: str) -> str:  # REQ-471, REQ-2163
    """The GraphQL type name of the ``domain_id`` table whose root field is ``field_name``."""
    if domain_id in SYSTEM_SCHEMA_GQL_PREFIX:
        return system_schema_names(domain_id, unprefixed_field_name(field_name, domain_id))[1]
    return to_type_name(field_name)


def to_type_name(field_name: str) -> str:  # REQ-471
    """Convert a field name to a GraphQL type name (PascalCase).

    Handles camelCase input: capitalizes first letter only.
    Preserves the domain separator: sa__userByName → Sa_UserByName
    """
    if "__" in field_name:
        prefix, rest = field_name.split("__", 1)
        rest_pascal = (rest[0].upper() + rest[1:]) if rest else ""
        return prefix.upper() + "__" + rest_pascal
    return (field_name[0].upper() + field_name[1:]) if field_name else ""


# ── REQ-194 (amended 2026-11-07): every client-facing name kind has its rule here ──────────────


def mutation_field_name(op: str, field_name: str, convention: str) -> str:  # REQ-194, REQ-411
    """The GraphQL mutation field for ``op`` (insert, update, delete, ...) on the table whose root
    field is ``field_name``, in the table's convention: ``insertOrders`` (camel) or
    ``insert_orders`` (snake). A domain-prefixed field keeps its prefix in front:
    ``sa__insertOrders`` / ``sa__insert_orders``."""
    snake = mutation_style(convention) == "snake"
    prefix, table = field_name.split("__", 1) if "__" in field_name else ("", field_name)
    name = f"{op}_{table}" if snake else f"{op}{table[:1].upper()}{table[1:]}"
    return prefixed_field_name(prefix, name) if prefix else name


def prefixed_field_name(prefix: str, field_name: str) -> str:  # REQ-471, REQ-194
    """A table's root field under its domain's prefix (an alias or the domain's SQL name):
    ``sa`` and ``orders`` are ``sa__orders``. The inverse is :func:`bare_field_name`."""
    return f"{prefix}__{field_name}"


def prefixed_type_name(prefix: str, field_name: str) -> str:  # REQ-471, REQ-194
    """The GraphQL type of a table whose unprefixed field is ``field_name`` under ``prefix``:
    ``sa`` and ``orders`` are ``SA__Orders``."""
    return to_type_name(prefixed_field_name(prefix, field_name))


#: The root-field variants a table's root field has besides itself (REQ-196, REQ-197, REQ-198).
ROOT_FIELD_VARIANTS: tuple[str, ...] = ("aggregate", "group_by", "connection")

_CAMEL_VARIANT = {"aggregate": "Aggregate", "group_by": "GroupBy"}


def root_field_variant(field_name: str, kind: str, convention: str) -> str:  # REQ-194
    """The root field of a table's ``kind`` variant under the table's own convention:
    ``ordersAggregate`` / ``orders_aggregate``, ``ordersGroupBy`` / ``orders_group_by``. A
    connection is ``orders_connection`` under every convention, as it has always been served."""
    if kind not in ROOT_FIELD_VARIANTS:
        raise ValueError(f"{kind!r} is not a root-field variant ({', '.join(ROOT_FIELD_VARIANTS)})")
    if kind != "connection" and _canonical_convention(convention) == "camelCase":
        return f"{field_name}{_CAMEL_VARIANT[kind]}"
    return f"{field_name}_{kind}"


_NON_GQL_CHAR = re.compile(r"[^a-zA-Z0-9_]")


def gql_enum_type_name(enum_name: str) -> str:  # REQ-221, REQ-194
    """The GraphQL enum type of a source enum: its name with each character GraphQL does not
    take as ``_``, ``e_`` before a leading digit, and ``_Enum`` after (``order_status_Enum``)."""
    cleaned = _NON_GQL_CHAR.sub("_", enum_name)
    if cleaned and cleaned[0].isdigit():
        cleaned = f"e_{cleaned}"
    return f"{cleaned}_Enum"


def gql_enum_value_name(label: str) -> str:  # REQ-221, REQ-194
    """The GraphQL value of an enum label: upper case, each character GraphQL does not take as
    ``_``, ``V_`` before a leading digit or in place of an empty label."""
    cleaned = _NON_GQL_CHAR.sub("_", label)
    if not cleaned or cleaned[0].isdigit():
        cleaned = f"V_{cleaned}"
    return cleaned.upper()


def cql_rel_type(field_name: str, cardinality: str | None = None) -> str:  # REQ-194, REQ-471
    """The Cypher relationship type of the edge a relationship field names: ``IS_`` for a
    many-to-one or one-to-one edge, ``HAS_`` otherwise, then the field's words in UPPER_SNAKE
    (``animalBreed`` is ``IS_ANIMAL_BREED``, ``tableColumns`` is ``HAS_TABLE_COLUMNS``,
    ``_queries`` is ``HAS_QUERIES``)."""
    words = re.sub(r"([a-z])([A-Z])", r"\1_\2", field_name).upper().lstrip("_")
    verb = "IS_" if cardinality in ("many-to-one", "one-to-one") else "HAS_"
    return f"{verb}{words}"


def cql_junction_rel_type(label: str) -> str:  # REQ-1586, REQ-194
    """The Cypher relationship type a junction's nominated label takes: its words in
    UPPER_SNAKE (``bonded pair`` and ``bondedPair`` are ``BONDED_PAIR``)."""
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", label)
    return re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9]+", "_", spaced)).strip("_").upper()


def proto_type_name(gql_type_name: str) -> str:  # REQ-045, REQ-194
    """The proto3 message of a GraphQL type: its domain prefix, if any, capitalised and joined
    to the rest (``PS__Pets`` is ``PsPets``)."""
    if "__" in gql_type_name:
        prefix, rest = gql_type_name.split("__", 1)
        return prefix.capitalize() + rest
    return gql_type_name


def proto_field_name(gql_field_name: str) -> str:  # REQ-045, REQ-194
    """The proto3 field of a GraphQL field: its domain separator as one underscore
    (``ps__pets`` is ``ps_pets``)."""
    return gql_field_name.replace("__", "_")


def proto_command_name(fn_name: str) -> str:  # REQ-1156, REQ-194
    """The base name of a command's RPC and request message: ``active_users`` and
    ``activeUsers`` are ``ActiveUsers``."""
    return _to_pascal_case(fn_name.replace("__", "_"))


#: The messages a table's proto type has, by suffix (proto_gen): its row, batch, filter,
#: parameters, request, input, and its aggregate and group-by messages.
PROTO_TABLE_MESSAGE_SUFFIXES: tuple[str, ...] = (
    "",
    "Batch",
    "Filter",
    "Params",
    "Request",
    "Input",
    "AggregateRequest",
    "AggregateResult",
    "GroupByRequest",
    "GroupByOrder",
    "GroupByRow",
)


def proto_table_rpcs(proto_type: str) -> dict[str, str]:  # REQ-045, REQ-194
    """The RPCs a table's proto type is served by, by kind: ``query`` (``Query{T}``), ``batch``,
    ``aggregate``, ``group_by`` and ``insert`` (``Insert{T}``)."""
    return {
        "query": f"Query{proto_type}",
        "batch": f"Query{proto_type}Batch",
        "aggregate": f"Query{proto_type}Aggregate",
        "group_by": f"Query{proto_type}GroupBy",
        "insert": f"Insert{proto_type}",
    }


def proto_command_rpc(fn_name: str) -> str:  # REQ-1156, REQ-194
    """A command's RPC: ``Call{Command}``."""
    return f"Call{proto_command_name(fn_name)}"


def pg_pk_constraint_name(table_sql_name: str) -> str:  # REQ-194
    """The primary-key constraint a table is served with through pgwire and Flight metadata."""
    return f"pk_{table_sql_name}"


def pg_fk_constraint_name(table_sql_name: str, key: str) -> str:  # REQ-194
    """A foreign-key constraint of a table, by what tells it apart: the referenced table (a
    composite key) or the referencing column (a single-column key): ``fk_orders__customer_id``."""
    return f"fk_{table_sql_name}__{key}"


def pg_meta_fk_constraint_name(table_sql_name: str) -> str:  # REQ-194
    """The constraint linking a table to its catalog row in meta: ``fk_orders___meta``."""
    return f"fk_{table_sql_name}___meta"


# ── REQ-194 (amended 2026-11-07, NAME SUGGESTIONS COME FROM THE AUTHORITY) ─────────────────────


def suggest_reverse_rel_alias(
    graphql_alias: str | None, cypher_alias: str | None, convention: str
) -> tuple[str, str]:  # REQ-194
    """The GraphQL and Cypher aliases suggested for the reverse of a relationship, from its
    own: a ``By``/``Of`` (``_BY``/``_OF``) form loses it, any other gains ``By`` (``_BY``). With
    no GraphQL alias, the Cypher suggestion's words name it under ``convention``."""
    cql = ""
    if cypher_alias:
        cql = cypher_alias[:-3] if cypher_alias.endswith(("_BY", "_OF")) else f"{cypher_alias}_BY"
    if graphql_alias:
        gql = graphql_alias[:-2] if graphql_alias.endswith(("By", "Of")) else f"{graphql_alias}By"
    else:
        gql = apply_convention(cql.lower(), convention) if cql else ""
    return gql, cql


def suggest_table_alias(
    table_name: str, source_id: str, taken: "Callable[[str], bool]", convention: str
) -> str:  # REQ-194, REQ-1831
    """The alias suggested for ``table_name`` registered from ``source_id``: its name under the
    SQL ``convention``; when another source's table is already called that, the source's id in
    front; when that is taken too, a counter after it."""
    candidate = apply_convention(table_name, convention)
    if not taken(candidate):
        return candidate
    # The source's catalog name, not its id: a hyphen (``pg-b``) is no SQL identifier character.
    source = source_to_catalog(source_id)
    prefixed = apply_convention(f"{source}_{table_name}", convention)
    if not taken(prefixed):
        return prefixed
    n = 1
    while taken(apply_convention(f"{source}_{table_name}_{n}", convention)):
        n += 1
    return apply_convention(f"{source}_{table_name}_{n}", convention)


def suggest_column_alias(column: str) -> str | None:  # REQ-194
    """The alias suggested for a newly registered column: its snake-case name, or None where
    the column is already named so."""
    alias = apply_convention(column, "snake")
    return alias if alias != column else None


def suggest_metric_name(fact: str, column: str, agg: str) -> str:  # REQ-194, REQ-2188
    """The governed metric proposed for an aggregate of a fact's measure: an identifier in
    snake case (``orders_amount_sum``)."""
    return apply_gql_name(f"{fact}_{column}_{agg}", "snake")


def suggest_junction_name(left_table: str, right_table: str) -> str:  # REQ-194, REQ-1586
    """The junction table proposed for linking two tables (a measureless fact): both names in
    snake case, joined (``pets_owners``)."""
    return apply_gql_name(f"{left_table}_{right_table}", "snake")


#: REQ-194 (CLASH POLICY): how the attribute that tells two clashing objects apart names the one
#: renamed, per object kind.
_DISAMBIGUATION_FORMS: dict[str, str] = {
    "relationship": "{name}_by_{attribute}",
    "table": "{attribute}_{name}",
    "view": "{attribute}_{name}",
    "junction": "{name}_{attribute}",
}


def disambiguation_candidate(kind: str, name: str, attribute: str | None) -> str | None:
    """The name an object of ``kind`` called ``name`` is given from its distinguishing
    ``attribute`` under the disambiguate policy, in snake case; None when the kind has no
    attribute rule or no attribute (the counter applies)."""
    form = _DISAMBIGUATION_FORMS.get(kind)
    if form is None or not attribute:
        return None
    return apply_convention(
        _to_field_name(form.format(name=name, attribute=source_to_catalog(attribute))), "snake"
    )


def disambiguate(
    kind: str, name: str, attribute: str | None, taken: "Callable[[str], bool]"
) -> str:  # REQ-194
    """REQ-194 (CLASH POLICY): a distinct name for an object of ``kind`` whose ``name`` another
    object has: derived from the ``attribute`` that tells them apart
    (:func:`disambiguation_candidate`), and a counter after it only when there is no such
    attribute or the derived name is taken too."""
    candidate = disambiguation_candidate(kind, name, attribute)
    if candidate is not None and not taken(candidate):
        return candidate
    base = candidate or name
    n = 2
    while taken(f"{base}_{n}"):
        n += 1
    return f"{base}_{n}"


# ── REQ-194 (amended 2026-11-07, THE NAMES TABLE FINDS NAMING PROBLEMS) ────────────────────────

#: The surfaces a name is served on, each a column of the names table.
SURFACES: tuple[str, ...] = ("sql", "graphql", "cypher", "proto", "pg_constraint")

#: PostgreSQL's reserved key words (Appendix C, "reserved" and "reserved (can be function or
#: type)"): an unquoted identifier never takes one.
SQL_RESERVED: frozenset[str] = frozenset(
    """all analyse analyze and any array as asc asymmetric authorization binary both case cast
    check collate collation column concurrently constraint create cross current_catalog
    current_date current_role current_schema current_time current_timestamp current_user default
    deferrable desc distinct do else end except false fetch for foreign freeze from full grant
    group having ilike in initially inner intersect into is isnull join lateral leading left like
    limit localtime localtimestamp natural not notnull null offset on only or order outer overlaps
    placing primary references returning right select session_user similar some symmetric
    system_user table tablesample then to trailing true union unique user using variadic verbose
    when where window with""".split()
)

#: Cypher's reserved words (openCypher): a label, type or property taking one is back-quoted.
CYPHER_RESERVED: frozenset[str] = frozenset(
    """all asc ascending by create delete desc descending detach exists limit match merge on
    optional order remove return set skip where with union unwind and as contains distinct ends in
    is not or starts xor case else end then when constraint do for require unique mandatory scalar
    of add drop false null true call yield""".split()
)

#: The type names every GraphQL schema already holds: a table type taking one is refused by the
#: schema, and a field named with the introspection prefix is reserved.
GRAPHQL_RESERVED: frozenset[str] = frozenset(
    {"Query", "Mutation", "Subscription", "String", "Int", "Float", "Boolean", "ID"}
)

#: The longest identifier a surface takes: PostgreSQL's NAMEDATALEN - 1 for SQL names and
#: constraint names served through pgwire.
SURFACE_LENGTH_LIMITS: dict[str, int] = {"sql": 63, "pg_constraint": 63}

_SURFACE_IDENTIFIER: dict[str, re.Pattern[str]] = {
    "sql": re.compile(r"^[a-z_][a-z0-9_$]*$"),
    "graphql": re.compile(r"^[_A-Za-z][_0-9A-Za-z]*$"),
    "cypher": re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$"),
    "proto": re.compile(r"^[A-Za-z][A-Za-z0-9_]*$"),
    "pg_constraint": re.compile(r"^[a-z_][a-z0-9_$]*$"),
}

_RESERVED_ON: dict[str, frozenset[str]] = {
    "sql": SQL_RESERVED,
    "pg_constraint": SQL_RESERVED,
    "cypher": CYPHER_RESERVED,
    "graphql": GRAPHQL_RESERVED,
}


def name_problems(name: str, surface: str) -> list[tuple[str, str]]:  # REQ-194
    """Each problem ``name`` has on ``surface``, as (code, message): ``reserved`` (a reserved
    word there), ``leading_digit``, ``illegal_char`` (a character the surface does not take
    unquoted), ``too_long`` (over the surface's length limit). Empty when none."""
    if surface not in SURFACES:
        raise ValueError(f"{surface!r} is not a surface ({', '.join(SURFACES)})")
    found: list[tuple[str, str]] = []
    reserved = _RESERVED_ON.get(surface, frozenset())
    if (name.lower() if surface != "graphql" else name) in reserved:
        found.append(("reserved", f"{name!r} is a reserved word on {surface}: it must be quoted"))
    if surface == "graphql" and name.startswith("__"):
        found.append(("reserved", f"{name!r} takes GraphQL's introspection prefix '__'"))
    if name[:1].isdigit():
        found.append(
            ("leading_digit", f"{name!r} starts with a digit: it must be quoted on {surface}")
        )
    elif not _SURFACE_IDENTIFIER[surface].match(name):
        found.append(
            ("illegal_char", f"{name!r} holds a character {surface} does not take unquoted")
        )
    limit = SURFACE_LENGTH_LIMITS.get(surface)
    if limit is not None and len(name) > limit:
        found.append(
            ("too_long", f"{name!r} is {len(name)} characters, over {surface}'s limit of {limit}")
        )
    return found


def conversion_problems(physical: str) -> list[tuple[str, str]]:  # REQ-194
    """Each way converting the physical name ``physical`` loses information or reads badly, as
    (code, message): ``lossy`` (its SQL and GraphQL names do not convert back into each other),
    ``reads_badly`` (a run of single-letter words, as ``h_t_t_p``), ``long_prefix_chain``
    (three or more ``__`` qualifiers)."""
    found: list[tuple[str, str]] = []
    sql = apply_convention(physical, "snake")
    gql = apply_convention(physical, "apollo_graphql")
    if apply_convention(gql, "snake") != sql or apply_convention(sql, "apollo_graphql") != gql:
        found.append(
            (
                "lossy",
                f"{physical!r} is {sql!r} in SQL and {gql!r} in GraphQL, which do not "
                "convert back into each other: set an alias",
            )
        )
    words = sql.split("_")
    run = longest = 0
    for w in words:
        run = run + 1 if len(w) == 1 and w.isalpha() else 0
        longest = max(longest, run)
    if longest >= 3:
        found.append(("reads_badly", f"{physical!r} converts to {sql!r}: set an alias"))
    if physical.count("__") >= 3:
        found.append(
            ("long_prefix_chain", f"{physical!r} chains {physical.count('__')} '__' qualifiers")
        )
    return found
