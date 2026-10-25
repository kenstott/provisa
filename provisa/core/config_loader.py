# Copyright (c) 2026 Kenneth Stott
# Canary: 998f7261-e877-4341-a621-634d0c8011ff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Config loader: YAML → validate → resolve secrets → upsert PG → create the engine catalogs."""

# Requirements: REQ-012, REQ-013, REQ-016, REQ-250, REQ-251, REQ-275, REQ-282, REQ-283, REQ-285
# complexity-gate: allow-ble=6 reason="per-source config registration is best-effort: source-driver register, OpenAPI spec load, SQLite migration post-step, OpenAPI cache, a MongoDB change-stream check (an unreachable server), and CBO analyze each log their own failure and continue, so one bad source never fails the whole config load"

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import AliasChoices, BaseModel

import yaml
from sqlalchemy import insert, select, update

from provisa.core.repositories.identity import TableInIdentity
from provisa.core.models import (
    ControlPlaneConfig,
    Domain,
    STORED_MODEL,
    ProvisaConfig,
    Source,
    Table,
    TagAssignment,
)
from provisa.core import domain_policy
from provisa.core.schema_org import (
    glossary_terms,
    model_seed,
    naming_rules,
    registered_tables,
)
from provisa.api_source.openapi_endpoint import normalize_op_id
from provisa.core.secrets import resolve_secrets
from provisa.security.rights import SYSTEM_ROLE_IDS
from provisa.core.repositories import (
    source as source_repo,
    domain as domain_repo,
    data_product as data_product_repo,
    glossary as glossary_repo,
    table as table_repo,
    metric as metric_repo,
    relationship as rel_repo,
    role as role_repo,
    rls as rls_repo,
    function as function_repo,
    tag as tag_repo,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)


def _merge_fragment(base: dict, fragment: dict, fragment_path: Path) -> None:  # REQ-1669
    """Merge an included fragment into ``base`` in place.

    List-valued sections (sources, tables, domains, relationships, roles, …) append — the fragment's
    entries follow the including file's. A scalar or mapping key the including file does not set is
    taken from the fragment; one both set to different values is a conflict and the load fails —
    a fragment never silently overrides the file that included it.
    """
    for key, value in fragment.items():
        if key not in base:
            base[key] = value
        elif isinstance(base[key], list) and isinstance(value, list):
            base[key] = base[key] + value
        elif base[key] != value:
            raise ValueError(
                f"config include {fragment_path}: key {key!r} conflicts with the including file; "
                "only list sections merge"
            )


def read_config_with_includes(
    path: str | Path, _seen: frozenset[Path] = frozenset()
) -> dict:  # REQ-1669
    """Read a YAML config and splice in its ``includes:`` fragments, recursively.

    Paths resolve relative to the including file. A file that includes itself (directly or through
    another fragment) is refused. The ``includes`` key never survives into the returned dict.
    """
    file_path = Path(path).resolve()
    if file_path in _seen:
        raise ValueError(f"config include cycle at {file_path}")
    if not _seen:
        from provisa.core.config_location import note_config_changed

        # A config file is being (re)loaded — startup, a rebuild: parsed copies held in memory
        # (the request path's platform config) are replaced at their next use.
        note_config_changed()
    with open(file_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"config {file_path}: top level must be a mapping")
    includes = raw.pop("includes", None) or []
    if not isinstance(includes, list) or not all(isinstance(i, str) for i in includes):
        raise ValueError(f"config {file_path}: includes must be a list of paths")
    for inc in includes:
        inc_path = Path(inc)
        if not inc_path.is_absolute():
            inc_path = file_path.parent / inc_path
        fragment = read_config_with_includes(inc_path, _seen | {file_path})
        _merge_fragment(raw, fragment, inc_path)
    if not _seen:
        commit_time_columns(views_as_tables(raw))
    return raw


#: A ``views:`` entry's keys that carry over to the table entry it becomes, by table-entry name.
_VIEW_KEYS = {
    "domain_id": "domain_id",
    "sql": "view_sql",
    "materialize": "materialize",
    "refresh_interval": "mv_refresh_interval",
    "description": "description",
    "alias": "alias",
    "columns": "columns",
    "preprocess": "mv_preprocess",
    "hold_on_failing_checks": "mv_hold_on_failing_checks",  # REQ-2133
    # REQ-2009: a view offers the GraphQL Aggregate and GroupBy root fields as a table does.
    "enable_aggregates": "enable_aggregates",
    "enable_group_by": "enable_group_by",
    # REQ-2109: the bitemporal declaration -- business key, valid time and recorded time, all
    # three or none; a materialized view declaring them keeps every version of its rows.
    "business_key": "business_key",
    "valid_time": "valid_time",
    "recorded_time": "recorded_time",
    # REQ-2066: a materialized view's load management window, when consumers may read it.
    "off_peak_window": "off_peak_window",
    "off_peak_tz": "off_peak_tz",
    # REQ-1921: a view naming a region is built and refreshed there alone.
    "region": "region",
    # REQ-1634: a view is a member of a data product as a table is.
    "product_id": "product_id",
    # REQ-2120: a materialized view's snapshot schedule, as a table entry's mv_* keys (REQ-961/962).
    "calendar": "mv_calendar",
    "grain": "mv_grain",
    "business_day_grain": "mv_business_day_grain",
    "allowed_lateness": "mv_allowed_lateness",
    "expected_events": "mv_expected_events",
}


def kafka_topics_as_tables(raw: dict) -> dict:  # REQ-147, REQ-1919
    """``raw`` with each Kafka source's topics declared as tables of the source, and each Kafka
    source declared as a source (a registered table names its source). A table or source the raw
    configuration already holds — the store's own, read back after the seed — is not declared
    again. Returns a new dict; ``raw`` is not changed."""
    kafka = raw.get("kafka_sources") or []
    if not kafka:
        return raw
    out = dict(raw)
    sources = list(out.get("sources") or [])
    tables = list(out.get("tables") or [])
    held_sources = {s.get("id") for s in sources}
    held_tables = {
        (
            t.get("source_id"),
            t.get("schema") or t.get("schema_name"),
            t.get("table") or t.get("table_name"),
        )
        for t in tables
    }
    for ks in kafka:
        source_id = ks["id"]
        if source_id not in held_sources:
            sources.append(
                {"id": source_id, "type": "kafka", "host": ks.get("bootstrap_servers", "")}
            )
            held_sources.add(source_id)
        for topic in ks.get("topics", []):
            name = topic.get("table_name") or topic.get("id", "").replace("-", "_")
            if (source_id, "default", name) in held_tables:
                continue
            tables.append(
                {
                    "source_id": source_id,
                    "domain_id": topic.get("domain_id", "support"),
                    "schema": "default",
                    "table": name,
                    "description": topic.get("description", ""),
                    "columns": [
                        {
                            "name": col.get("name", col) if isinstance(col, dict) else col,
                            # REQ-1426: the topic's declared type is the registered column's.
                            "data_type": col.get("data_type") if isinstance(col, dict) else None,
                            "visible_to": col.get("visible_to", ["org_admin", "analyst"])
                            if isinstance(col, dict)
                            else ["org_admin", "analyst"],
                            "description": col.get("description", "")
                            if isinstance(col, dict)
                            else "",
                        }
                        for col in topic.get("columns", [])
                    ],
                }
            )
            held_tables.add((source_id, "default", name))
    out["sources"] = sources
    out["tables"] = tables
    return out


def views_as_tables(raw: dict) -> dict:
    """Turn the config's ``views:`` block into the table entries it declares (REQ-133).

    A view is a table of the derived source whose rows its SQL defines — the spelling a ``tables:``
    entry with ``view_sql`` already has, which the load stores and the schema build registers
    (inline, and also materialized when it says so). Declared under ``views:``, a view used to be
    registered for refresh but never stored as a table, so nothing could read it. One spelling,
    one path: each entry becomes a table entry here, where the raw config is read."""
    from provisa.core.models import DERIVED_SOURCE_ID

    views = raw.pop("views", None) or []
    if not isinstance(views, list):
        raise ValueError("config views: must be a list")
    if not views:
        return raw  # nothing declared: the config is left exactly as written
    tables = raw.setdefault("tables", [])
    for view in views:
        unknown = set(view) - set(_VIEW_KEYS) - {"id"}
        if unknown:
            raise ValueError(f"view {view.get('id')!r}: unknown keys {sorted(unknown)}")
        if view.get("off_peak_window") and not view.get("materialize"):
            # REQ-2066: the window says when a stored view may be read; a view not stored is read
            # through its SQL from what it reads, each with its own load management.
            raise ValueError(
                f"view {view.get('id')!r}: off_peak_window applies to a materialized view "
                "(materialize: true)"
            )
        entry: dict[str, Any] = {
            "source_id": DERIVED_SOURCE_ID,
            "schema": "views",
            "table": f"view_{view['id'].replace('-', '_')}",
        }
        entry.update({_VIEW_KEYS[k]: v for k, v in view.items() if k in _VIEW_KEYS})
        if entry.get("business_key") and entry.get("columns"):
            # REQ-2064: a bitemporal view answers each version's times beside its own columns.
            from provisa.mv.bitemporal import with_read_columns

            entry["columns"] = with_read_columns(
                entry["columns"],
                list(entry["business_key"]),
                entry.get("recorded_time"),
            )
        tables.append(entry)
    return raw


def commit_time_columns(raw: dict) -> dict:
    """``raw`` with each table kept by change data capture given ``source_committed_at``, the
    commit time its landing carries (REQ-1163, federation/commit_time.py). Returns ``raw``."""
    from provisa.federation.commit_time import carries, with_column

    sources = {s.get("id"): s for s in raw.get("sources") or [] if isinstance(s, dict)}
    for table in raw.get("tables") or []:
        source = sources.get(table.get("source_id"))
        if source is None or not table.get("columns"):
            continue
        signal = table.get("change_signal") or source.get("change_signal")
        if carries(source.get("type"), signal):
            table["columns"] = with_column(table["columns"])
    return raw


def parse_config(path: str | Path) -> ProvisaConfig:  # REQ-250, REQ-1669
    """Parse and validate a YAML config file (with its ``includes:``). Does NOT resolve secrets."""
    return ProvisaConfig.model_validate(read_config_with_includes(path))


def load_control_plane(config_path: str | Path | None) -> ControlPlaneConfig:  # REQ-837
    """Read just the ``control_plane`` config section (or defaults).

    The control-plane database connections must be available before the full
    config is loaded (the admin UI needs the DB on first start, possibly before a
    config file exists), so this is parsed independently of ``parse_config``. It
    is the config layer — env/secret resolution happens here, not in callers."""
    if config_path and Path(config_path).exists():
        # REQ-1669: a wrapper config that only ``includes:`` the real one carries its
        # control_plane section through the same merge the full parse uses.
        raw = read_config_with_includes(config_path)
        return ControlPlaneConfig.model_validate(raw.get("control_plane", {}))
    return ControlPlaneConfig()


def parse_config_dict(data: dict, *, stored: bool = False) -> ProvisaConfig:  # REQ-250
    """Parse and validate a config dict.

    Resolve secret references (``${provider:ref}``) on the raw dict BEFORE pydantic
    validation so they work for every field, not just the string fields re-resolved
    at use time. Without this, a ``${env:PG_PORT}`` in an ``int`` field (a source
    ``port``) reaches pydantic as the literal template and fails int-parsing.
    Resolution is idempotent, so the later per-field ``resolve_secrets(...)`` calls
    stay no-ops.

    ``stored``: ``data`` is the model the store holds (:func:`parse_store_raw`), parsed with the
    ``STORED_MODEL`` validation context.
    """
    from provisa.core.secrets import resolve_secrets_in_dict

    config = ProvisaConfig.model_validate(
        kafka_topics_as_tables(
            commit_time_columns(
                views_as_tables(_code_secrets_as_written(data, resolve_secrets_in_dict))
            )
        ),
        context={STORED_MODEL: stored},
    )
    # What is STORED is the config as written: a credential stays the reference the file gave,
    # and its value is resolved where it is used.
    config._written = _as_written(
        config, kafka_topics_as_tables(commit_time_columns(views_as_tables(data)))
    )
    return config


def _code_secrets_as_written(data: dict, resolve: Any) -> dict:
    """``resolve(data)``, but the mapping values resolved at use kept as the references written:
    a code source's declared secrets (REQ-2104), each resolved only when the code reads it,
    through Provisa's services (provisa/codesource/services.py), so a config naming a secret not
    yet in the vault loads; and a data-quality checker's whole connection (REQ-1443), resolved at
    each scan from the stored mapping (dq.scan.checker_connection), so a password the deployment
    has not set fails that scan by its name rather than every load of the org's model."""
    from provisa.codesource.source import CODE_SOURCE_TYPES
    from provisa.dq.contract import CHECKERS

    held: dict[int, dict[str, Any]] = {}
    sources = data.get("sources")
    if isinstance(sources, list):
        for i, source in enumerate(sources):
            mapping = source.get("mapping") if isinstance(source, dict) else None
            if not isinstance(mapping, dict):
                continue
            if source.get("type") in CHECKERS:
                held[i] = dict(mapping)
            elif source.get("type") in CODE_SOURCE_TYPES and "secrets" in mapping:
                held[i] = {"secrets": mapping["secrets"]}
    if not held:
        return resolve(data)
    resolved = resolve(
        {
            **data,
            "sources": [
                {**s, "mapping": {k: v for k, v in s["mapping"].items() if k not in held[i]}}
                if i in held
                else s
                for i, s in enumerate(sources)  # type: ignore[arg-type]
            ],
        }
    )
    for i, written in held.items():
        resolved["sources"][i]["mapping"].update(written)
    return resolved


_REFERENCE = "${"
#: A table's fields that name its data where it is: stored resolved (_as_written). Its live
#: block names the Kafka topic it follows, and the broker a change is published to.
_TABLE_NAMES = frozenset({"schema_name", "table_name", "alias", "query_template", "live"})


def _raw_of(raw: dict, model: BaseModel, name: str) -> Any:
    """The value a config dict gave the model field ``name`` (under its name or an alias)."""
    field = type(model).model_fields[name]
    keys = [name]
    if field.alias:
        keys.append(field.alias)
    if isinstance(field.validation_alias, str):
        keys.append(field.validation_alias)
    elif isinstance(field.validation_alias, AliasChoices):
        keys += [c for c in field.validation_alias.choices if isinstance(c, str)]
    for key in keys:
        if key in raw:
            return raw[key]
    return None


def _as_written(resolved: Any, raw: Any) -> Any:
    """``resolved`` (a validated config value) with every text value the file gave as a reference
    put back as that reference. ``raw`` is the same value in the file's own, unresolved, form.

    A value that is not text (a port given as ``${env:PG_PORT}``) stays resolved: it has no
    text form to store. A list the two forms disagree on in length cannot be paired, and is
    refused rather than stored resolved."""
    if isinstance(resolved, str):
        return raw if isinstance(raw, str) and _REFERENCE in raw else resolved
    if isinstance(resolved, BaseModel):
        if not isinstance(raw, dict):
            return resolved
        # What names a table's data is no credential: stored as resolved, it is read wherever the
        # table is, as its source's fields are not (resolved_source). A demo's shared stores name
        # its data by a variable its launcher sets (``catalog${env:REVIEWS_MONGO_NS}``).
        named = _TABLE_NAMES if isinstance(resolved, Table) else frozenset()
        update = {
            name: _as_written(getattr(resolved, name), _raw_of(raw, resolved, name))
            for name in type(resolved).model_fields
            if name not in named
        }
        return resolved.model_copy(update=update)
    if isinstance(resolved, dict):
        if not isinstance(raw, dict):
            return resolved
        return {key: _as_written(value, raw.get(key)) for key, value in resolved.items()}
    if isinstance(resolved, list):
        if not isinstance(raw, list):
            return resolved
        if len(raw) != len(resolved):
            raise ValueError(
                "the config as written and as validated disagree on a list's length; its "
                "references cannot be kept, so it is not loaded"
            )
        return [_as_written(value, raw[i]) for i, value in enumerate(resolved)]
    return resolved


async def _upsert_sources(conn: "Connection", config: ProvisaConfig) -> None:  # REQ-012, REQ-250
    """Add each source the config declares to the store, or replace its definition. Stored as
    written: a credential's reference, never its value."""
    for src in config.written.sources:
        await source_repo.upsert(conn, src)


def resolved_source(source: Source) -> Source:  # REQ-1919
    """A stored source with every reference it holds resolved, as the engine and the pools are
    given it. The store keeps the reference; the value is resolved here, at use."""
    from provisa.core.secrets import resolve_secrets_in_dict

    return Source.model_validate(resolve_secrets_in_dict(source.model_dump(by_alias=True)))


def register_sources(  # REQ-012, REQ-1266, REQ-1730, REQ-1919
    engine: Any, sources: list[Source], catalog_names: dict[str, str] | None = None
) -> list[str]:
    """(Re)issue the engine catalog of each source. Returns the ids whose catalog failed.

    ``sources`` are resolved (:func:`resolved_source`). A failure is REPORTED, not swallowed:
    boot tolerates it (an unreachable source must not brick startup) but a wake does not,
    because on a wake this IS the catalog the next query reads. The caller decides which it is.
    ``catalog_names`` (REQ-1266) supplies the org-prefixed physical catalog name for a
    non-default org so the source attaches under its own namespace.
    """
    failed: list[str] = []
    for src in sources:
        try:
            engine.register_source(
                src,
                resolve_secrets(src.password),
                catalog_name=(catalog_names or {}).get(src.id),
            )
        except Exception:
            log.exception(
                "registering source %r on the engine catalog %r failed",
                src.id,
                (catalog_names or {}).get(src.id) or src.id,
            )
            failed.append(src.id)
    return failed


async def _upsert_naming_rules(conn: "Connection", config: ProvisaConfig) -> None:
    """Add each naming rule the config declares, or replace the replacement of the rule with
    its pattern (REQ-1919: an apply removes nothing)."""
    for rule in config.naming.rules:
        held = (
            await conn.execute_core(
                select(naming_rules.c.id).where(naming_rules.c.pattern == rule.pattern)
            )
        ).fetchone()
        if held is None:
            await conn.execute_core(
                insert(naming_rules).values(pattern=rule.pattern, replacement=rule.replace)
            )
        else:
            await conn.execute_core(
                update(naming_rules)
                .where(naming_rules.c.id == held.id)
                .values(replacement=rule.replace)
            )


def _load_openapi_specs(config: ProvisaConfig) -> dict[str, dict]:
    """Pre-load OpenAPI specs once per source (avoid repeated HTTP fetches)."""
    openapi_specs: dict[str, dict] = {}
    for src in config.sources:
        if src.type.value == "openapi" and src.path:
            try:
                from provisa.openapi.exclusions import load_source_spec

                # REQ-1957: without what the source's own exclusions name.
                openapi_specs[src.id] = load_source_spec(
                    resolve_secrets(src.path), src.mapping, source_id=src.id
                )
            except Exception as _e:
                log.warning("Failed to load OpenAPI spec for %s: %s", src.id, _e)
    return openapi_specs


def _enrich_openapi_table_columns(
    tbl: Table,
    spec: dict,
) -> None:
    """Update table columns with descriptions from the OpenAPI spec (in-place).

    REQ-1426: descriptions only. A column's data_type is design-time metadata carried by the
    config; nothing types a column while loading it.
    """
    from provisa.openapi.mapper import parse_spec
    from provisa.openapi.register import _schema_to_columns

    queries, _ = parse_spec(spec)
    match = next(
        (q for q in queries if normalize_op_id(q.operation_id) == normalize_op_id(tbl.table_name)),
        None,
    )
    if not match:
        return
    spec_col_map = {c["name"]: c for c in _schema_to_columns(match.response_schema)}
    for col in tbl.columns:
        if col.name in spec_col_map and not col.description:
            col.description = spec_col_map[col.name].get("description")


async def _handle_openapi_table(
    conn: "Connection",
    tbl: Table,
    src: Source,
    spec: dict,
) -> None:
    """The endpoint a config-declared OpenAPI table is served from, derived by the same function
    the admin registration uses (REQ-316, REQ-318). REQ-1915: nothing is fetched here. A table
    whose spec has no operation of its name fails the load: it would have nothing to be read
    from. The source's ``auth``, when it declares one, is what the caller sends."""
    from provisa.api_source.openapi_endpoint import (
        register_openapi_endpoint,
        register_openapi_source,
        store_openapi_auth,
    )

    assert src.base_url is not None
    # ``src`` is the source as written: a reference in its base URL or auth is stored as the
    # reference and resolved at each call (api_source.caller).
    await register_openapi_source(conn, src.id, src.base_url)
    if src.auth is not None:
        await store_openapi_auth(conn, src.id, src.auth.model_dump())
    await register_openapi_endpoint(conn, tbl, spec=spec, ttl=src.cache_ttl or 300)


_QUERY_API_TYPES = frozenset({"neo4j", "sparql"})  # REQ-1668, REQ-1683


def _validate_neo4j_sources(config: ProvisaConfig) -> None:  # REQ-1668, REQ-1683
    """A neo4j source names host, port and database; a sparql source's host is its endpoint URL;
    a table under either carries the query it runs, and no other table carries one."""
    sources_by_id = {s.id: s for s in config.sources}
    for src in config.sources:
        if src.type.value == "neo4j":
            missing = [f for f in ("host", "port", "database") if not getattr(src, f)]
            if missing:
                raise ValueError(
                    f"neo4j source {src.id!r}: {', '.join(missing)} required (the HTTP transaction "
                    "endpoint is http://host:port/db/<database>/tx/commit)"
                )
        elif src.type.value == "sparql":
            if not (src.host or "").startswith(("http://", "https://")):
                raise ValueError(
                    f"sparql source {src.id!r}: host must be the SPARQL endpoint URL "
                    "(e.g. http://fuseki:3030/ds/query)"
                )
            from provisa.sparql.reasoning import source_reasoner

            source_reasoner(src.id, src.mapping)  # REQ-2138
    for tbl in config.tables:
        src = sources_by_id.get(tbl.source_id)
        stype = src.type.value if src is not None else None
        is_query_api = stype in _QUERY_API_TYPES
        if is_query_api and not tbl.query_template:
            raise ValueError(
                f"table {tbl.table_name!r}: a table under {stype} source {tbl.source_id!r} requires "
                "query_template (the query that produces its rows)"
            )
        if stype == "sparql" and src is not None:
            from provisa.sparql.reasoning import inferred_iri, source_reasoner

            if inferred_iri(tbl.query_template) and source_reasoner(src.id, src.mapping) is None:
                raise ValueError(
                    f"table {tbl.table_name!r}: its query names one IRI, an inferred class or "
                    f"property, and sparql source {src.id!r} names no reasoner"
                )
        if stype == "cosmosdb":
            # REQ-1982: a container table, or a query table; its parameters checked either way.
            from provisa.cosmosdb.table import check_table as check_cosmos_table

            check_cosmos_table(tbl)
            continue
        if stype == "qdrant":
            # REQ-1983: a collection table, or a search; its arguments checked either way.
            from provisa.qdrant.table import check_table as check_qdrant_table

            check_qdrant_table(tbl)
            continue
        if stype in ("node", "python"):
            # REQ-1984, REQ-1993: a code table names its function; checked against the code when
            # the table is registered (its manifest is the code's, read in the worker's sandbox).
            from provisa.codesource.table import check_table as check_code_table

            check_code_table(tbl)
            continue
        if tbl.query_template and stype == "mongodb":
            # REQ-1980: a MongoDB table with a native query is a pipeline table.
            from provisa.mongodb.native_query import check_table

            check_table(tbl)
            continue
        if tbl.query_template and stype == "elasticsearch":
            # REQ-2017: an Elasticsearch table with a native query is a native search.
            from provisa.elasticsearch.native_query import check_table as check_search_table

            check_search_table(tbl)
            continue
        if tbl.query_template and not is_query_api:
            raise ValueError(
                f"table {tbl.table_name!r}: query_template is only valid under a neo4j, sparql, "
                "mongodb, elasticsearch, cosmosdb, qdrant, node or python source"
            )


#: REQ-2061: the sources whose tables take arguments by name, which a table may fix. REQ-2069: a
#: Jira table's arguments are its form's project and issue type.
_ARGUMENT_SOURCE_TYPES = frozenset({"graphql_remote", "openapi", "jira"})


def _validate_table_arguments(config: ProvisaConfig) -> None:  # REQ-2061, REQ-2069
    """A table fixes arguments only where its source takes them by name: a GraphQL root field's
    arguments, an OpenAPI operation's parameters, a Jira form's project and issue type. Anywhere
    else it is refused by name; a Jira table fixing anything but its form is refused too."""
    from provisa.federation.jira import form_of

    types = {s.id: s.type.value for s in config.sources}
    for tbl in config.tables:
        stype = types.get(tbl.source_id)
        if stype == "jira":
            form_of(tbl)
        elif tbl.arguments and stype not in _ARGUMENT_SOURCE_TYPES:
            raise ValueError(
                f"table {tbl.table_name!r}: arguments are fixed only on a table of a "
                f"graphql_remote, openapi or jira source, not {stype!r}"
            )


async def _handle_neo4j_table(conn: "Connection", tbl: Table, src: Source) -> None:  # REQ-1668
    """Persist the config table's Cypher as an api_endpoints row (plus its api_sources row)."""
    from provisa.neo4j.persist import persist_neo4j_table

    assert tbl.query_template is not None  # _validate_neo4j_sources
    await persist_neo4j_table(
        conn,
        source_id=src.id,
        host=src.host,
        port=src.port,
        database=src.database,
        base_url=src.base_url or None,  # as written; resolved at each call
        table_name=tbl.table_name,
        query_template=tbl.query_template,
        columns=tbl.columns,
        ttl=tbl.cache_ttl or src.cache_ttl or 300,
    )


async def _handle_sparql_table(conn: "Connection", tbl: Table, src: Source) -> None:  # REQ-1683
    """Persist the config table's SPARQL as an api_endpoints row (plus its api_sources row)."""
    from provisa.sparql.persist import persist_sparql_table
    from provisa.sparql.reasoning import source_reasoner

    assert tbl.query_template is not None  # _validate_neo4j_sources
    await persist_sparql_table(
        conn,
        source_id=src.id,
        endpoint_url=src.host,  # as written; resolved at each call
        table_name=tbl.table_name,
        query_template=tbl.query_template,
        columns=tbl.columns,
        ttl=tbl.cache_ttl or src.cache_ttl or 300,
        default_graph_uri=src.federation_hints.get("default_graph_uri"),  # REQ-1740
        reasoner=source_reasoner(src.id, src.mapping),  # REQ-2138
    )


async def _upsert_single_table(
    conn: "Connection",
    engine: Any,
    tbl: Table,
    src: Source | None,
    openapi_specs: dict[str, dict],
) -> None:
    """Upsert one table and run source-type-specific post-upsert steps."""
    if src and src.type.value == "openapi" and src.base_url:
        spec = openapi_specs.get(src.id, {})
        if spec:
            _enrich_openapi_table_columns(tbl, spec)

    # REQ-1426: data_type is design-time metadata — the config carries it and nothing infers it
    # here. A column that reaches this point untyped means the design was never completed; the
    # repository refuses it rather than persisting a hole the catalog renders as "unknown".
    await table_repo.upsert(conn, tbl)

    if src and src.type.value == "openapi" and src.base_url:
        spec = openapi_specs.get(src.id, {})
        if spec:
            await _handle_openapi_table(conn, tbl, src, spec)

    if src and src.type.value == "neo4j":
        await _handle_neo4j_table(conn, tbl, src)

    if src and src.type.value == "sparql":
        await _handle_sparql_table(conn, tbl, src)


async def _analyze_sources(  # REQ-275
    engine: Any, config: ProvisaConfig, catalog_names: dict[str, str] | None = None
) -> None:
    """Prime federation CBO stats after tables are registered — through the engine seam.

    ``catalog_names`` (REQ-1266) is the same map ``_upsert_sources`` registers under: the source
    lives at the org-prefixed catalog on a non-default org, so analyzing the bare name asked an
    isolated coordinator about a catalog it does not have and every ANALYZE died TABLE_NOT_FOUND.
    """
    for src in config.sources:
        try:
            engine.analyze(src, config.tables, catalog_name=(catalog_names or {}).get(src.id))
        except Exception:
            # Stats are an optimization — a source whose catalog cannot be analyzed still serves
            # queries — so this does not join the failed-catalog list. It is reported, never
            # silently dropped.
            log.exception("priming CBO stats for source %r failed", src.id)


def _expand_view_metrics(config: ProvisaConfig) -> None:  # REQ-1318
    """Compile each table's metric-composed view definition to SQL, in place on ``config``."""
    # REQ-1318: compile metric-composed view definitions to SQL at registration, so the
    # generated SELECT flows everywhere view_sql does (registered_tables.view_sql →
    # view_sql_map / MV registration). Free-hand view_sql with metric() calls is inlined
    # the same way. Config relationships key tables by table_name, so the registry ids
    # here ARE the semantic table names.
    from provisa.compiler.metric_expand import (
        expand_metric_calls_in_sql,
        generate_view_metrics_sql,
    )

    metric_registry = {m.name: m for m in config.metrics}
    semantic_tables: dict[str, dict] = {}
    _virtual_to_name: dict[str, str] = {}
    for t in config.tables:
        entry = {"id": t.table_name, "columns": [c.name for c in t.columns]}
        semantic_tables[t.table_name] = entry
        _virtual_to_name[t.table_name] = t.table_name
        _alias = getattr(t, "alias", None)
        if _alias:
            # Config relationships address tables by virtual name (alias when set) —
            # normalize both the registry key and the relationship ids to table_name.
            semantic_tables[_alias] = entry
            _virtual_to_name[_alias] = t.table_name
    semantic_rels = [
        {
            "source_table_id": _virtual_to_name.get(r.source_table_id, r.source_table_id),
            "target_table_id": _virtual_to_name.get(r.target_table_id, r.target_table_id),
            "source_column": r.source_column,
            "target_column": r.target_column,
        }
        for r in config.relationships
    ]
    for tbl in config.tables:
        if tbl.view_metrics is not None:
            tbl.view_sql = generate_view_metrics_sql(
                tbl.view_metrics, metric_registry, semantic_tables, semantic_rels
            )
        elif tbl.view_sql is not None:
            tbl.view_sql, _ = expand_metric_calls_in_sql(tbl.view_sql, metric_registry)


def _settle_table_names(engine: Any, config: ProvisaConfig) -> None:  # REQ-471
    """Give each declared table the name it is registered under, in place on ``config``.

    Semantic-SQL naming authority (REQ-471): a source the engine cannot ATTACH in place is
    materialized into the store, so its registered/physical name is a semantic alias — not a
    source-physical name — and MUST be normalized through the central naming authority so all
    data in the store follows one convention. Attach sources keep their source-physical name
    (it must match the real table). Never rename inline — always route through apply_sql_name.
    """
    from provisa.compiler.naming import apply_sql_name
    from provisa.federation.strategy import engine_attaches

    if engine is None:
        return
    sources_by_id = {src.id: src for src in config.sources}
    for tbl in config.tables:
        src = sources_by_id.get(tbl.source_id)
        if src is not None and not engine_attaches(engine, src.type.value):
            tbl.table_name = apply_sql_name(tbl.table_name)


async def _upsert_tables(  # REQ-013, REQ-016, REQ-251
    conn: "Connection",
    engine: Any,
    config: ProvisaConfig,
    openapi_specs: dict[str, dict],
) -> None:
    _expand_view_metrics(config)
    # REQ-2052: a profile result named by its member takes the member's stored id where the member
    # is stored already; one whose member this apply registers first is resolved after it is.
    await _resolve_profile_results(conn, config, required=False)
    await _place_in_control_plane(conn, config)
    _settle_table_names(engine, config)
    await _refuse_published_name_clash(conn, config)

    # The rows a table's source-specific step writes (its API source and endpoint) are stored
    # from the source as written: a credential in its address stays a reference.
    written_by_id = {src.id: src for src in config.written.sources}
    pending = [t for t in config.tables if _profile_result_pending(t)]
    for tbl in [t for t in config.tables if not _profile_result_pending(t)]:
        src = written_by_id.get(tbl.source_id)
        await _upsert_single_table(
            conn, engine, await _keep_unlisted_columns(conn, tbl), src, openapi_specs
        )
    await _resolve_profile_results(conn, config, required=True)
    for tbl in pending:
        src = written_by_id.get(tbl.source_id)
        await _upsert_single_table(
            conn, engine, await _keep_unlisted_columns(conn, tbl), src, openapi_specs
        )


def _profile_result_pending(tbl: Table) -> bool:  # REQ-2052
    from provisa.core.models import pending_profile_result_name

    member, kind = tbl.profile_of, tbl.profile_kind
    if member is None or kind is None:
        return False
    return tbl.table_name == pending_profile_result_name(member, kind)


async def _resolve_profile_results(  # REQ-2052
    conn: "Connection", config: ProvisaConfig, *, required: bool
) -> None:
    """Name each result table the configuration names by its member (``profile_of``, ``kind``)
    by the member's stored id: ``<member>_<id>_profile_<kind>``. A member not stored yet is left
    pending unless ``required``, which refuses it by name; two members of one profiler sharing
    the name are refused, naming both."""
    from provisa.profiler.schema import result_table_name

    for tbl in config.tables:
        member, kind = tbl.profile_of, tbl.profile_kind
        if member is None or kind is None or not _profile_result_pending(tbl):
            continue
        rows = (
            await conn.execute_core(
                select(registered_tables.c.id, registered_tables.c.source_id).where(
                    registered_tables.c.table_name == tbl.profile_of,
                    registered_tables.c.profiler_source_id == tbl.source_id,
                )
            )
        ).fetchall()
        if len(rows) > 1:
            raise ValueError(
                f"profile_of {tbl.profile_of!r}: profiler {tbl.source_id!r} has more than one "
                f"member of that name ({', '.join(r.source_id for r in rows)})"
            )
        if not rows:
            if required:
                raise ValueError(
                    f"profile_of {tbl.profile_of!r}: no table of that name is a member of "
                    f"profiler {tbl.source_id!r}"
                )
            continue
        tbl.table_name = result_table_name(member, rows[0].id, kind)


async def _place_in_control_plane(conn: "Connection", config: ProvisaConfig) -> None:
    """A table of a kind that writes its own rows into the org's control plane (ingest, a
    profiler's results, a checker's scans: ``NO_REPLICA_TYPES``) is registered under the org's
    control-plane schema, the one location the compiler reaches it through (provisa_admin), as the
    admin path registers it (REQ-1771, REQ-1934, REQ-1443). A checker table's result relation is
    created there, empty until its first scan."""
    from provisa.core.environments import active_org_schema  # noqa: PLC0415
    from provisa.core.request_context import require_current_org  # noqa: PLC0415
    from provisa.dq.registration import is_checker_source_type  # noqa: PLC0415
    from provisa.dq.scan import ensure_relation  # noqa: PLC0415
    from provisa.federation.replica_routing import NO_REPLICA_TYPES  # noqa: PLC0415

    types = {s.id: str(getattr(s.type, "value", s.type)) for s in config.sources}
    for tbl in config.tables:
        stype = types.get(tbl.source_id)
        if stype not in NO_REPLICA_TYPES:
            continue
        tbl.schema_name = active_org_schema(require_current_org(), "")
        if is_checker_source_type(stype):
            await ensure_relation(conn, tbl.table_name)


class PublishedNameClash(ValueError):  # REQ-1919
    """An apply would register a second table under a name a table already has in its domain."""


async def _refuse_published_name_clash(  # REQ-1919
    conn: "Connection", config: ProvisaConfig
) -> None:
    """Refuse, naming both, an apply after which two tables would share a published name (alias,
    else table name) in one domain. An apply removes nothing, so a table the file moved to another
    schema, or a second source's table of the same name, would otherwise stand beside the one
    already registered and every lookup by that name would be ambiguous."""
    rows = (
        await conn.execute_core(
            select(
                registered_tables.c.source_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
                registered_tables.c.alias,
                registered_tables.c.domain_id,
            )
        )
    ).fetchall()
    final: dict[tuple[str, str, str], tuple[str, str]] = {
        (r.source_id, r.schema_name, r.table_name): (r.domain_id, r.alias or r.table_name)
        for r in rows
    }
    for tbl in config.tables:
        final[(tbl.source_id, tbl.schema_name, tbl.table_name)] = (
            domain_policy.resolve_domain_id(tbl.domain_id),
            tbl.alias or tbl.table_name,
        )
    holders: dict[tuple[str, str], list[str]] = {}
    for (source_id, schema_name, table_name), key in final.items():
        holders.setdefault(key, []).append(f"{source_id}.{schema_name}.{table_name}")
    clashes = {key: sorted(found) for key, found in holders.items() if len(found) > 1}
    if clashes:
        detail = "; ".join(
            f"{domain}.{name}: {' and '.join(found)}"
            for (domain, name), found in sorted(clashes.items())
        )
        raise PublishedNameClash(
            "the configuration would register two tables under one name in one domain — "
            f"{detail}. Delete one of them, or give one an alias, and apply again"
        )


async def _keep_unlisted_columns(conn: "Connection", tbl: Table) -> Table:  # REQ-1919
    """``tbl`` with the registered columns its declaration no longer lists added back as they are
    stored: an apply adds and updates and removes nothing, a column included."""
    from provisa.core.models import Column as ColumnModel

    held = (
        await conn.execute_core(
            select(registered_tables.c.id).where(
                registered_tables.c.source_id == tbl.source_id,
                registered_tables.c.schema_name == tbl.schema_name,
                registered_tables.c.table_name == tbl.table_name,
            )
        )
    ).fetchone()
    if held is None:
        return tbl
    listed = {c.name for c in tbl.columns}
    kept = [
        ColumnModel.model_validate(
            {
                **{k: v for k, v in col.items() if k in ColumnModel.model_fields and v is not None},
                "name": col["column_name"],
            }
        )
        for col in await table_repo.load_columns(conn, held.id)
        if col["column_name"] not in listed
    ]
    if not kept:
        return tbl
    return tbl.model_copy(update={"columns": [*tbl.columns, *kept]})


async def _upsert_relationships(
    conn: "Connection", config: ProvisaConfig, *, defer_unregistered: bool
) -> None:  # REQ-018, REQ-019, REQ-020
    """Upsert the relationships the file declares. Nothing is removed here (REQ-1919): one the
    file no longer declares is judged with everything else at the end of the load.

    ``defer_unregistered``: a relationship naming a table the file does not declare -- one a
    remote source's registration brings later in the same boot -- is left for that registration's
    retry (:func:`upsert_declared_relationship`); the caller that passes it retries. Every other
    failure fails the load, naming the relationship."""
    declared = declared_table_names(config)
    for rel in config.relationships:
        waiting = await upsert_declared_relationship(conn, rel, declared)
        if waiting is not None and not defer_unregistered:
            raise ValueError(
                f"relationship {rel.id!r} names {waiting!r}, which no registration holds"
            )


def declared_table_names(config: ProvisaConfig) -> frozenset[str]:
    """The names a relationship reaches the file's own tables by: each one's alias, else its
    table name (table_repo.find_by_table_name)."""
    return frozenset(t.alias or t.table_name for t in config.tables)


async def upsert_declared_relationship(
    conn: "Connection", rel: Any, declared: frozenset[str]
) -> str | None:
    """Upsert one relationship the configuration declares. None once it is registered; the table
    it is waiting for when that table is not one the file declares (a remote source registers it)
    and is not registered yet. Anything else -- a table the file declares that did not register,
    a type its table already has -- raises, naming the relationship."""
    try:
        await rel_repo.upsert(conn, rel)
    except rel_repo.TableNotRegistered as exc:
        if exc.table_name not in declared:
            return exc.table_name
        raise ValueError(f"relationship {rel.id!r}: {exc}") from exc
    except ValueError as exc:
        raise ValueError(f"relationship {rel.id!r}: {exc}") from exc
    return None


async def _upsert_metrics(conn: "Connection", config: ProvisaConfig) -> None:  # REQ-1317, REQ-1320
    """Upsert the metrics the file declares. Nothing is removed here (REQ-1919): one the file no
    longer declares is judged at the end of the load. Fact-derived metrics (``from_fact`` set,
    REQ-1320) are parts of their fact table and are never judged by the file's list."""
    for m in config.metrics:
        await metric_repo.upsert(conn, m)


async def _resolve_tag_assignment_table(
    conn: "Connection", ta: TagAssignment
) -> TagAssignment:  # REQ-1377, REQ-1266
    """The assignment with ``table_id`` resolved in THIS org's registry.

    ``table_ref`` ("source.schema.table") is the config-vocabulary identity and wins whenever it
    is present: a serial ``table_id`` is local to one org's ``registered_tables``. The demo config
    object is shared by every demo org, and the default org's runtime rewrites its assignments
    from its own rows — serial included — so a second org honouring that serial inserts a
    ``table_id`` its registry never issued and the FK rejects the whole org build.
    """
    if ta.table_ref is None:
        return ta
    parts = ta.table_ref.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"tag assignment {ta.tag_id!r}: table_ref {ta.table_ref!r} "
            "must be 'source.schema.table'"
        )
    resolved = await tag_repo.resolve_table_id(conn, *parts)
    if resolved is None:
        raise ValueError(
            f"tag assignment {ta.tag_id!r}: table_ref {ta.table_ref!r} "
            "names a table that is not registered"
        )
    return ta.model_copy(update={"table_id": resolved})


async def _load_after_tables(  # REQ-1919
    conn: "Connection", config: ProvisaConfig, *, refs_before: Any, defer_unregistered: bool
) -> None:
    """The apply's steps after the tables: what refers to tables."""

    # 6. Relationships (tables must exist first).
    await _upsert_relationships(conn, config, defer_unregistered=defer_unregistered)

    # 6.5 Metrics (REQ-1317/REQ-1320): governed metric definitions; fact-derived ones preserved.
    await _upsert_metrics(conn, config)

    # 6.6 Tags (REQ-1373/REQ-1377): registry rows then assignments — tables and relationships
    # must exist first so assignment FKs resolve. System tags are code-defined intrinsics
    # (models.SYSTEM_TAGS) and never stored, so a config naming one upserts nothing.
    from provisa.core.models import SYSTEM_TAG_IDS, base_tag_id

    for tg in config.tags:
        # base id: a config naming "entity:customer" still names the code-defined `entity`
        # tag, and storing it would shadow the intrinsic with a user row (REQ-1467).
        if base_tag_id(tg.id) in SYSTEM_TAG_IDS:
            continue
        await tag_repo.upsert(conn, tg)
    for ta in config.tag_assignments:
        if ta.object_type == "term":
            continue  # REQ-2145: placed once the glossary has settled, below
        await tag_repo.assign(conn, await _resolve_tag_assignment_table(conn, ta))

    # 7. RLS rules (tables + roles must exist first)
    for rule in config.rls_rules:
        await rls_repo.upsert(conn, rule)

    # 8. Tracked DB functions
    for func in config.functions:
        await function_repo.upsert_function(conn, func, return_schema=func.return_schema)

    # 9. Tracked webhooks. Config is the trusted source of truth, so a config-declared webhook is
    # pre-approved (REQ-209): without an 'executed' creation_request the schema gate in
    # app_loaders would silently exclude it from GraphQL forever (DB functions load ungated).
    from provisa.core.repositories import creation_request as cr_repo

    for wh in config.written.webhooks:  # as written: a credential in its URL stays a reference
        await function_repo.upsert_webhook(conn, wh)
        await cr_repo.ensure_executed(conn, "webhook", wh.name, "config")

    # 9b. Scheduled triggers (REQ-1003): the file's are the loading org's -- at boot, the
    # deployment org's -- written to its model store, never scheduled from the file. Its own
    # admin-made triggers are left as they are.
    from provisa.core.repositories import scheduled_trigger as trigger_repo

    await trigger_repo.load_from_config(conn, list(config.scheduled_triggers))

    # 10. Policy sweep: dynamically-registered rows (openapi/hasura/graphql_remote) are not
    # in this config file, so the model validator can't catch them. In single-domain mode any
    # surviving row with a foreign domain_id is a hard error — re-register the offending source.
    if domain_policy.single_domain():
        await _validate_existing_domains(conn, config.naming.default_domain)

    # 10.5 Glossary term edges (REQ-1641): resolved by name after every term this config
    # declares or derives exists — an abstract term with no edge to a grounded term can never
    # be live, so this is what actually connects it rather than the name-collision trick of
    # declaring a term with the same name as a derived one.
    if config.glossary_terms:
        term_ids = {
            row.name: row.id
            for row in (
                await conn.execute_core(select(glossary_terms.c.id, glossary_terms.c.name))
            ).fetchall()
        }
        for gt in config.glossary_terms:
            # REQ-1844: every term name in the catalog is lowercase, and the declared term was
            # stored that way (upsert_declared_term), so it and its edges' targets are looked up
            # that way too — a name the file wrote with a capital letter is the same term.
            from_id = term_ids[gt.name.strip().lower()]
            for edge in gt.edges:
                target = edge.to.strip().lower()
                if target not in term_ids:
                    raise ValueError(
                        f"glossary term {gt.name!r} has an edge to {edge.to!r}, "
                        "which does not exist in this config or the catalog"
                    )
                await glossary_repo.add_edge(conn, from_id, term_ids[target], edge.rel_type)

    # 10.6 Class terms (REQ-2141): resolved by name, like the edges above, once every term the
    # config declares or derives exists and every table has its columns (a class needs a key).
    await _declare_class_terms(conn, config)
    # The relationships this load replaced (step 10) and the class terms it declared re-settle
    # every key ref on its Class (REQ-2141).
    await glossary_repo.rehome_keys(
        conn,
        {r.id for r in (await conn.execute_core(select(registered_tables.c.id))).fetchall()},
    )
    # 10.7 Same-entity relationships (REQ-2141): held to their rule once both their tables have
    # the class terms this config declares (the relationships were saved before them, at 10).
    await _check_identities(conn, config)
    # 10.8 Term statuses (REQ-2144): what the config asserts of each declared term's status,
    # once its edges, refs and class groundings make its admission what it will be.
    await _declare_term_statuses(conn, config)

    # 11. Glossary settle (REQ-1387): purged/replaced tables cascaded their term refs away
    # before the upserts above could relink them; runs LAST so a rename that re-registers the
    # same column names revives its terms instead of losing their definitions to an early sweep.
    await glossary_repo.sweep_refless_terms(conn, before=refs_before)

    # 12. Tags on glossary terms (REQ-2145), once every term and its refs have settled.
    for ta in config.tag_assignments:
        if ta.object_type == "term":
            await tag_repo.assign(conn, await _resolve_tag_assignment_term(conn, ta))


async def _resolve_tag_assignment_term(
    conn: "Connection", ta: TagAssignment
) -> TagAssignment:  # REQ-2145
    """The term assignment with ``term_id`` resolved by its term's name, refused as the term's own
    tagging refuses it (provisa.core.term_tags.refuse_term_tag)."""
    from provisa.core.term_tags import TermTagRefused, refuse_term_tag

    if not ta.term:
        raise ValueError(f"tag assignment {ta.tag_id!r} on a term names no term")
    name = ta.term.strip().lower()  # REQ-1844: every term name in the catalog is lowercase
    row = (
        await conn.execute_core(select(glossary_terms.c.id).where(glossary_terms.c.name == name))
    ).fetchone()
    if row is None:
        raise ValueError(
            f"tag assignment {ta.tag_id!r} names term {ta.term!r}, which does not exist in this "
            "config or the catalog"
        )
    try:
        await refuse_term_tag(conn, row.id, ta.tag_id)
    except TermTagRefused as refused:
        raise ValueError(f"tag {ta.tag_id!r} cannot go on term {ta.term!r}: {refused}") from None
    return ta.model_copy(update={"term_id": row.id})


async def _declare_term_statuses(conn: "Connection", config: ProvisaConfig) -> None:
    import datetime

    stated = [
        t
        for t in config.glossary_terms
        if t.deprecate is not None or t.retire is not None or t.restore
    ]
    if not stated:
        return
    term_ids = {
        row.name: row.id
        for row in (
            await conn.execute_core(select(glossary_terms.c.id, glossary_terms.c.name))
        ).fetchall()
    }
    statuses = await glossary_repo.term_statuses(conn)
    today = datetime.date.today()
    for gt in stated:
        term_id = term_ids[gt.name.strip().lower()]
        try:
            if gt.restore:
                if statuses[term_id] in ("deprecated", "retired"):
                    await glossary_repo.restore_term(conn, term_id, by=None)
            elif gt.retire is not None:
                successor = None
                if gt.retire.successor is not None:
                    named = gt.retire.successor.strip().lower()
                    if named not in term_ids:
                        raise ValueError(
                            f"glossary term {gt.name!r} names {gt.retire.successor!r} as its "
                            "successor, which does not exist in this config or the catalog"
                        )
                    successor = term_ids[named]
                if statuses[term_id] != "retired":
                    await glossary_repo.retire_term(
                        conn, term_id, reason=gt.retire.reason, successor=successor, by=None
                    )
            elif gt.deprecate is not None:
                # Loading the same configuration again changes nothing: a deprecation already
                # stated stands, and one its planned date has retired stays retired.
                if statuses[term_id] == "live":
                    await glossary_repo.deprecate_term(
                        conn,
                        term_id,
                        reason=gt.deprecate.reason,
                        retire_on=gt.deprecate.retire_on,
                        by=None,
                        today=today,
                    )
                elif statuses[term_id] == "draft":
                    raise glossary_repo.StatusRefused(
                        "term_not_live", "only a live term is deprecated; this one is draft"
                    )
        except glossary_repo.StatusRefused as exc:
            raise ValueError(f"glossary term {gt.name!r}: {exc}") from exc


async def _check_identities(conn: "Connection", config: ProvisaConfig) -> None:
    from provisa.core.repositories.identity import stored_refusal

    for rel in config.relationships:
        if rel.same_entity:
            refused = await stored_refusal(conn, rel.id)
            if refused is not None:
                raise ValueError(refused.message)


async def _declare_class_terms(conn: "Connection", config: ProvisaConfig) -> None:
    declared = [t for t in config.tables if t.class_term is not None]
    if not declared:
        return
    term_ids = {
        row.name: row.id
        for row in (
            await conn.execute_core(select(glossary_terms.c.id, glossary_terms.c.name))
        ).fetchall()
    }
    for table in declared:
        assert table.class_term is not None
        address = f"{table.source_id}.{table.schema_name}.{table.table_name}"
        # REQ-1844: every term name in the catalog is lowercase.
        name = table.class_term.strip().lower()
        if name not in term_ids:
            raise ValueError(
                f"table {address} names {table.class_term!r} as its class term, "
                "which does not exist in this config or the catalog"
            )
        row = (
            await conn.execute_core(
                select(registered_tables.c.id).where(
                    (registered_tables.c.source_id == table.source_id)
                    & (registered_tables.c.schema_name == table.schema_name)
                    & (registered_tables.c.table_name == table.table_name)
                )
            )
        ).fetchone()
        assert row is not None, f"table {address} was upserted by this load"
        try:
            await glossary_repo.declare_class_term(conn, row.id, term_ids[name])
        except (
            glossary_repo.TableHasNoPrimaryKey,
            glossary_repo.TermIsProperty,
            TableInIdentity,
        ) as exc:
            raise ValueError(f"table {address} cannot have a class term: {exc}") from exc


async def _apply_in_txn(  # REQ-012, REQ-013, REQ-016, REQ-041, REQ-250, REQ-1266, REQ-1919
    config: ProvisaConfig, conn: "Connection", engine: Any = None, *, defer_unregistered: bool
) -> list[str]:
    """Add and update, through the model store, everything the config declares, within the
    caller's transaction. Nothing is removed. ``engine`` decides only the names tables are
    registered under (REQ-471); no catalog is issued here (:func:`register_sources`).

    Returns the source ids whose change feed could not be reached (REQ-1861).
    """
    # REQ-1591: a term's domains are derived by joining its refs to registered_tables, so the
    # snapshot step 11's sweep needs is taken here — before the column replace in _upsert_tables
    # removes the very rows it reads.
    refs_before = await glossary_repo.before_ref_removal(conn)

    # 0. Regions and their stores (REQ-1921/1922), before the sources and tables that name them.
    from provisa.core.repositories import region as region_repo

    for store in config.stores:
        await region_repo.upsert_store(conn, store)
    for selected in config.regions:
        await region_repo.upsert_region(conn, selected)

    # 1. Sources, and the Kafka sources with their topics (REQ-147)
    await _upsert_sources(conn, config)
    from provisa.core.repositories import kafka_source as kafka_repo

    for spec in config.written.kafka_sources:
        await kafka_repo.upsert(conn, spec)

    # 2. Domains
    if domain_policy.single_domain():
        # Seed the implicit single-domain bucket so registered_tables FK resolves.
        await domain_repo.upsert(conn, Domain(id=config.naming.default_domain))
    for dom in config.domains:
        await domain_repo.upsert(conn, dom)

    # 3. Naming rules
    await _upsert_naming_rules(conn, config)

    # 4. Roles (before tables/RLS so FK refs exist)
    for role in config.roles:
        # A role the config declares is the deployment's own definition, as a seeded role is:
        # it carries no org, and the admin surfaces do not delete it.
        await role_repo.upsert(conn, role, org_id=None)

    # 4.5 Data products (before tables so product_id FK refs exist)  # REQ-1634
    for dp in config.data_products:
        await data_product_repo.upsert(conn, dp)

    # 4.6 Glossary terms (after domains, so declared scope names something real)  # REQ-1641
    # upsert_declared_term upserts by name (its unique key); an apply removes nothing.
    for gt in config.glossary_terms:
        await glossary_repo.upsert_declared_term(
            conn, gt.name, definition=gt.definition, domains=set(gt.domains)
        )

    # 4.7 Calendars (REQ-2120): before the tables, whose views name one as their mv_calendar.
    from provisa.core.repositories import calendar as calendar_repo
    from provisa.events.calendars import calendar_of

    for declared in config.calendars:
        fields = declared.model_dump()
        # Validated when the config was parsed (CalendarConfig); the column holds a date.
        await calendar_repo.upsert(
            conn, {**fields, "retail_anchor": calendar_of(fields).retail_anchor}
        )

    # 5. Tables + columns
    openapi_specs = _load_openapi_specs(config)
    _validate_table_live_delivery(config)
    _validate_change_signal(config)
    _validate_dq_contracts(config)
    from provisa.profiler.registration import validate_config as _validate_profilers  # noqa: PLC0415

    _validate_profilers(config)  # REQ-1934
    _validate_probe_type(config)
    _validate_watermark_columns(config)
    _validate_neo4j_sources(config)
    _validate_table_arguments(config)
    _validate_row_materialize(config)
    _validate_file_globs(config)
    _validate_delta(config)
    _validate_role_ttl(config)
    _validate_triggers(config)
    _validate_paging(config)
    _validate_replicate(config)
    _validate_landing_ttl(config)
    failed_feeds = await _check_change_feeds(config)
    await _upsert_tables(conn, engine, config, openapi_specs)
    await _load_after_tables(
        conn, config, refs_before=refs_before, defer_unregistered=defer_unregistered
    )
    return failed_feeds


# REQ-824: non-PG RDBMS have no native push mechanism; they reach CDC only through a
# source-level Debezium transport block. This is also the exhaustive set of source
# types on which a cdc block is meaningful, alongside "postgresql" (native LISTEN/NOTIFY).
_CDC_DEBEZIUM_SOURCE_TYPES = {"mysql", "mariadb", "sqlserver", "oracle"}
_CDC_BLOCK_ALLOWED_SOURCE_TYPES = _CDC_DEBEZIUM_SOURCE_TYPES | {"postgresql"}

# REQ-814: which live strategies each source type can use. Dispatch is on strategy,
# not source_type; this matrix capability-gates strategy by the source's real push
# ability. Any pollable federated SQL source may use "poll".
_STRATEGIES_BY_SOURCE_TYPE: dict[str, set[str]] = {
    "postgresql": {"poll", "native", "debezium", "kafka"},
    "mongodb": {"poll", "native"},
    "kafka": {"kafka"},
    **{t: {"poll", "debezium", "kafka"} for t in _CDC_DEBEZIUM_SOURCE_TYPES},
}
# Strategies whose delta-transport is inherited from Source.cdc (REQ-824), and so
# require the source to declare a cdc block — except on a "kafka" source type, whose
# transport is the source's own Kafka connection.
_TRANSPORT_STRATEGIES = {"debezium", "kafka"}


def _allowed_strategies(source_type: str | None) -> set[str]:
    # Default: only watermark polling through the engine (any federated SQL source).
    return _STRATEGIES_BY_SOURCE_TYPE.get(source_type or "", {"poll"})


def _validate_table_live_delivery(config) -> None:
    """Validate live change-feed config on all tables (REQ-282–287, REQ-813, REQ-814, REQ-824)."""
    for source in config.sources:
        # REQ-824: a source-level cdc block only makes sense on CDC-capable RDBMS sources.
        if getattr(source, "cdc", None) is not None:
            stype = getattr(source, "type", None)
            if stype not in _CDC_BLOCK_ALLOWED_SOURCE_TYPES:
                raise ValueError(
                    f"Source {source.id!r}: cdc transport config not supported for source type "
                    f"{stype!r} (only PostgreSQL and Debezium-captured RDBMS)"
                )
            # REQ-1951: how its schema registry is reached is refused by name here as it is when
            # the source is saved through the admin API (RegistrySettingRefused is a ValueError).
            from provisa.kafka.avro_registry import validate_settings  # noqa: PLC0415

            validate_settings(source.id, source.cdc.model_dump())

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        if table.live is None:
            continue
        strategy = table.live.strategy
        source = sources_by_id.get(table.source_id)
        stype = getattr(source, "type", None) if source else None

        if strategy == "poll" and not table.live.watermark_column:
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy=poll requires watermark_column"
            )
        if strategy == "kafka" and table.live.kafka is None and stype != "kafka":
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy=kafka requires a kafka params block"
            )
        if source is None:
            continue
        # REQ-814: capability-gate strategy by source type.
        if strategy not in _allowed_strategies(stype):
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy={strategy!r} not supported for source "
                f"type {stype!r} (allowed: {sorted(_allowed_strategies(stype))})"
            )
        # REQ-824: debezium/kafka transport on an RDBMS source is inherited from the
        # source's cdc block — require it. (A "kafka" source type carries its own transport.)
        if (
            strategy in _TRANSPORT_STRATEGIES
            and stype in _CDC_DEBEZIUM_SOURCE_TYPES | {"postgresql"}
            and getattr(source, "cdc", None) is None
        ):
            raise ValueError(
                f"Table {table.table_name!r}: live.strategy={strategy} on source {source.id!r} "
                f"({stype}) requires source-level cdc transport (bootstrap_servers/topic_prefix)"
            )


# REQ-925: canonical IR types a watermark may take. A watermark drives WHERE wm > cursor
# incremental reads, so it MUST be monotonic non-decreasing: an incrementing integer or a
# temporal column. Text/float/boolean/uuid/bytea/numeric are rejected — they give no reliable
# ordering for delta reads. (numeric is excluded: a scale/rounding column is not an increment.)
_MONOTONIC_WATERMARK_IR = frozenset(
    {"smallint", "integer", "bigint", "timestamp", "timestamptz", "date", "time"}
)


def _validate_watermark_columns(config) -> None:  # REQ-924, REQ-925
    """A table's watermark must be one of its OWN columns (REQ-924) and monotonic (REQ-925).

    The watermark is the top-level ``Table.watermark_column`` or, when set on live config, the table's
    ``live.watermark_column``. It is rejected when it names a column the table does not have, or
    when that column's type is not a monotonic (integer/temporal) IR type. Type is checked only
    when the column's ``data_type`` is known — introspection fills it for sources whose columns are
    reflected at startup, and the selection is re-validated then; existence is always checked when
    the table declares columns."""
    from provisa.core.ir_types import to_ir  # noqa: PLC0415

    for table in config.tables:
        live = getattr(table, "live", None)
        watermark = table.watermark_column or (live.watermark_column if live is not None else None)
        if not watermark:
            continue
        columns = list(table.columns or [])
        if not columns:
            continue  # columns filled by introspection later; re-validated at selection then
        col = next((c for c in columns if c.name == watermark), None)
        if col is None:
            raise ValueError(
                f"Table {table.table_name!r}: watermark_column {watermark!r} is not a column of "
                f"the table (watermark must name an existing column)"
            )
        if col.data_type is None:
            continue  # type not yet resolved (deferred introspection); re-checked at selection
        try:
            ir = to_ir(col.data_type)
        except ValueError:
            ir = None
        if ir not in _MONOTONIC_WATERMARK_IR:
            raise ValueError(
                f"Table {table.table_name!r}: watermark_column {watermark!r} has type "
                f"{col.data_type!r} which is not monotonic non-decreasing; a watermark must be a "
                f"timestamp/date/time or an incrementing integer"
            )


def _validate_change_signal(config) -> None:  # REQ-932
    """Capability-gate change_signal. Push transports (debezium/kafka) require the source's cdc
    block (a "kafka" source type carries its own transport). The signal resolves table → source."""
    from provisa.core.change_signal import (  # noqa: PLC0415
        kafka_topic_refusal,
        native_signal_refusal,
        resolve,
    )

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        source = sources_by_id.get(table.source_id)
        source_signal = getattr(source, "change_signal", None) if source else None
        sig = resolve(getattr(table, "change_signal", None), source_signal)
        stype = getattr(source, "type", None) if source else None
        stype_val = getattr(stype, "value", stype)
        refused = native_signal_refusal(str(stype_val), sig) if source is not None else None
        if refused is None and source is not None:
            live = getattr(table, "live", None)
            kafka = getattr(live, "kafka", None) if live is not None else None
            refused = kafka_topic_refusal(
                str(stype_val), sig, getattr(kafka, "topic", None) if kafka is not None else None
            )
        if refused is not None:
            raise ValueError(f"Table {table.table_name!r}: {refused}")
        if sig not in ("debezium", "kafka"):
            continue
        if stype_val == "kafka":
            continue
        if source is None or getattr(source, "cdc", None) is None:
            raise ValueError(
                f"Table {table.table_name!r}: change_signal={sig} requires source-level cdc "
                f"transport on {table.source_id!r} (bootstrap_servers/topic_prefix)"
            )


def _validate_dq_contracts(config) -> None:  # REQ-1443
    """Validate data-quality checker tables and REPLACE their columns with the shipped schema.

    The per-table derivation lives in :func:`provisa.dq.registration.derive_checker_table`, shared
    with the admin registerTable mutation so a checker table registered through the UI comes out
    identical to the same table written in YAML. What this adds is the whole-config half: a
    ``dq_contract`` on a non-checker table is a config error, because its rows would come from
    wherever that source's loader fetched them and the results schema does not describe those.

    Whether the dataset names a governed table is checked after the schema build
    (``app_loaders._build_and_register_schemas``): the dataset carries pgwire's semantic names,
    which do not exist until the tables are compiled.
    """
    from provisa.dq.contract import CHECKERS  # noqa: PLC0415
    from provisa.dq.registration import (  # noqa: PLC0415
        derive_checker_table,
        is_checker_source_type,
        require_scan_interval,
    )

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        source = sources_by_id.get(table.source_id)
        stype = getattr(source, "type", None) if source else None
        if not is_checker_source_type(stype):
            if table.dq_contract is not None:
                raise ValueError(
                    f"Table {table.table_name!r}: dq_contract is only valid on a data-quality "
                    f"checker source ({sorted(CHECKERS)}), not on source type "
                    f"{str(getattr(stype, 'value', stype))!r}"
                )
            continue
        derive_checker_table(table, stype)
        require_scan_interval(table)


def _validate_probe_type(config) -> None:  # REQ-982
    """Capability-gate probe_type. A table's probe_type must be supported by its source's capability
    class (probe_capabilities); ttl cadence forces none. Delegates to ``resolve_probe_type``, which
    raises on an unsupported type or a ttl+explicit-type mismatch — surfaced as a config error."""
    from provisa.core.change_signal import resolve  # noqa: PLC0415
    from provisa.events.probes import resolve_probe_type  # noqa: PLC0415

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        if getattr(table, "probe_type", None) is None:
            continue  # unset → resolved per class at wiring time; nothing to reject
        source = sources_by_id.get(table.source_id)
        source_signal = getattr(source, "change_signal", None) if source else None
        sig = resolve(getattr(table, "change_signal", None), source_signal)
        stype = getattr(source, "type", None) if source else None
        stype_val = getattr(stype, "value", stype)
        try:
            resolve_probe_type(
                table.probe_type,
                source_type=str(stype_val),
                change_signal=sig,
                has_watermark=getattr(table, "watermark_column", None) is not None,
            )
        except ValueError as exc:
            raise ValueError(f"Table {table.table_name!r}: {exc}") from exc


def _validate_role_ttl(config) -> None:  # REQ-1907
    """Every role a table's role_ttl names must be a declared (or system) role — a TTL for a role
    that does not exist is an operator typo, never silently ignored."""
    known = {r.id for r in config.roles} | set(SYSTEM_ROLE_IDS)
    for table in config.tables:
        unknown = sorted(set(table.role_ttl) - known)
        if unknown:
            raise ValueError(
                f"table {table.table_name!r}: role_ttl names unknown role(s) {unknown}"
            )


def _validate_triggers(config) -> None:  # REQ-2054
    """Each trigger is on a registered table, runs a registered command that takes only what it
    gives, as a role that exists: refused at load by name, never fired to fail."""
    from provisa.core.models import trigger_refusal

    commands = {f.name: [a.name for a in f.arguments] for f in config.functions}
    tables = {f"{t.source_id}.{t.schema_name}.{t.table_name}" for t in config.tables}
    roles = {r.id for r in config.roles} | set(SYSTEM_ROLE_IDS)
    for trigger in config.scheduled_triggers:
        refused = trigger_refusal(trigger, commands=commands, tables=tables, roles=roles)
        if refused is not None:
            raise ValueError(refused)


def _validate_paging(config) -> None:  # REQ-318
    """A table's paging must suit what reads the table, and a connection table's max_rows may only
    lower the operator's graphql_remote.max_rows -- refused at load as it is at save."""
    from provisa.core.paging import CONNECTION, ENDPOINT, check_paging, paging_kind

    kinds = {s.id: s.type.value for s in config.sources}
    for table in config.tables:
        if table.pagination is None:
            continue
        if table.source_id not in kinds:
            # A table of a source registered in the control plane only (createSource): what reads
            # it is known from that row, where the table is registered (_upsert_tables). The
            # operator's ceiling holds for it here all the same.
            check_paging(
                table.pagination,
                table=table.table_name,
                kind=CONNECTION if table.pagination.max_rows is not None else ENDPOINT,
                ceiling_rows=config.graphql_remote.max_rows,
            )
            continue
        check_paging(
            table.pagination,
            table=table.table_name,
            kind=paging_kind(kinds[table.source_id]),
            ceiling_rows=config.graphql_remote.max_rows,
        )


async def _check_change_feeds(config) -> list[str]:  # REQ-1861
    """A MongoDB table that follows its source's change feed needs a server that serves change
    streams: a standalone one is refused by name and the load fails. A server that cannot be
    reached is that source's ordinary unreachable failure, reported with the sources whose engine
    registration failed (the ids returned), never a pass."""
    from provisa.mongodb.change_feed import (
        FEED_REFUSALS,
        follows_change_feed,
        require_change_feed,
    )

    by_id = {s.id: s for s in config.sources}
    following = {
        t.source_id
        for t in config.tables
        if t.source_id in by_id
        and follows_change_feed(
            by_id[t.source_id].type.value, t.change_signal, by_id[t.source_id].change_signal
        )
    }
    unreachable: list[str] = []
    for source_id in sorted(following):
        try:
            await require_change_feed(by_id[source_id])
        except FEED_REFUSALS:
            raise
        except Exception:
            log.exception("source %r: its change-stream support could not be checked", source_id)
            unreachable.append(source_id)
    return unreachable


def _validate_replicate(config) -> None:  # REQ-826
    """A table's RESOLVED settings (its own, else its source's) may not pair load_protected with
    replicate -1 (never): a load-protected table is never read live. The models refuse the pair
    on one object; this judges a table against the source it inherits from."""
    from provisa.core.replicate import contradiction, resolved_load_protected, resolved_replicate

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        source = sources_by_id.get(table.source_id)
        if source is None:
            continue  # registered outside this file: judged at save (the admin mutations)
        refused = contradiction(
            resolved_replicate(source, table), resolved_load_protected(source, table)
        )
        if refused is not None:
            raise ValueError(f"table {table.table_name!r} (source {source.id!r}): {refused}")


def _validate_landing_ttl(config) -> None:  # REQ-1907
    """A table config says is replicated (materialize, row_materialize, or a resolved
    replicate 0 / N > 0 / load_protected) with change_signal ttl / ttl_probe (table's own, else its
    source's) needs a cache_ttl on the table or its source: it is that signal's refresh clock
    (REQ-930), and the global response-cache default_ttl is never a landing clock (REQ-1907,
    amended 2026-09-30). A table that lands only because the engine cannot reach its source is
    judged on the read path (role_ttl.require_landing_ttl)."""
    from provisa.core.source_registry import is_live_only
    from provisa.federation.role_ttl import lands_from_config, missing_landing_ttl

    sources_by_id = {s.id: s for s in config.sources}
    for source in config.sources:
        stype = str(getattr(source.type, "value", source.type))
        if is_live_only(stype) and source.replicate is not None:  # REQ-2000
            raise ValueError(
                f"Source {source.id!r}: {stype} is live-only: its tables are read live, "
                "never replicated"
            )
    for table in config.tables:
        source = sources_by_id.get(table.source_id)
        if source is None:
            # A table whose source is registered outside this file (_upsert_single_table takes
            # src=None): its source's signal and cache_ttl are not known here; the read path
            # (role_ttl.require_landing_ttl) enforces the same rule against the registered source.
            continue
        stype = str(getattr(source.type, "value", source.type))
        if is_live_only(stype):  # REQ-2000: no replica, so no landing clock
            if table.replicate is not None or table.row_materialize:
                raise ValueError(
                    f"Table {table.table_name!r}: {stype} is live-only: its tables are read "
                    "live, never replicated"
                )
            continue
        if not lands_from_config(
            materialize=table.materialize,
            row_materialize=table.row_materialize,
            table_replicate=table.replicate,
            source_replicate=source.replicate,
            table_load_protected=table.load_protected,
            source_load_protected=source.load_protected,
        ):
            continue
        err = missing_landing_ttl(
            table.change_signal,
            source.change_signal,
            table.cache_ttl,
            source.cache_ttl,
            gated=source.freshness_gate,
        )
        if err is not None:
            raise ValueError(f"table {table.table_name!r} (source {source.id!r}): {err}")


def _validate_delta(config) -> None:  # REQ-874
    """A table with a delta reload must be replicated and must not also carry a probe change
    signal — the delta query IS the probe for a monotonic cursor, so the two are mutually
    exclusive per entry (Table's own validator covers the delta's own shape; this is the
    cross-table half that needs the source)."""
    from provisa.core.change_signal import resolve as _resolve_signal
    from provisa.core.replicate import resolved_replicate

    from provisa.federation.delta import delta_source_supported

    by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        if getattr(table, "delta", None) is None:
            continue
        source = by_id.get(table.source_id)
        if source is None:
            continue
        # REQ-874: the delta apply path is a generated SQL delta on the source's native pool, so it
        # is defined only for SQL source types; a delta on any other type is refused by name (it
        # would otherwise silently whole-rebuild every time).
        if not delta_source_supported(source.type.value):
            raise ValueError(
                f"table {table.table_name!r}: delta replication is only available for SQL sources, "
                f"not {source.type.value!r}"
            )
        if resolved_replicate(source, table) == -1:
            raise ValueError(
                f"table {table.table_name!r}: delta reload needs the table replicated "
                "(replicate is -1/Never)"
            )
        signal = _resolve_signal(table.change_signal, source.change_signal)
        if signal in ("probe", "ttl_probe"):
            raise ValueError(
                f"table {table.table_name!r}: delta and a probe change_signal are mutually "
                f"exclusive — the delta query is the probe (change_signal is {signal!r})"
            )


def _validate_file_globs(config) -> None:  # REQ-788
    """A files table that declares ``file_glob`` is one logical table over the files the glob
    matches under its source's path: they must share a column set, else ``schema.file_columns_differ``
    by name. A glob that matches nothing is a configuration error. The source must be a files
    source (the only kind the glob read is defined for)."""
    from provisa.file_source.files_glob import columns_of_file, matched_files, validate_glob_table

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        glob = getattr(table, "file_glob", None)
        if not glob:
            continue
        source = sources_by_id.get(table.source_id)
        if source is None:
            continue  # a table naming no declared source is caught by the FK/registration check
        src_type = getattr(getattr(source, "type", None), "value", None)
        if src_type not in ("files", "csv", "parquet"):
            raise ValueError(
                f"table {table.table_name!r}: file_glob is defined only for a files source, "
                f"not {src_type!r}"
            )
        from provisa.core.secrets import resolve_secrets

        files = matched_files(resolve_secrets(source.path or ""), glob)
        validate_glob_table(glob, files, columns_of_file)


#: REQ-1915: source types a table cannot be replicated row by row from, and why. An API source is
#: filtered by its own parameters (its parameter sets are its row-level reads); govdata is read live
#: on every request and never replicated; the remaining types have no connector or driver that
#: reaches them yet (REQ-1874), so nothing can read their rows at all.
def _table_fetches_by_keys(source: Any, table: Any) -> bool:  # REQ-1865
    """Whether a remote GraphQL ``table`` fetches its rows by its primary key: the fetch its
    spec, stored on the source when it was registered, names (graphql_remote.by_keys)."""
    from provisa.graphql_remote.by_keys import fetches_by_keys

    pk_columns = [c.name for c in table.columns if c.is_primary_key]
    return fetches_by_keys(source.mapping, table.table_name, pk_columns)


def _validate_row_materialize(config) -> None:  # REQ-1865
    """row_materialize needs a resolved cache_ttl (table's own OR inherited from its source) to
    drive each cached row's independent freshness clock (Table's own model_validator cannot see
    Source, so this half of the check lives here, the same place _upsert_single_table/
    _handle_neo4j_table already combine ``table.cache_ttl or source.cache_ttl``). Also enforces the
    query-API keyed-fetch registration requirement (design doc section 3d, revised): a
    query_template table's keyed fetch wraps the template itself with a filter on one of its OWN
    projected properties (``provisa/cypher/query_template_filter.py``) rather than requiring the
    author to hand-splice a ``$keys`` placeholder — so the check here is that the table's own
    declared PK column is actually among what the template projects, and that this source type has
    a keyed-fetch translation wired at all (today: neo4j only — sparql has no parser/AST to safely
    resolve a projected property against, confirmed absent). Both are registration-time
    ValueErrors, never a silent fallback to an undefined TTL or an unbounded per-lookup scan."""
    from provisa.core.source_registry import ROW_FETCH_BY_KEYS_PER_TABLE, row_materialize_refusal
    from provisa.cypher.query_template_filter import (
        ProjectionFilterError,
        resolve_projected_property,
    )

    sources_by_id = {s.id: s for s in config.sources}
    for table in config.tables:
        if not getattr(table, "row_materialize", False):
            continue
        source = sources_by_id.get(table.source_id)
        if source is not None:
            stype = source.type.value if hasattr(source.type, "value") else str(source.type)
            # REQ-1865, REQ-1915: the type's own declaration (core.source_registry), and for a
            # type whose fetch by keys is each table's own, this table's.
            refused = row_materialize_refusal(
                table.table_name,
                stype,
                table_fetches_by_keys=_table_fetches_by_keys(source, table)
                if stype in ROW_FETCH_BY_KEYS_PER_TABLE
                else None,
            )
            if refused is not None:
                raise ValueError(refused)
        eff_ttl = (
            table.cache_ttl
            if table.cache_ttl is not None
            else (source.cache_ttl if source is not None else None)
        )
        if eff_ttl is None:
            raise ValueError(
                f"table {table.table_name!r}: row_materialize=True requires a resolved cache_ttl "
                f"(own or inherited from source {table.source_id!r}) to drive each cached row's "
                "freshness clock"
            )
        if table.query_template is not None:
            _raw_type = getattr(source, "type", None) if source is not None else None
            src_type = (
                _raw_type.value
                if _raw_type is not None and hasattr(_raw_type, "value")
                else _raw_type
            )
            if src_type != "neo4j":
                raise ValueError(
                    f"table {table.table_name!r}: row_materialize=True on a query_template table "
                    f"has no keyed-fetch translation for source type {src_type!r} (today: neo4j "
                    "only — sparql has no parser/AST to safely resolve a projected property or "
                    "splice a filter against)"
                )
            pk_columns = [c.name for c in table.columns if c.is_primary_key]
            if len(pk_columns) != 1:
                raise ValueError(
                    f"table {table.table_name!r}: row_materialize=True on a query_template table "
                    f"requires exactly one PK column for keyed fetch, got {pk_columns!r} "
                    "(composite-PK keyed fetch is not implemented)"
                )
            try:
                resolve_projected_property(table.query_template, pk_columns[0])
            except ProjectionFilterError as exc:
                raise ValueError(
                    f"table {table.table_name!r}: row_materialize=True requires its PK column "
                    f"{pk_columns[0]!r} to be among query_template's own projected elements: {exc}"
                ) from exc


async def _validate_existing_domains(conn: "Connection", default_domain: str) -> None:
    result = await conn.execute_core(
        select(
            registered_tables.c.source_id,
            registered_tables.c.schema_name,
            registered_tables.c.table_name,
            registered_tables.c.domain_id,
        ).where(
            registered_tables.c.domain_id != "",
            registered_tables.c.domain_id.not_in([default_domain, "meta", "ops"]),
        )
    )
    rows = [dict(r._mapping) for r in result.fetchall()]
    if rows:
        offenders = ", ".join(
            f"{r['source_id']}.{r['schema_name']}.{r['table_name']}={r['domain_id']!r}"
            for r in rows
        )
        raise RuntimeError(
            f"naming.use_domains=false permits only domain {default_domain!r}; "
            f"re-register these sources: {offenders}"
        )


async def is_seeded(conn: "Connection") -> bool:  # REQ-1919
    """Whether a configuration has seeded this model store."""
    return (await conn.execute_core(select(model_seed.c.id))).fetchone() is not None


async def _mark_seeded(conn: "Connection") -> None:
    if not await is_seeded(conn):
        await conn.execute_core(insert(model_seed).values(id=1))


async def reset_model(conn: "Connection") -> None:  # REQ-1919
    """Remove the org's model — everything a configuration can declare, and everything an admin
    made — leaving what the deployment seeds into every store (the built-in sources and their
    tables, the system domains, the seeded roles and the people holding them). Within the
    caller's transaction. A demo organisation is rebuilt from its configuration by this followed
    by an apply (:func:`rebuild_from_config`)."""
    from sqlalchemy import delete

    from provisa.core.db import SEEDED_DOMAIN_IDS, SEEDED_ROLE_IDS, SEEDED_SOURCE_IDS
    from provisa.core.models import DERIVED_SOURCE_ID, DERIVED_TAG_IDS, SYSTEM_TAG_IDS
    from provisa.core.repositories.integrity import ObjectRef, discard
    from provisa.core.schema_org import (
        calendars,
        data_products,
        domains,
        glossary_term_refs,
        glossary_terms,
        kafka_sources,
        metrics,
        org_regions,
        relationships,
        rls_rules,
        roles,
        scheduled_triggers,
        sources,
        stores,
        synthetic_datasets,
        tag_assignments,
        tags,
        tracked_functions,
        tracked_webhooks,
    )

    async def _ids(statement: Any) -> list[Any]:
        return [r[0] for r in (await conn.execute_core(statement)).fetchall()]

    # REQ-1591: taken before the tables go — a term's domains are derived from its refs.
    refs_before = await glossary_repo.before_ref_removal(conn)

    seeded_sources = SEEDED_SOURCE_IDS - {DERIVED_SOURCE_ID}
    internal = select(registered_tables.c.id).where(
        registered_tables.c.source_id.in_(sorted(seeded_sources))
        | registered_tables.c.domain_id.in_(["meta", "ops"])
    )
    internal_ids = set(await _ids(internal))
    model_tables = [
        t
        for t in await _ids(select(registered_tables.c.id).order_by(registered_tables.c.id.desc()))
        if t not in internal_ids
    ]

    def _off_internal(column: Any) -> Any:
        return column.is_(None) | column.not_in(sorted(internal_ids) or [-1])

    order: list[tuple[str, list[Any]]] = [
        ("synthetic_dataset", await _ids(select(synthetic_datasets.c.id))),
        (
            "tag_assignment",
            await _ids(
                select(tag_assignments.c.id).where(_off_internal(tag_assignments.c.table_id))
            ),
        ),
        (
            "row_filter",
            await _ids(
                select(rls_rules.c.id).where(
                    _off_internal(rls_rules.c.table_id),
                    rls_rules.c.domain_id.is_(None) | rls_rules.c.domain_id.not_in(["meta", "ops"]),
                )
            ),
        ),
        (
            "relationship",
            await _ids(
                select(relationships.c.id).where(
                    _off_internal(relationships.c.source_table_id),
                    _off_internal(relationships.c.target_table_id),
                )
            ),
        ),
        ("metric", await _ids(select(metrics.c.name).where(metrics.c.from_fact.is_(None)))),
        (
            "command",
            await _ids(
                select(tracked_functions.c.name).where(
                    tracked_functions.c.source_id.not_in(sorted(seeded_sources))
                )
            ),
        ),
        ("webhook", await _ids(select(tracked_webhooks.c.name))),
        ("table", model_tables),
        (
            "data_product",
            await _ids(
                select(data_products.c.id).where(data_products.c.domain_id.not_in(["meta", "ops"]))
            ),
        ),
        (
            "glossary_term",
            # A term holding no column ref is the config's or a curator's (a Concept or a Class);
            # a Property is derived from its columns and goes with its tables (REQ-2141).
            await _ids(
                select(glossary_terms.c.id).where(
                    glossary_terms.c.id.not_in(select(glossary_term_refs.c.term_id))
                )
            ),
        ),
        (
            "tag",
            await _ids(
                select(tags.c.id).where(tags.c.id.not_in([*SYSTEM_TAG_IDS, *DERIVED_TAG_IDS]))
            ),
        ),
        (
            "source",
            await _ids(select(sources.c.id).where(sources.c.id.not_in(sorted(SEEDED_SOURCE_IDS)))),
        ),
        # A calendar is removed by name, every version at once (integrity.discard).
        ("calendar", await _ids(select(calendars.c.name).distinct())),
    ]
    for kind, idents in order:
        for ident in idents:
            if kind == "table":
                await table_repo.discard(conn, ident)  # its endpoint goes with it
            elif kind == "source":
                await source_repo.discard(conn, ident)
            else:
                await discard(conn, ObjectRef(kind, ident))
    await conn.execute_core(delete(scheduled_triggers))
    await conn.execute_core(delete(kafka_sources))
    await conn.execute_core(delete(naming_rules))
    # Roles a child before its parent; the seeded ones and those who hold them stay.
    remaining = {
        r.id: r.parent_role_id
        for r in (
            await conn.execute_core(
                select(roles.c.id, roles.c.parent_role_id).where(
                    roles.c.id.not_in(sorted(SEEDED_ROLE_IDS))
                )
            )
        ).fetchall()
    }
    while remaining:
        leaves = [r for r in remaining if r not in set(remaining.values())]
        for role_id in leaves:
            await role_repo.discard(conn, role_id)
            del remaining[role_id]
    keep_domains = sorted(SEEDED_DOMAIN_IDS | set(domain_policy.system_domain_ids()))
    for domain_id in await _ids(select(domains.c.id).where(domains.c.id.not_in(keep_domains))):
        await domain_repo.discard(conn, domain_id)
    for region_id in await _ids(select(org_regions.c.id)):
        await discard(conn, ObjectRef("region", region_id))
    for store_id in await _ids(select(stores.c.id)):
        await discard(conn, ObjectRef("store", store_id))
    await glossary_repo.sweep_refless_terms(conn, before=refs_before)


async def rebuild_from_config(  # REQ-1919: DEMO ORGANISATIONS ARE THEIR CONFIG
    config: ProvisaConfig, conn: "Connection", engine: Any = None
) -> list[str]:
    """Rebuild a demo organisation's model from its configuration: remove its model
    (:func:`reset_model`) and apply the configuration, in one transaction, so the organisation
    starts exactly as its configuration says. Returns the source ids whose change feed could not
    be reached."""
    # Its remote sources' tables are registered after the apply, which retries their
    # relationships (graphql_remote_router._upsert_relationships_to_semantic_layer).
    return await _apply(
        config,
        conn,
        engine,
        label="demo rebuilt from its configuration",
        defer_unregistered=True,
        reset=True,
    )


async def _apply(
    config: ProvisaConfig,
    conn: "Connection",
    engine: Any,
    *,
    label: str,
    defer_unregistered: bool,
    reset: bool = False,
) -> list[str]:
    from provisa.core import model_change

    # Resolve domain policy before any registration so repos/compilers read one source of truth.
    domain_policy.configure(config.naming.use_domains, config.naming.default_domain)
    # REQ-1524: an apply is one model change, committed once its transaction has committed.
    async with model_change.scope(label):
        model_change.label(label)
        async with conn.transaction():
            # Serialize concurrent applies (parallel workers, test app lifespans) so they do not
            # deadlock on the same rows. A no-op on single-writer backends (SQLite).
            await conn.advisory_xact_lock(7261748190)
            if reset:
                await reset_model(conn)
            failed = await _apply_in_txn(
                config, conn, engine, defer_unregistered=defer_unregistered
            )
            await _mark_seeded(conn)
    return failed


async def apply_config(  # REQ-1919, REQ-164
    config: ProvisaConfig, conn: "Connection", engine: Any = None
) -> list[str]:
    """Apply a configuration as an explicit one-time seed: add and update, through the model
    store (its guards and versions), everything the configuration declares, and remove nothing.
    What it does not mention stays as it is. An admin's act; boot never calls it.

    ``engine`` decides only the names tables are registered under. Returns the source ids whose
    change feed could not be reached.
    """
    # Nothing registers a table after an admin's apply: every relationship registers now.
    return await _apply(
        config, conn, engine, label="configuration applied", defer_unregistered=False
    )


async def seed_config(  # REQ-1919
    config: ProvisaConfig, conn: "Connection", engine: Any = None
) -> bool:
    """Seed the model store from the deployment's configuration file at its first start: when
    no configuration has seeded the store, apply ``config`` (:func:`apply_config`) and return
    True. A store that has been seeded is never written: a restart, redeploy or reload never
    reapplies the file, and nothing an admin changed is overwritten by it. Returns False then."""
    async with conn.transaction():
        await conn.advisory_xact_lock(7261748190)
        if await is_seeded(conn):
            return False
    # The boot registers its remote sources' tables after the seed and retries the relationships
    # naming them (api.app, after _load_grpc_remote_sources_from_db).
    await _apply(config, conn, engine, label="configuration seed", defer_unregistered=True)
    return True


async def store_raw(raw: dict, conn: "Connection") -> dict:  # REQ-1919
    """The file's settings (``raw``, as written) with every model section taken from the model
    store: the raw configuration every reader of a model section reads, never the file's."""
    from provisa.core.store_config import store_model, with_store_model

    return with_store_model(raw, await store_model(conn))


def parse_store_raw(raw: dict) -> ProvisaConfig:  # REQ-1919
    """The configuration the process runs, parsed from :func:`store_raw`'s dict."""
    config = parse_config_dict(raw, stored=True)
    _expand_view_metrics(config)
    return config


async def store_config(raw: dict, conn: "Connection") -> ProvisaConfig:  # REQ-1919
    """The configuration the process runs: the file's settings (``raw``, as written) with every
    model section taken from the model store. After the seed the store alone owns the model, so
    nothing here is read from the file's model sections."""
    return parse_store_raw(await store_raw(raw, conn))


async def attach_store_sources(  # REQ-1919, REQ-1266, REQ-1448
    config: ProvisaConfig, engine: Any, catalog_names: dict[str, str] | None = None
) -> list[str]:
    """(Re)issue the engine catalog of every source the store holds and prime the engine's
    statistics. ``config`` is :func:`store_config`'s. Runs at every launch, wake and org build:
    an engine's catalogs do not outlive it. Returns the ids whose catalog failed."""
    failed = register_sources(engine, list(config.sources), catalog_names)
    await _analyze_sources(engine, config, catalog_names)
    return failed
