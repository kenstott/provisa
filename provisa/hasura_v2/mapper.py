# Copyright (c) 2026 Kenneth Stott
# Canary: 356f1b55-f2d0-472d-a664-0a9062edff7d
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map Hasura v2 intermediate models to Provisa config objects."""

from __future__ import annotations

import re
from typing import Any

from provisa.core import domain_policy
from provisa.security.rights import ORG_ADMIN_ROLE, Capability
from provisa.core.models import (
    AuthConfig,
    Cardinality,
    Column,
    ColumnPreset,
    Function,
    FunctionArgument,
    InlineType,
    NamingConfig,
    ProvisaConfig,
    Relationship,
    RLSRule,
    Role,
    RoleRateLimit,
    ScheduledTrigger,
    Source,
    SourceType,
    Table,
    Webhook,
    WriteGrant,
)
from provisa.hasura_v2.models import (
    HasuraAction,
    HasuraEventTrigger,
    HasuraPermission,
    HasuraMetadata,
    HasuraRemoteSchema,
    HasuraSource,
    HasuraTable,
)
from provisa.import_shared.filters import (
    Hop,
    Relations,
    hasura_session_name,
    bool_expr_to_sql,
)
from provisa.import_shared.warnings import WarningCollector

# Requirements: REQ-041, REQ-019, REQ-040, REQ-155, REQ-195, REQ-205, REQ-209, REQ-417


_HASURA_KIND_TO_SOURCE_TYPE: dict[str, str] = {
    "postgres": "postgresql",
    "pg": "postgresql",
    "mysql": "mysql",
    "mssql": "sqlserver",
    "bigquery": "bigquery",
    "citus": "postgresql",
}


def _imported_role(role_id: str) -> str:  # REQ-2080
    """The description an imported role starts with; the administrator replaces it."""
    return f"Imported from the Hasura metadata's role {role_id}."


def _source_type_from_kind(kind: str) -> SourceType:
    mapped = _HASURA_KIND_TO_SOURCE_TYPE.get(kind.lower(), "postgresql")
    return SourceType(mapped)


def _connection_fields(url: str) -> dict[str, Any]:
    """host, port, database, username and password a literal ``postgres://`` URL names; only
    what it names."""
    fields: dict[str, Any] = {}
    try:
        rest = url.split("://", 1)[1]
        if "@" in rest:
            creds, hostpart = rest.rsplit("@", 1)
            user, _, password = creds.partition(":")
            fields["username"] = user
            if password:
                fields["password"] = password
        else:
            hostpart = rest
        hp, _, database = hostpart.partition("/")
        if database:
            fields["database"] = database.split("?", 1)[0]
        host, _, port = hp.partition(":")
        fields["host"] = host
        if port:
            fields["port"] = int(port)
    except (ValueError, IndexError) as exc:
        raise ValueError(f"malformed database_url: {url!r}") from exc
    return fields


def _map_source(
    hs: HasuraSource, collector: WarningCollector, *, overridden: bool
) -> Source:  # REQ-417
    """The source as its connection_info states it. A URL Hasura reads from an environment
    variable is not in the export: nothing is made up for it, and unless the import's source
    settings supply it (``overridden``) the report says to."""
    conn = hs.connection_info or {}
    db_url = conn.get("database_url", "")
    fields: dict[str, Any] = {}
    if isinstance(db_url, str) and "://" in db_url:
        fields = _connection_fields(db_url)
    elif not overridden:
        var = db_url.get("from_env") if isinstance(db_url, dict) else None
        where = f"the environment variable {var}" if var else "a setting the export does not carry"
        collector.warn(
            "sources",
            f"Source {hs.name!r} reads its connection from {where}. Enter its host, port, "
            "database, user and password in the import's source settings and convert again.",
        )
    pool = conn.get("pool_settings") or {}
    if pool.get("min_connections"):  # REQ-623: both pool settings are preserved
        fields["pool_min"] = int(pool["min_connections"])
    if pool.get("max_connections"):
        fields["pool_max"] = int(pool["max_connections"])
    source = Source(id=hs.name, type=_source_type_from_kind(hs.kind), **fields)
    collector.carry("sources", hs.name, f"{source.type.value} source {hs.name}")
    return source


def _map_remote_schema(rs: HasuraRemoteSchema) -> Source:  # REQ-417
    """Map a Hasura Remote Schema to a Provisa graphql_remote source (REQ-417).

    Preserves the remote name, URL (or ${env:...} for url_from_env), headers, and
    forward-client-headers/timeout options in the source mapping.
    """
    from provisa.core.models import SourceType

    d = rs.definition or {}
    url = d.get("url") or ""
    if not url and d.get("url_from_env"):
        url = "${env:" + str(d["url_from_env"]) + "}"
    headers = {
        h["name"]: h.get("value", "")
        for h in d.get("headers", [])
        if isinstance(h, dict) and "name" in h
    }
    mapping: dict = {}
    if headers:
        mapping["headers"] = headers
    if d.get("forward_client_headers"):
        mapping["forward_client_headers"] = True
    if d.get("timeout_seconds") is not None:
        mapping["timeout_seconds"] = d["timeout_seconds"]
    # The runtime reads a graphql_remote source's endpoint off ``path`` (app_loaders
    # _load_graphql_remote_sources_from_db), the same field the demo config declares (REQ-1681).
    return Source(id=rs.name, type=SourceType.graphql_remote, path=url, mapping=mapping)


# REQ-1684: a Hasura role with any select permission may query, which in Provisa is the
# query_development right; one with an insert/update/delete permission may also write. Hasura's
# implicit admin is Provisa's org_admin, which is why every imported column names it.
_READ_CAPS = [Capability.QUERY_DEVELOPMENT.value]


def _collect_roles(metadata: HasuraMetadata) -> dict[str, Role]:  # REQ-041, REQ-040, REQ-1684
    """Collect all roles mentioned across permissions."""
    roles: dict[str, Role] = {}

    for source in metadata.sources:
        for table in source.tables:
            for perms in (
                table.select_permissions,
                table.insert_permissions,
                table.update_permissions,
                table.delete_permissions,
            ):
                for p in perms:
                    if p.role not in roles:
                        roles[p.role] = Role(
                            id=p.role,
                            description=_imported_role(p.role),
                            capabilities=list(_READ_CAPS),
                            domain_access=["*"],
                        )

    for action in metadata.actions:
        for perm in action.permissions:
            role_id = perm.get("role", "")
            if role_id and role_id not in roles:
                roles[role_id] = Role(
                    id=role_id,
                    description=_imported_role(role_id),
                    capabilities=list(_READ_CAPS),
                    domain_access=["*"],
                )

    # Inherited roles
    for ir in metadata.inherited_roles:
        if ir.role_name not in roles:
            roles[ir.role_name] = Role(
                id=ir.role_name,
                description=_imported_role(ir.role_name),
                capabilities=list(_READ_CAPS),
                domain_access=["*"],
            )
        # One role inherited is a Provisa parent exactly. Several are written out as the union
        # of their grants once the tables are mapped (_inherit_several): a Provisa role has one
        # parent.
        if len(ir.role_set) == 1:
            roles[ir.role_name].parent_role_id = ir.role_set[0]
        if ir.role_set:
            # Ensure child roles exist
            for child_role in ir.role_set:
                if child_role not in roles:
                    roles[child_role] = Role(
                        id=child_role,
                        description=_imported_role(child_role),
                        capabilities=list(_READ_CAPS),
                        domain_access=["*"],
                    )

    # Upgrade capabilities based on permission types
    for source in metadata.sources:
        for table in source.tables:
            for p in table.insert_permissions:
                if p.role in roles and "write" not in roles[p.role].capabilities:
                    roles[p.role].capabilities.append("write")
            for p in table.update_permissions:
                if p.role in roles and "write" not in roles[p.role].capabilities:
                    roles[p.role].capabilities.append("write")
            for p in table.delete_permissions:
                if p.role in roles and "write" not in roles[p.role].capabilities:
                    roles[p.role].capabilities.append("write")

    # REQ-1174: apply api_limits.yaml (rate + query-complexity) to each collected role.
    for role_id, role in roles.items():
        limits = _role_api_limits(metadata.api_limits, role_id)
        if limits is not None:
            role.rate_limit = limits

    return roles


def _api_limit_dim(api_limits: dict, name: str, role_id: str) -> object | None:
    """A Hasura api_limits dimension is ``{global: X, per_role: {role: Y}}``; the per-role value wins,
    else the global. Missing → None."""
    dim = api_limits.get(name) or {}
    if not isinstance(dim, dict):
        return None
    per_role = dim.get("per_role") or {}
    if role_id in per_role and per_role[role_id] is not None:
        return per_role[role_id]
    return dim.get("global")


def _role_api_limits(api_limits: dict, role_id: str) -> RoleRateLimit | None:
    """Map Hasura ``api_limits`` for ``role_id`` to a :class:`RoleRateLimit` (REQ-1174). Returns None
    when the role has no limits. Hasura's rate is per-MINUTE; Provisa's requests_per_second is
    per-SECOND, so convert (floor at 1). ``time_limit`` (seconds) → ms. ``node_limit`` caps the
    fields one query selects and becomes ``max_query_complexity``, which counts the same things on
    the statement the query compiles to. ``depth_limit`` has no counterpart: a deeper query is a
    statement with more relations and joins, which the complexity limit already prices."""
    if not api_limits:
        return None
    rate = _api_limit_dim(api_limits, "rate_limit", role_id)
    rps = None
    if isinstance(rate, dict) and isinstance(rate.get("max_reqs_per_min"), int):
        rps = max(1, round(rate["max_reqs_per_min"] / 60))
    nodes = _api_limit_dim(api_limits, "node_limit", role_id)
    time_s = _api_limit_dim(api_limits, "time_limit", role_id)
    nodes = nodes if isinstance(nodes, int) else None
    time_ms = time_s * 1000 if isinstance(time_s, int) else None
    if rps is None and nodes is None and time_ms is None:
        return None
    return RoleRateLimit(
        requests_per_second=rps,
        max_query_complexity=nodes,
        max_query_time_ms=time_ms,
    )


def _virtual_name(ht: HasuraTable) -> str:
    """The name the config loader resolves a table reference by (REQ-1680): the exposed alias when
    the export sets one (custom_root_fields.select / select_by_pk / custom_name), else the table
    name — exactly what ``Table.alias``/``table_name`` carry, so an RLS rule, relationship or event
    trigger emitted here binds at load."""
    return (
        ht.custom_root_fields.get("select")
        or ht.custom_root_fields.get("select_by_pk")
        or ht.custom_name
        or ht.name
    )


def _virtual_names(hs: HasuraSource) -> dict[tuple[str, str], str]:
    return {(ht.schema_name, ht.name): _virtual_name(ht) for ht in hs.tables}


def _table_id(virtual: dict[tuple[str, str], str] | None, schema: str, table_name: str) -> str:
    """A table reference for the config: the tracked table's virtual name; for a table the export
    does not track, its bare name, which the loader refuses loudly if it is not registered."""
    return (virtual or {}).get((schema, table_name), table_name)


def _fk_targets(hs: HasuraSource) -> dict[tuple[str, str, str], tuple[str, str]]:
    """REQ-1680: (schema, table, fk_column) → (target_schema, target_table), read off every array
    relationship in the source. An array relationship names the table that holds the FK column
    and the column, which is exactly what an object relationship declared by column omits."""
    out: dict[tuple[str, str, str], tuple[str, str]] = {}
    for ht in hs.tables:
        for rel in ht.array_relationships:
            if not rel.remote_table or not rel.column_mapping:
                continue
            fk_col = next(iter(rel.column_mapping.values()))
            out[(rel.remote_schema, rel.remote_table, fk_col)] = (ht.schema_name, ht.name)
    return out


def _predicate(
    expr: dict[str, Any] | None, table: str = "", relations: Relations | None = None
) -> str:
    """A Hasura boolean expression as the SQL predicate a write grant states; an empty one (no
    restriction) is every row."""
    return bool_expr_to_sql(expr or {}, table=table, relations=relations)


def _presets(set_: dict[str, Any]) -> list[ColumnPreset]:
    """Hasura's ``set``: a column set to a session variable (``X-Hasura-User-Id``) or a literal."""
    presets: list[ColumnPreset] = []
    for column, value in set_.items():
        text = str(value)
        if text.lower().startswith("x-hasura-"):
            presets.append(
                ColumnPreset(column=column, source="session", name=hasura_session_name(text))
            )
        else:
            presets.append(ColumnPreset(column=column, source="literal", value=text))
    return presets


def _write_grants(
    ht: HasuraTable, table_columns: list[str], tid: str = "", relations: Relations | None = None
) -> list[WriteGrant]:  # REQ-2074
    """Each insert, update and delete permission as the role's write grant for that operation --
    its columns, filter (rows), check and set (presets) -- and Hasura's admin, who holds every
    permission, as org_admin's grant for every operation over every column."""
    every = [c for c in table_columns if c != "*"]

    def _cols(perm: HasuraPermission) -> list[str]:
        cols = perm.columns
        listed = every if cols == "*" or (isinstance(cols, list) and "*" in cols) else cols
        return [c for c in listed if c not in perm.set]

    grants: list[WriteGrant] = []
    for perm in ht.insert_permissions:
        grants.append(
            WriteGrant(
                role=perm.role,
                operation="insert",
                columns=_cols(perm),
                check=_predicate(perm.check, tid, relations),
                presets=_presets(perm.set),
            )
        )
    for perm in ht.update_permissions:
        grants.append(
            WriteGrant(
                role=perm.role,
                operation="update",
                columns=_cols(perm),
                rows=_predicate(perm.filter, tid, relations),
                check=_predicate(perm.check, tid, relations),
                presets=_presets(perm.set),
            )
        )
    for perm in ht.delete_permissions:
        grants.append(
            WriteGrant(
                role=perm.role, operation="delete", rows=_predicate(perm.filter, tid, relations)
            )
        )
    grants += [
        WriteGrant(role=ORG_ADMIN_ROLE, operation="insert", columns=every, check="TRUE"),
        WriteGrant(
            role=ORG_ADMIN_ROLE, operation="update", columns=every, rows="TRUE", check="TRUE"
        ),
        WriteGrant(role=ORG_ADMIN_ROLE, operation="delete", rows="TRUE"),
    ]
    return grants


def _relationships(
    hs: HasuraSource,
    fk_targets: dict[tuple[str, str, str], tuple[str, str]],
    virtual: dict[tuple[str, str], str],
    collector: WarningCollector,
) -> tuple[list[Relationship], dict[tuple[str, str], Hop]]:  # REQ-019, REQ-1680
    """Every object and array relationship as an approved Provisa relationship, its GraphQL field
    the Hasura relationship's name; and the hop each makes, by (table, name), for the row rules
    that filter through it."""
    relationships: list[Relationship] = []
    hops: dict[tuple[str, str], Hop] = {}
    for ht in hs.tables:
        tid = _table_id(virtual, ht.schema_name, ht.name)
        for rel, cardinality in (
            *((r, Cardinality.many_to_one) for r in ht.object_relationships),
            *((r, Cardinality.one_to_many) for r in ht.array_relationships),
        ):
            where = f"{tid}.{rel.name}"
            if not rel.column_mapping:
                collector.warn(
                    "relationships",
                    f"Relationship {where} names no columns; add it after import on the "
                    "Relationships page.",
                )
                continue
            remote_schema, remote_table = rel.remote_schema, rel.remote_table
            if not remote_table:
                # REQ-1680: declared by FK column only; Hasura infers the target from the
                # constraint, which the export names through the inverse array relationship.
                src_col = next(iter(rel.column_mapping))
                found = fk_targets.get((ht.schema_name, ht.name, src_col))
                if found is None:
                    collector.warn(
                        "relationships",
                        f"Relationship {where} is declared by the foreign key column "
                        f"{src_col!r} and the export names no table it points to; add it after "
                        "import on the Relationships page.",
                    )
                    continue
                remote_schema, remote_table = found
            target = _table_id(virtual, remote_schema, remote_table)
            pairs = list(rel.column_mapping.items())
            hops[(tid, rel.name)] = Hop(target, pairs)
            if len(pairs) > 1:
                collector.warn(
                    "relationships",
                    f"Relationship {where} joins on {len(pairs)} column pairs; a Provisa "
                    "relationship joins on one. Row rules through it are carried; add the "
                    "relationship as a view joining both columns, or on the Relationships page "
                    "on one of them.",
                )
                continue
            ((src_col, tgt_col),) = pairs
            relationships.append(
                Relationship(
                    id=where,
                    source_table_id=tid,
                    target_table_id=target,
                    source_column=src_col,
                    target_column=tgt_col,
                    cardinality=cardinality,
                    graphql_alias=rel.name,
                )
            )
            collector.carry(
                "relationships",
                where,
                f"approved relationship {tid}.{src_col} -> {target}.{tgt_col}"
                + (" (declared by hand in Hasura: no foreign key backs it)" if rel.manual else ""),
            )
    return relationships, hops


def _map_table(
    ht: HasuraTable,
    source_name: str,
    collector: WarningCollector,
    fk_targets: dict[tuple[str, str, str], tuple[str, str]] | None = None,
    virtual: dict[tuple[str, str], str] | None = None,
    relations: Relations | None = None,
) -> tuple[
    Table, list[RLSRule], list[Relationship], list[Function]
]:  # REQ-041, REQ-040, REQ-019, REQ-155, REQ-205
    """Map a Hasura table to Provisa Table + side-effects."""
    tid = _table_id(virtual, ht.schema_name, ht.name)

    # Build columns from select permissions. REQ-1426: Hasura permission metadata names columns but
    # carries no types, and this converter runs offline against files — it cannot reach the database.
    # The emitted YAML therefore leaves data_type null, which makes it an INCOMPLETE design: the
    # user must assign every missing type before it will load. Nothing infers a type at load or run
    # time, so an unfinished design fails the load rather than persisting an untyped column.
    all_columns: dict[str, Column] = {}
    for perm in ht.select_permissions:
        cols = perm.columns
        if cols == "*" or (isinstance(cols, list) and "*" in cols):
            # Wildcard — we'll use a placeholder
            if "*" not in all_columns:
                all_columns["*"] = Column(name="*", visible_to=[])
            all_columns["*"].visible_to.append(perm.role)
        elif isinstance(cols, list):
            for col_name in cols:
                if col_name not in all_columns:
                    all_columns[col_name] = Column(name=col_name, visible_to=[])
                all_columns[col_name].visible_to.append(perm.role)

    # Columns an insert or update permission writes or presets that no select permission names:
    # the table has them, visible to the admin alone (REQ-2074: who writes them is the grants').
    for perm in [*ht.insert_permissions, *ht.update_permissions]:
        named = perm.columns if isinstance(perm.columns, list) else []
        for col_name in [*named, *perm.set]:
            if col_name != "*" and col_name not in all_columns:
                all_columns[col_name] = Column(name=col_name, visible_to=[])

    # Apply custom column names as aliases
    for orig_name, alias in ht.custom_column_names.items():
        if orig_name in all_columns:
            all_columns[orig_name].alias = alias

    # REQ-1684: Hasura's admin holds every permission implicitly; Provisa's org_admin is that
    # role, and a column's grant list is literal, so it is named on every imported column.
    for col in all_columns.values():
        col.visible_to = sorted(set(col.visible_to) | {ORG_ADMIN_ROLE})

    columns = list(all_columns.values())

    # Table alias from custom_root_fields
    table_alias = (
        ht.custom_root_fields.get("select")
        or ht.custom_root_fields.get("select_by_pk")
        or ht.custom_name
    )

    table = Table(
        source_id=source_name,
        domain_id=domain_policy.import_default(),
        schema_name=ht.schema_name,
        table_name=ht.name,
        columns=columns,
        alias=table_alias,
        write_grants=_write_grants(ht, [c.name for c in columns], tid, relations),
    )

    # Root fields Hasura names beyond the table's own: Provisa serves a table as one field, its
    # by-key read, aggregates and writes as arguments and mutations of it.
    others = {k: v for k, v in ht.custom_root_fields.items() if v and k != "select"}
    if others:
        collector.map(
            "names",
            ", ".join(sorted(others.values())),
            f"root fields of {tid}, served through it: a by-key read is a filter on it, an aggregate its "
            "_aggregate field, a write its mutation -- the app's queries take that shape",
        )

    # RLS rules from select permission filters
    rls_rules: list[RLSRule] = []
    for perm in ht.select_permissions:
        if perm.filter:
            sql_filter = bool_expr_to_sql(perm.filter, table=tid, relations=relations)
            if sql_filter != "TRUE":
                rls_rules.append(
                    RLSRule(
                        table_id=tid,
                        role_id=perm.role,
                        filter=sql_filter,
                        description=f"The rows of {tid} the Hasura select permission of "
                        f"{perm.role} allowed.",
                    )
                )
    for perm in ht.select_permissions:
        collector.carry(
            "permissions",
            f"{perm.role} select on {tid}",
            "column grants" + (" and a row filter" if perm.filter else ""),
        )
    for kind, perms in (
        ("insert", ht.insert_permissions),
        ("update", ht.update_permissions),
        ("delete", ht.delete_permissions),
    ):
        for perm in perms:
            collector.carry("permissions", f"{perm.role} {kind} on {tid}", f"{kind} write grant")

    # Computed fields: a database function over the row, whose body the export does not carry.
    for cf in ht.computed_fields:
        collector.warn(
            "computed_fields",
            f"Computed field {cf.name!r} on {tid} calls the database function "
            f"{cf.function_schema}.{cf.function_name}, which the export names but does not "
            f"define. Add it as a column of a view over {tid} (SELECT t.*, "
            f"{cf.function_schema}.{cf.function_name}(t) AS {cf.name} FROM {tid} t), or compute "
            "it in the app.",
        )
    relationships: list[Relationship] = []
    functions: list[Function] = []
    return table, rls_rules, relationships, functions


_ENV_TEMPLATE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def _env_template(url: str) -> str:
    """A Hasura URL template, ``{{VAR}}/path``, as the environment reference Provisa resolves
    where the URL is used: ``${env:VAR}/path``."""
    return _ENV_TEMPLATE.sub(lambda m: "${env:" + m.group(1) + "}", url)


def _retries(what: str, retry_conf: dict[str, Any], collector: WarningCollector) -> None:
    if retry_conf.get("num_retries"):
        collector.warn(
            "triggers",
            f"{what} retries a failed call {retry_conf['num_retries']} time(s); a Provisa "
            "trigger calls once. Retry in the handler if a missed call matters.",
        )


def _change_trigger(
    et: HasuraEventTrigger, source_id: str, collector: WarningCollector
) -> ScheduledTrigger:  # REQ-2054
    """A Hasura event trigger as a trigger on its table's change, calling the same webhook."""
    url = "${env:" + et.webhook + "}" if et.webhook_from_env else _env_template(et.webhook)
    table = f"{source_id}.{et.table_schema}.{et.table_name}"
    collector.map(
        "triggers",
        et.name,
        f"a trigger on changes to {table} calling the same webhook: Provisa fires it whether "
        "the change came through Provisa or straight to the database",
    )
    if sorted(et.operations) != ["delete", "insert", "update"]:
        collector.warn(
            "triggers",
            f"Event trigger {et.name!r} fires on {', '.join(et.operations)} alone; its Provisa "
            f"trigger fires on every change to {table}, once per batch of changes, posting the "
            "table and the changed rows rather than one event per row with its old and new "
            "values. Make the handler act on the rows it is meant for.",
        )
    _retries(f"Event trigger {et.name!r}", et.retry_conf, collector)
    return ScheduledTrigger(id=et.name, on_table=table, on_event="change", url=url)


def _ir_type(gql: str, input_types: set[str]) -> str:
    """An action argument's GraphQL type as the IR type a command argument declares; an input
    object (``input_types``) is the JSON object the handler receives."""
    from provisa.core.ir_types import to_ir

    named = gql.strip().rstrip("!").strip("[]!")
    if named in input_types:
        return "json"
    # GraphQL serializes an ID as a string.
    return to_ir("String" if named == "ID" else named, "graphql")


def _map_action(
    action: HasuraAction,
    custom_types: dict[str, list[tuple[str, str]]],
    input_types: set[str],
    collector: WarningCollector,
) -> Webhook:  # REQ-205, REQ-209
    """A Hasura action as a command calling the same handler, with its arguments and output."""
    # Hasura answers an action with no permissions to the admin alone; so does the command it
    # becomes (a command's one role list, empty, would assign it to every role).
    visible_to = [p.get("role", "") for p in action.permissions if p.get("role")] or [
        ORG_ADMIN_ROLE
    ]
    defn = action.definition
    kind = defn.action_type if defn.action_type in ("mutation", "query") else "mutation"
    output = defn.output_type.strip().rstrip("!").strip("[]!")
    fields = custom_types.get(output) or []
    if not fields:
        collector.warn(
            "actions",
            f"Action {action.name!r} returns {output or 'nothing named'}, whose fields the export "
            "does not carry; add them to the command's return type.",
        )
    if defn.kind == "asynchronous":
        collector.warn(
            "actions",
            f"Action {action.name!r} is asynchronous: Hasura answers with an id and the result "
            "later; its command waits for the handler's answer. Have the handler answer at once.",
        )
    collector.map(
        "actions",
        action.name,
        f"a {kind} command calling {_env_template(defn.handler)}, governed by role like any "
        "other command",
    )
    return Webhook(
        name=action.name,
        url=_env_template(defn.handler),
        method="POST",
        arguments=[
            FunctionArgument(
                name=a.get("name", ""),
                type=_ir_type(str(a.get("type", "String")), input_types),
            )
            for a in defn.arguments
        ],
        inline_return_type=[
            InlineType(name=name, type=gql.strip().rstrip("!")) for name, gql in fields
        ],
        visible_to=visible_to,
        domain_id=domain_policy.import_default(),
        kind=kind,
    )


def _inherit_several(
    role_id: str,
    members: list[str],
    tables: list[Table],
    rls: list[RLSRule],
    roles: dict[str, Role],
    collector: WarningCollector,
) -> None:  # REQ-041, REQ-2074
    """A Hasura role inheriting several roles, written out as the union of their rights: each
    column any member reads; each table's rows any member reads (no rule where one reads every
    row); each write operation any member holds, over the union of their columns, the OR of
    their rows and checks, and the presets they all set alike."""
    rules = {(r.table_id, r.role_id): r.filter for r in rls}
    for table in tables:
        tid = table.alias or table.table_name
        readers = [m for m in members if any(m in c.visible_to for c in table.columns)]
        for column in table.columns:
            if any(m in column.visible_to for m in members) and role_id not in column.visible_to:
                column.visible_to = sorted({*column.visible_to, role_id})
        filters = [rules.get((tid, m)) for m in readers]
        if readers and all(filters):
            rls.append(
                RLSRule(
                    table_id=tid,
                    role_id=role_id,
                    filter=_or(filters),
                    description=f"The rows of {tid} any role {role_id} inherits from may read.",
                )
            )
        held = [g for g in table.write_grants if g.role in members]
        for operation in ("insert", "update", "delete"):
            holders = [g for g in held if g.operation == operation]
            if not holders:
                continue
            columns: list[str] = []
            for g in holders:
                columns += [c for c in g.columns if c not in columns]
            first = holders[0].presets
            table.write_grants.append(
                WriteGrant(
                    role=role_id,
                    operation=operation,
                    columns=columns,
                    rows=None if operation == "insert" else _or([g.rows for g in holders]),
                    check=None if operation == "delete" else _or([g.check for g in holders]),
                    presets=[p for p in first if all(p in g.presets for g in holders)],
                )
            )
    roles[role_id].capabilities = sorted(
        {cap for m in members for cap in roles[m].capabilities} | set(roles[role_id].capabilities)
    )
    collector.map(
        "roles",
        role_id,
        f"a role holding the union of the grants of {' and '.join(members)}: a Provisa role "
        "inherits from one parent, so what it inherits from several is written out on it",
    )


def _settled_relationships(
    rels: list[Relationship], tables: list[Table], collector: WarningCollector
) -> list[Relationship]:  # REQ-019
    """One relationship per join: a second declaring the same join merges into the first. A
    relationship's field keeps its Hasura name unless the table already has a field of that
    name; then it takes the name Provisa gives it, and the report says so."""
    fields = {(t.alias or t.table_name): {c.alias or c.name for c in t.columns} for t in tables}
    kept: dict[tuple, Relationship] = {}
    for rel in rels:
        key = (
            rel.source_table_id,
            rel.target_table_id,
            rel.source_column,
            rel.target_column,
            rel.cardinality,
        )
        if key in kept:
            collector.carry(
                "relationships",
                rel.id,
                f"merged into {kept[key].id}, which declares the same join",
            )
            continue
        # _relationships names every relationship's field after its Hasura relationship.
        assert rel.graphql_alias is not None
        taken = fields.setdefault(rel.source_table_id, set())
        if rel.graphql_alias in taken:
            collector.warn(
                "relationships",
                f"Relationship {rel.id}: {rel.source_table_id} already has a field named "
                f"{rel.graphql_alias!r}, so the relationship takes the name Provisa gives it. "
                "Rename it on the Relationships page if the app needs another name.",
            )
            rel.graphql_alias = None
        else:
            taken.add(rel.graphql_alias)
        kept[key] = rel
    return list(kept.values())


def _session_variables(node: Any, found: set[str]) -> None:
    """Every X-Hasura-* name a permission's filter, check or set mentions."""
    if isinstance(node, str):
        if node.lower().startswith("x-hasura-"):
            found.add(node)
    elif isinstance(node, dict):
        for key, val in node.items():
            _session_variables(key, found)
            _session_variables(val, found)
    elif isinstance(node, list):
        for val in node:
            _session_variables(val, found)


def _report_session_variables(
    metadata: HasuraMetadata, collector: WarningCollector
) -> None:  # REQ-1682
    """Each session variable the permissions read, and what supplies it now: the token claim
    Hasura read it from, as an attribute of its own (provisa.user_id stays the identity)."""
    found: set[str] = set()
    for source in metadata.sources:
        for ht in source.tables:
            for perm in [
                *ht.select_permissions,
                *ht.insert_permissions,
                *ht.update_permissions,
                *ht.delete_permissions,
            ]:
                _session_variables([perm.filter, perm.check, perm.set], found)
    for name in sorted({hasura_session_name(var) for var in found}):
        words = name[len("hasura_") :].split("_")
        var = "X-Hasura-" + "-".join(w.capitalize() for w in words)
        claim = var.lower()
        collector.map(
            "session_variables",
            var,
            f"provisa.{name}, supplied by the token's {claim} claim (at the top level or in the "
            f"Hasura claims object, as Hasura read it), or recorded for the member as the "
            f"attribute {name}: provisa.user_id stays the signed-in identity",
        )


_ENV_REF = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}")


def _report_environment(
    webhooks: list[Webhook], triggers: list[ScheduledTrigger], collector: WarningCollector
) -> None:  # REQ-417
    """Each environment variable a converted handler URL reads, named once with what reads it:
    Hasura read it from its own environment, and a configuration naming a variable the Provisa
    server does not have does not load."""
    readers: dict[str, list[str]] = {}
    for name, url in [
        *((w.name, w.url) for w in webhooks),
        *((t.id, t.url or "") for t in triggers),
    ]:
        for var in _ENV_REF.findall(url):
            readers.setdefault(var, []).append(name)
    for var, names in sorted(readers.items()):
        collector.warn(
            "environment",
            f"Set {var} in the Provisa server's environment, as it was in Hasura's: "
            f"{', '.join(names)} read it. The configuration does not load until it is set.",
        )


def _or(terms: list[str | None]) -> str:
    """The OR of predicates; every row where any is every row."""
    if any(t in (None, "TRUE") for t in terms):
        return "TRUE"
    unique = list(dict.fromkeys(t for t in terms if t))
    return unique[0] if len(unique) == 1 else " OR ".join(f"({t})" for t in unique)


def convert_metadata(
    metadata: HasuraMetadata,
    collector: WarningCollector | None = None,
    domain_map: dict[str, str] | None = None,
    auth_env: dict[str, str] | None = None,
    source_overrides: dict[str, Any] | None = None,
) -> ProvisaConfig:  # REQ-417, REQ-041, REQ-019, REQ-040, REQ-155, REQ-195, REQ-205, REQ-209
    """Convert Hasura v2 metadata to a ProvisaConfig.

    Args:
        metadata: Parsed Hasura v2 metadata.
        collector: Warning collector for unsupported features.
        domain_map: Optional schema->domain mapping.
        auth_env: Optional auth environment variables.
        source_overrides: Optional per-source connection overrides.
    """
    if collector is None:
        collector = WarningCollector()

    domain_map = domain_map or {}
    source_overrides = source_overrides or {}

    # Sources
    sources: list[Source] = []
    for hs in metadata.sources:
        src = _map_source(hs, collector, overridden=hs.name in source_overrides)
        # Apply overrides
        if hs.name in source_overrides:
            overrides = source_overrides[hs.name]
            for k, v in overrides.items():
                if hasattr(src, k):
                    object.__setattr__(src, k, v)
        sources.append(src)

    # Remote schemas -> graphql_remote sources (REQ-417), landed as tables (REQ-1681)
    from provisa.hasura_v2.remote_schema import land_remote_schema

    remote_tables: list[Table] = []
    for rs in metadata.remote_schemas:
        sources.append(_map_remote_schema(rs))
        landed = land_remote_schema(
            rs, domain_map.get(rs.name) or domain_policy.import_default(), collector
        )
        remote_tables.extend(landed)
        collector.map(
            "remote_schemas",
            rs.name,
            f"a GraphQL remote source whose root fields are governed tables "
            f"({', '.join(t.table_name for t in landed)}): Provisa reads a remote schema as "
            "tables its roles, row rules and relationships govern",
        )

    # Roles
    roles_dict = _collect_roles(metadata)
    roles = sorted(roles_dict.values(), key=lambda r: r.id)

    # Tables, RLS, Relationships
    tables: list[Table] = list(remote_tables)
    all_rls: list[RLSRule] = []
    all_rels: list[Relationship] = []
    all_functions: list[Function] = []
    scheduled: list[ScheduledTrigger] = []

    for hs in metadata.sources:
        fk_targets = _fk_targets(hs)  # REQ-1680
        virtual = _virtual_names(hs)
        rels, hops = _relationships(hs, fk_targets, virtual, collector)
        all_rels.extend(rels)
        for ht in hs.tables:
            table, rls_rules, _rels, _fns = _map_table(
                ht,
                hs.name,
                collector,
                fk_targets,
                virtual,
                relations=lambda t, k, hops=hops: hops.get((t, k)),
            )
            # Apply domain mapping
            dm_key = f"{ht.schema_name}"
            if dm_key in domain_map:
                table.domain_id = domain_map[dm_key]
            tables.append(table)
            all_rls.extend(rls_rules)
            collector.carry(
                "tables",
                f"{ht.schema_name}.{ht.name}",
                f"registered table {table.alias or ht.name}",
            )

            # Event triggers -> change triggers on the table (REQ-2054)
            for et in ht.event_triggers:
                scheduled.append(_change_trigger(et, hs.name, collector))

        # Tracked functions: the export names each and the roles that call it; its arguments and
        # what it returns are the database's, read at preview (api/admin/import_typing.py).
        for hf in hs.functions:
            collector.pending_functions.append(
                {
                    "name": hf.name,
                    "source_id": hs.name,
                    "schema_name": hf.schema_name,
                    "kind": "query" if hf.exposed_as == "query" else "mutation",
                    "visible_to": list(hf.roles) or [ORG_ADMIN_ROLE],
                }
            )

    all_rels = _settled_relationships(all_rels, tables, collector)

    _report_session_variables(metadata, collector)

    # Hasura inherited roles of several roles: the union of their grants (_inherit_several).
    for ir in metadata.inherited_roles:
        if len(ir.role_set) > 1:
            _inherit_several(ir.role_name, ir.role_set, tables, all_rls, roles_dict, collector)

    # Actions -> commands calling their handler
    webhooks: list[Webhook] = []
    for action in metadata.actions:
        webhooks.append(_map_action(action, metadata.custom_types, metadata.input_types, collector))

    # Cron triggers -> scheduled triggers
    for ct in metadata.cron_triggers:
        scheduled.append(
            ScheduledTrigger(
                id=ct.name,
                cron=ct.schedule,
                url=_env_template(ct.webhook),
                args=dict(ct.payload),
                enabled=ct.enabled,
            )
        )
        collector.carry("triggers", ct.name, f"scheduled trigger on {ct.schedule}")
        _retries(f"Cron trigger {ct.name!r}", ct.retry_conf, collector)

    _report_environment(webhooks, scheduled, collector)

    # Auth config from env
    auth = AuthConfig()
    if auth_env:
        provider = auth_env.get("AUTH_PROVIDER", "none")
        if provider == "webhook":
            collector.warn(
                "webhook_auth",
                "AUTH_PROVIDER=webhook is not supported by Provisa; "
                "configure an explicit provider (firebase, keycloak, oauth, simple).",
            )
        auth.provider = provider
        if provider == "firebase":
            # Direct subscript hard-fails (KeyError) if the env var is absent.
            auth.firebase = {
                "project_id": auth_env["FIREBASE_PROJECT_ID"],
            }
        elif provider == "keycloak":
            auth.keycloak = {
                "url": auth_env["KEYCLOAK_URL"],
                "realm": auth_env["KEYCLOAK_REALM"],
            }
        elif provider == "oauth" or auth_env.get("JWK_URL"):
            auth.provider = "oauth"
            auth.oauth = {"jwk_url": auth_env["JWK_URL"]}
        # Admin secret -> superuser role
        if auth_env.get("HASURA_GRAPHQL_ADMIN_SECRET"):
            auth.superuser = {"secret": auth_env["HASURA_GRAPHQL_ADMIN_SECRET"]}
        # Claims map -> role_mapping entries
        claims_map_raw = auth_env.get("CLAIMS_MAP")
        if claims_map_raw:
            import json

            try:
                claims_map = json.loads(claims_map_raw)
                if isinstance(claims_map, dict):
                    auth.role_mapping = [{"claim": k, "role": v} for k, v in claims_map.items()]
            except json.JSONDecodeError:
                collector.warn(
                    "claims_map_parse_error",
                    f"CLAIMS_MAP is not valid JSON: {claims_map_raw!r}",
                )

    # Naming config — read enable_relay from graphql_engine config
    enable_relay = bool(metadata.graphql_engine.get("enable_relay", False))
    naming = NamingConfig(relay_pagination=enable_relay)

    # Domains — collect unique domain_ids
    domain_ids = {t.domain_id for t in tables} | {"default"}
    from provisa.core.models import Domain

    domains = [Domain(id=did) for did in sorted(domain_ids)]

    return ProvisaConfig(
        sources=sources,
        domains=domains,
        naming=naming,
        tables=tables,
        relationships=all_rels,
        roles=roles,
        rls_rules=all_rls,
        scheduled_triggers=scheduled,
        functions=all_functions,
        webhooks=webhooks,
        auth=auth,
    )
