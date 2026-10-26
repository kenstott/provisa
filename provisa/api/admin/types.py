# Copyright (c) 2026 Kenneth Stott
# Canary: f60382a1-5df1-4ae3-aca5-e4e0d6139efc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Strawberry types mirroring Pydantic config models."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import enum

import strawberry

if TYPE_CHECKING:
    # ``strawberry.scalars.JSON`` is ``NewType("JSON", object)``, so no dict literal is assignable
    # to it and every ``params={...}`` / ``variables={...}`` site in the mutation resolvers reads as
    # a type error -- 115 of them, all of the same shape. The GraphQL schema is built from the
    # RUNTIME annotation (strawberry resolves it with ``get_type_hints`` against this module), so
    # naming the mapping here gives the checker the shape the code actually passes while strawberry
    # still receives its scalar and the published schema keeps saying ``JSON``.
    JsonScalar = dict[str, Any]
else:
    JsonScalar = strawberry.scalars.JSON

# Requirements: REQ-012, REQ-013, REQ-019, REQ-040, REQ-041, REQ-042, REQ-135, REQ-247, REQ-252, REQ-262, REQ-402


@strawberry.type
class CalendarType:  # REQ-962: a named, versioned snapshot-boundary calendar
    name: str
    version: str
    base_system: str = "gregorian"  # gregorian | fiscal | retail_445
    tz: str = "UTC"
    fiscal_anchor_month: int = 1
    fiscal_anchor_day: int = 1
    retail_anchor: str | None = None  # ISO date; retail_445 reference year start
    week_start: int = 0  # 0 = Monday
    holidays: list[str] = strawberry.field(default_factory=list)  # ISO dates, immutable per version
    weekend: list[int] = strawberry.field(default_factory=lambda: [5, 6])  # weekday ints (Sat, Sun)
    day_close: str | None = None  # REQ-2111: local close of the day, "HH:MM"; None = midnight


@strawberry.input
class CalendarInput:  # REQ-962
    name: str
    version: str
    base_system: str = "gregorian"
    tz: str = "UTC"
    fiscal_anchor_month: int = 1
    fiscal_anchor_day: int = 1
    retail_anchor: str | None = None
    week_start: int = 0
    holidays: list[str] = strawberry.field(default_factory=list)
    weekend: list[int] = strawberry.field(default_factory=lambda: [5, 6])
    day_close: str | None = None  # REQ-2111


@strawberry.type
class SourceCdcConfigType:  # REQ-824
    bootstrap_servers: str
    topic_prefix: str
    schema_registry_url: str | None = None
    # REQ-1951: how the registry is reached. The password and the token are returned as the
    # vault references the row holds, never as credentials.
    schema_registry_auth: str = "none"
    schema_registry_username: str | None = None
    schema_registry_password: str | None = None
    schema_registry_token: str | None = None
    schema_registry_client_cert: str | None = None
    schema_registry_client_key: str | None = None
    schema_registry_ca: str | None = None
    consumer_group_id: str | None = (
        None  # REQ-931: None = inherit Provisa-level cdc_consumer_group_id
    )


@strawberry.type
class SourceType:  # REQ-012
    id: str
    type: str

    @strawberry.field
    def row_fetch_by_keys(self) -> bool:  # REQ-1865
        """Whether this source's type fetches rows by primary key, so a table of it may be
        replicated row by row (core.source_registry). For a type whose fetch is each table's
        own (a remote GraphQL source), whether a table may have one: its save says."""
        from provisa.core.source_registry import supports_row_fetch_by_keys

        return supports_row_fetch_by_keys(self.type)

    # The fields that locate or authenticate to the source are null for a caller without
    # source_registration (see _row_mappers._source_from_row).
    host: str | None
    port: int | None
    database: str | None
    username: str | None
    dialect: str
    cache_enabled: bool
    cache_ttl: int | None
    replicate: int | None  # REQ-826: NULL = Default; -1 never, N hot, 0 always
    region: str | None  # REQ-1921: the region its new tables start in; null = none
    load_protected: bool = False  # REQ-1141: scheduled-refresh-only load protection
    off_peak_window: str | None = None  # REQ-1141: "HH:MM-HH:MM" maintenance window
    off_peak_tz: str = "UTC"  # REQ-1141: IANA zone for the window
    max_live_concurrency: int | None = None  # REQ-1909: None = no cap on concurrent live reads
    sentinel_path: str | None = None  # REQ-1148: freshness marker URL
    download_from: str | None = None  # REQ-2113: where its file is downloaded from
    freshness_gate: bool = False  # REQ-860: gate reads on the source's freshness decision
    gql_naming_convention: str | None = None
    path: str | None = None
    allowed_domains: list[str] = strawberry.field(default_factory=list)
    description: str = ""
    mapping_json: str | None = "{}"
    # REQ-1736: read-side counterpart of SourceInput.federation_hints_json — without this, a
    # source's warehouse extras (Snowflake warehouse/role, Databricks http_path, ...) round-trip
    # into the store on create but can never be read back into the edit form, so a save-then-edit
    # silently drops them from the form (though not from the store, since update_source only
    # overwrites federation_hints when the input actually carries a federation_hints_json). Also
    # the channel rss's feed_url and websocket's subscribe_payload round-trip through (REQ-1739).
    federation_hints_json: str | None = "{}"
    # What the source's password field holds, as its operator entered it: the value, or a
    # reference they typed. Exposed so the form shows the field's own text again, and so a caller
    # re-registering an already-validated source on a second engine (the cross-engine swap
    # replay) can carry the same field forward.
    password_ref: str | None = ""
    change_signal: str = "ttl"  # REQ-929: source default change signal (inherited by its tables)
    cdc: SourceCdcConfigType | None = None  # REQ-824: source-level CDC transport


@strawberry.type
class GovDataSubjectType:  # REQ-540
    """A subject an AskAmerica source can be given, and the adapter schemas it brings."""

    value: str
    label: str
    schemas: list[str]


@strawberry.type
class GovDataSubjectsType:  # REQ-540, REQ-541
    """What the Sources form offers for an AskAmerica source: the subjects, and the linker
    schemas every such source serves whatever subjects it is given."""

    subjects: list[GovDataSubjectType]
    linker_schemas: list[str]


@strawberry.type
class DomainType:  # REQ-533, REQ-609
    id: str
    description: str
    steward: str | None = None  # REQ-609: None = pending, no designated steward
    graphql_alias: str | None = None
    # REQ-1363: the domain this one sits under; None = no parent. A role that lists the parent
    # reaches this domain too.
    parent_domain_id: str | None = None
    # REQ-1363: the steward the domain answers to -- its own, else its nearest ancestor's; None
    # when no domain up the tree has one.
    effective_steward: str | None = None
    # domain_policy.system_domain_ids() (meta/ops/"") -- auto-generated, preserved across
    # config replace-mode reloads, never a target an end user picks for their own tables. Was
    # entirely absent from this type, so every domain picker in the UI (KaggleFormSection's
    # included) had no way to exclude them -- confirmed live, both still listed on a real domain
    # dropdown.
    is_system: bool = False


@strawberry.type
class ListedNamesType:  # REQ-2153: a listed table's names, and nothing more
    domain_id: str
    table_name: str
    column_names: list[str]


@strawberry.type
class DataProductType:  # REQ-1634, REQ-1660
    id: str
    domain_id: str
    name: str
    owner_role: str | None = None
    team_role: str | None = None
    purpose: str = ""
    limitations: str = ""
    usage: str = ""
    version: str | None = None
    status: str | None = None
    sla: str | None = None
    support: str | None = None
    custom_properties: JsonScalar


@strawberry.type
class TagParamValueType:  # REQ-1467
    value: str
    description: str


@strawberry.type
class TagType:  # REQ-1373, REQ-1375
    id: str
    description: str
    applies_to: list[str]
    is_system: bool
    reason_policy: str = "optional"  # hidden | optional | required
    expires_policy: str = "optional"
    # REQ-1443: computed from the object's own registration, so it is read-only everywhere —
    # the picker must not offer it and assignTag refuses it.
    derived: bool = False
    # REQ-1467: "none" or "required". A required parameter is assigned as "{id}:{value}" and
    # the value must be one of param_values — the list is closed, because an open one would
    # accept "entity:custmoer" and the misspelt type then reads downstream as absence.
    param_policy: str = "none"
    sensitive: bool = False  # REQ-1943: the Sensitive data option
    param_values: list[TagParamValueType] = strawberry.field(default_factory=list)


@strawberry.type
class TagAssignmentType:  # REQ-1377
    tag_id: str
    object_type: str  # source | table | column | relationship
    source_id: str | None = None
    table_id: int | None = None
    column_name: str | None = None
    relationship_id: str | None = None
    command_name: str | None = None  # tracked function/webhook name
    table_ref: str | None = None  # qualified "source.schema.table" for table/column targets
    reason: str | None = None  # required for 'deprecated'
    expires_on: str | None = None  # ISO date; planned removal for 'deprecated'
    # REQ-2094: purpose, bases and review-by date, and who decided it, when and on what evidence.
    why: JsonScalar | None = None


@strawberry.type
class ColumnDependentType:  # REQ-1484
    """One artifact referencing a column the administrator is about to rename or drop."""

    kind: (
        str  # view | mv | metric | metric_view | rls | dq_contract | relationship | glossary | ...
    )
    name: str  # the artifact's addressable name
    detail: str  # how it references the column
    breaks_on: str  # "rename" (authored against the exposed name) | "remove" (physical ref)


@strawberry.type
class ColumnDependentsType:  # REQ-1484
    column_name: str
    dependents: list[ColumnDependentType]


@strawberry.type
class WritePresetType:  # REQ-2074
    column: str
    source: str  # session | literal | now
    name: str | None = None
    value: str | None = None


@strawberry.type
class WriteGrantType:  # REQ-2074
    """What one role may write to the table by one operation. ``rows`` / ``check`` null: the
    role's read row filter on the table."""

    role: str
    operation: str
    columns: list[str]
    rows: str | None
    check: str | None
    presets: list[WritePresetType]
    why: JsonScalar | None = None  # REQ-2094: why the grant is what it is, who decided it and when


@strawberry.type
class UniqueConstraintType:  # REQ-1093
    name: str
    columns: list[str]


@strawberry.type
class LiveOutputConfigType:  # REQ-565
    type: str  # "sse" | "kafka"
    topic: str | None = None
    key_column: str | None = None
    bootstrap_servers: str | None = None
    role: str | None = None  # REQ-286: the role a Kafka output publishes as


@strawberry.type
class LiveKafkaParamsType:  # REQ-813
    topic: str
    format: str = "json"
    key_column: str | None = None
    landing_time_column: str | None = None  # REQ-813 (amended 2026-10-21)


@strawberry.type
class LiveDeliveryConfigType:  # REQ-565, REQ-813
    strategy: str = "poll"  # poll | native | debezium | kafka
    watermark_column: str | None = None
    poll_interval: int = 10
    kafka: LiveKafkaParamsType | None = None
    query_id: str | None = None
    outputs: list[LiveOutputConfigType] = strawberry.field(default_factory=list)


@strawberry.type
class DeltaConfigType:  # REQ-874
    """A table's incremental-reload (delta) declaration, as the table form reads it back. The
    cursor field is the table's ``watermark_column``."""

    query: str | None = None  # source-native delta query; None = generated for a SQL source
    apply: str = "upsert"  # upsert | append
    deletes: str = "none"  # none | tombstone
    tombstone_column: str | None = None
    rebuild_every: int | None = None  # seconds between whole rebuilds; None = never on a clock


@strawberry.type
class RefreshPolicySummaryType:  # REQ-1143
    """Server-derived plain-English summary of a table's effective refresh/serving policy, computed
    per (source, engine) from the same resolution the planner uses. ``serving`` ∈
    live|scheduled|cache|frozen; ``warning`` is a non-null misconfiguration note."""

    text: str
    serving: str
    warning: str | None = None


@strawberry.type
class ImplicitMeasureType:  # REQ-1360: metadata-only Kimball measure annotation
    column: str
    agg_funcs: list[str]


@strawberry.type
class RoleTtlType:  # REQ-1907
    role: str
    ttl: int


@strawberry.type
class PagingType:  # REQ-318
    """A table's paging as declared (provisa.core.paging): a paged REST endpoint's type and
    parameters, or a connection table's row bound. Unset fields are null."""

    type: str | None = None
    cursor_field: str | None = None
    cursor_param: str | None = None
    page_param: str | None = None
    page_size_param: str | None = None
    page_size: int | None = None
    max_pages: int | None = None
    max_rows: int | None = None
    rows_field: str | None = None  # REQ-316: where a REST answer's rows are


def _dq_dataset_of(table_id: int) -> str | None:
    """Table ``table_id`` as a data-quality contract dataset, by its compiled TableMeta; None when
    no role's compiled context exposes it."""
    from provisa.api.app import state
    from provisa.dq.contract import meta_dataset

    for ctx in state.contexts.values():
        for meta in ctx.tables.values():
            if meta.table_id == table_id:
                return meta_dataset(meta)
    return None


@strawberry.type
class QualityCheckerType:  # REQ-1443
    """A data-quality checker table whose contract scans a table, and its last scan."""

    table_id: int
    table_name: str
    source_id: str
    # pass | fail | warn | error | skipped: the last scan's worst check; null before a first scan.
    outcome: str | None
    # When the last scan ran (ISO 8601, UTC); null before a first scan.
    scan_time: str | None


@strawberry.type
class RegisteredTableType:  # REQ-013, REQ-014, REQ-016, REQ-135
    id: int
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    # The data writes the table's source can take — insert, update, delete — as the write
    # admission and the write surfaces apply them (executor/write_capability.py).
    write_ops: list[str]
    alias: str | None
    description: str | None
    cache_ttl: int | None
    replicate: int | None  # REQ-826: NULL = inherit source
    region: str | None  # REQ-1921: where its data lives; null = no region
    draft: bool  # REQ-1921: out of service until its domain's owners release it
    row_materialize: bool  # REQ-1865: replicated row by row, fetched by key as statements ask
    load_protected: bool | None  # REQ-1141: NULL = inherit source
    off_peak_window: str | None  # REQ-1141: per-table window override
    off_peak_tz: str | None  # REQ-1141: per-table window zone override
    gql_naming_convention: str | None
    watermark_column: str | None
    columns: list[TableColumnType]
    # REQ-2074: null for a caller without view_governance, as a column's grant lists are.
    write_grants: list[WriteGrantType] | None = None
    unique_constraints: list[UniqueConstraintType] = strawberry.field(
        default_factory=list
    )  # REQ-1093
    api_endpoint: str | None = None
    # REQ-1907: operator-set role -> TTL seconds (effective = max(cache_ttl, ttl)); empty = none.
    role_ttl: list[RoleTtlType] = strawberry.field(default_factory=list)
    # REQ-318: what reads the table page by page ("endpoint" | "connection"), null when nothing
    # does; its declared paging; and the operator's graphql_remote.max_rows a connection table's
    # own bound may only lower.
    paging_kind: str | None = None
    pagination: PagingType | None = None
    paging_ceiling_rows: int | None = None
    view_sql: str | None = None
    view_metrics: ViewMetricsType | None = None  # REQ-1318: metric-composed view spec
    # REQ-1443: the data-quality contract this table's rows are the scan results of, verbatim.
    dq_contract: str | None = None
    # REQ-1934: the Data Profiler source this table is a member of.
    profiler_source_id: str | None = None
    # REQ-1670: the Cypher a neo4j table runs (from its persisted api_endpoints row).
    query_template: str | None = None
    # REQ-788: one logical table over a glob of files; source_file_column names an optional column
    # carrying each row's file path. None = a single-file table.
    file_glob: str | None = None
    source_file_column: str | None = None
    # REQ-1954: a ServiceNow table's import set (the staging table its inserts go through).
    import_set: str | None = None
    # REQ-2061: the arguments its source takes, fixed by name for every read ({} = none).
    arguments: JsonScalar | None = None
    # REQ-2062: the predicate over its own columns its replica's rows pass (None = every row).
    landing_filter: str | None = None
    change_signal: str | None = None  # REQ-929: override source change signal; None = inherit
    probe_query: str | None = None  # REQ-929: source-native freshness probe
    probe_type: str | None = None  # REQ-982: input-probe method; None = resolve per source class
    materialize: bool = False
    mv_refresh_interval: int = 300
    mv_debounce_quiet: float = 0.0  # REQ-963: seconds of quiet before firing; 0 = real-time
    mv_debounce_max_delay: float = 5.0  # REQ-963: staleness cap under continuous churn
    push_debounce_quiet: float = 0.0  # REQ-1733: kafka/websocket CDC landing debounce
    push_debounce_max_delay: float = 5.0  # REQ-1733: staleness cap under continuous message churn
    mv_consistency: str = (
        "shared"  # REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    )
    mv_preprocess: str | None = None  # REQ-957: inline preprocess(rows, ctx) hook source
    # REQ-2109: the bitemporal declaration, all three or none.
    business_key: list[str] = strawberry.field(default_factory=list)
    valid_time: str | None = None
    recorded_time: str | None = None
    mv_persist: str = "replace"  # REQ-965: replace | append | upsert
    mv_primary_key: list[str] = strawberry.field(default_factory=list)  # REQ-970: row identity
    mv_incremental: bool = False  # REQ-969: incremental maintenance
    mv_calendar: str | None = None  # REQ-962: periodic-snapshot calendar (None = not periodic)
    mv_grain: str | None = None  # REQ-962/1168: nesting grain ("daily".."annual") or "3WE"/"LFR"
    mv_allowed_lateness: float = 0.0  # REQ-961: seal-deadline slack (s)
    mv_expected_events: list[str] | None = None  # REQ-961: preflight freshness contract
    mv_business_day_grain: bool = False  # REQ-962: gate windows to business days
    modeling_role: str | None = None  # REQ-1320: "dimension" | "fact" | None
    modeling_history: str | None = None  # REQ-1320: "scd2" | "snapshot" | None
    # REQ-1634: the row's own product_id; read through the product_id resolver below.
    stored_product_id: strawberry.Private[str | None] = None
    enable_aggregates: bool = False
    enable_group_by: bool = False
    can_deploy_to_db: bool = False
    live: LiveDeliveryConfigType | None = None
    delta: DeltaConfigType | None = None  # REQ-874: incremental-reload declaration
    # REQ-1360: metadata-only, discoverability annotations derived from the same
    # numeric/comparable classification build_agg_fields_type uses (REQ-196). Never
    # governed/reusable — that stays exclusively the named `metrics:` path (REQ-1319).
    implicit_measures: list[ImplicitMeasureType] = strawberry.field(default_factory=list)
    implicit_dimensions: list[str] = strawberry.field(default_factory=list)

    @strawberry.field
    async def refresh_policy_summary(self) -> RefreshPolicySummaryType | None:  # REQ-1143
        """The effective refresh/serving policy summary, DERIVED SERVER-SIDE from the same planner
        resolution (federate + resolve_refresh_policy) per (source, engine). Returns None when the
        federation engine is not yet available (startup); never re-derives the decision tree in the
        client. Resolved lazily — only clients that request the field pay for it."""
        from provisa.api.admin._refresh_summary import summarize_table_policy

        return await summarize_table_policy(self)

    @strawberry.field
    def graphql_field_name(self) -> str | None:  # REQ-1634: GraphQL Queries panel
        """This table's root query field name in the compiled data-plane GraphQL schema, so the
        Data Product GraphQL Queries panel can build a runnable example query without duplicating
        provisa.compiler.naming.generate_name's domain-wide uniqueness algorithm client-side.
        None when no role's compiled schema currently exposes this table (e.g. mid-startup)."""
        from provisa.api.admin._graphql_field_name import resolve_graphql_field_name

        return resolve_graphql_field_name(
            domain_id=self.domain_id, schema_name=self.schema_name, table_name=self.table_name
        )

    @strawberry.field
    async def product_id(self) -> str | None:  # REQ-1634, REQ-1443 clause 10
        """The data product this table belongs to. A checker table (one carrying a dq_contract —
        registration admits a contract on no other source type) inherits the product of the table
        its contract scans, resolved through the compiled contexts the same way dq_dataset is;
        registration refuses a stored product_id on it, so the row's own column is never the
        answer there. Every other table answers with its own row."""
        if self.dq_contract is None:
            return self.stored_product_id
        from sqlalchemy import select

        from provisa.api.app import state
        from provisa.core.schema_org import registered_tables
        from provisa.dq.contract import contract_dataset, resolve_contract_target

        checker = state.source_types[self.source_id]
        meta = resolve_contract_target(contract_dataset(self.dq_contract, checker), state.contexts)
        if state.model_db is None:
            raise RuntimeError("tenant database not connected")
        async with state.model_db.acquire() as conn:
            res = await conn.execute_core(
                select(registered_tables.c.product_id).where(
                    registered_tables.c.id == meta.table_id
                )
            )
            row = res.fetchone()
        if row is None:
            raise ValueError(
                f"contract target table {meta.table_id} for {self.schema_name}.{self.table_name} "
                f"is not registered"
            )
        return row._mapping["product_id"]

    @strawberry.field
    def dq_dataset(self) -> str | None:  # REQ-1443
        """This table as a data-quality contract dataset — the pgwire names a checker scans
        (provisa.dq.contract.meta_dataset over the compiled TableMeta, REQ-641), so the dataset
        picker never rebuilds them client-side. None when no role's compiled context exposes this
        table (e.g. mid-startup)."""
        return _dq_dataset_of(self.id)

    @strawberry.field
    async def quality_checkers(self, info: strawberry.Info) -> list[QualityCheckerType]:  # REQ-1443
        """The checker tables whose contracts scan this table, as the caller's catalog lists
        them, each with its last scan's outcome. A contract scans a table by its dataset
        (dq_dataset), read from the contract by the checker's own parser; a table no role's
        compiled context exposes has no dataset, so no contract can name it."""
        from provisa.api.admin.catalog_scope import catalog_scope
        from provisa.api.app import state
        from provisa.dq.related import checkers_of

        dataset = _dq_dataset_of(self.id)
        if dataset is None:
            return []
        if state.model_db is None:
            raise RuntimeError("tenant database not connected")
        scope = catalog_scope(info)
        async with state.model_db.acquire() as conn:
            found = await checkers_of(conn, dataset)
        return [
            QualityCheckerType(
                table_id=c["table_id"],
                table_name=c["table_name"],
                source_id=c["source_id"],
                outcome=c["outcome"],
                scan_time=c["scan_time"].isoformat() if c["scan_time"] else None,
            )
            for c in found
            if scope.lists_table(c["table_id"], c["domain_id"])
        ]


@strawberry.type
class MaskSpecType:  # REQ-2059: how a mask entry masks the value
    type: str  # regex | constant | truncate
    pattern: str | None = None
    replace: str | None = None
    value: JsonScalar | None = None  # a constant's value, as typed (a number stays a number)
    precision: str | None = None


@strawberry.type
class MaskEntryType:  # REQ-2059: one entry of a column's ordered mask list
    to: list[str] | None = None  # grant entries, composites included; None: every other reader
    mask: MaskSpecType | None = None  # None: the readers it is for read the column unmasked
    why: JsonScalar | None = None  # REQ-2094: why the entry is what it is


@strawberry.input
class MaskSpecInput:  # REQ-2059
    type: str
    pattern: str | None = None
    replace: str | None = None
    value: JsonScalar | None = None
    precision: str | None = None


@strawberry.input
class MaskEntryInput:  # REQ-2059
    to: list[str] | None = None  # None: every other reader, the list's last entry (REQ-2153)
    mask: MaskSpecInput | None = None
    why: JsonScalar | None = None  # REQ-2094: the reason, purpose, bases and review-by date given


def mask_entry_types(masks: list[dict]) -> list[MaskEntryType]:
    """A stored mask list (REQ-2059) as the admin API answers it."""
    return [
        MaskEntryType(
            to=None if e.get("to") is None else list(e["to"]),
            mask=None if e["mask"] is None else MaskSpecType(**e["mask"]),
            why=e.get("why"),
        )
        for e in masks
    ]


def mask_entries_of(masks: list[MaskEntryInput]) -> list[dict]:
    """A mask list as sent (REQ-2059), as the model takes it."""
    return [
        {
            "to": None if e.to is None else list(e.to),
            "mask": None
            if e.mask is None
            else {
                "type": e.mask.type,
                "pattern": e.mask.pattern,
                "replace": e.mask.replace,
                "value": e.mask.value,
                "precision": e.mask.precision,
            },
            **({"why": e.why} if e.why else {}),  # REQ-2094
        }
        for e in masks
    ]


@strawberry.type
class TableColumnType:  # REQ-040, REQ-041, REQ-393, REQ-399
    id: int
    column_name: str
    # Grant lists and mask settings are null for a caller without view_governance (REQ-1134,
    # REQ-1958): null is "not shown to you", an empty list is "granted to nobody / everybody".
    visible_to: list[str] | None
    masks: list[MaskEntryType] | None  # REQ-2059: the ordered mask list
    alias: str | None
    computed_sql_alias: str
    computed_gql_alias: str  # REQ-1634: this column's field name in the compiled GraphQL schema
    description: str | None
    data_type: str | None = None
    native_filter_type: str | None = None
    # Whether the source requires a value for this parameter; None where it is not recorded.
    native_filter_required: bool | None = None
    # REQ-1986: a steward made the parameter required for the table.
    required_by_steward: bool = False
    # REQ-1739: dot-notation JSON extraction path for an `ingest` push source's column mapping
    # (provisa/ingest/router.py's `_extract_row`) — already a DB column and a core Column model
    # field, just never surfaced through this GraphQL type, so the table editor couldn't read it.
    path: str | None = None
    # REQ-421, REQ-2000: the column is an embedding of this registered model -- what a similarity
    # search over it and a text written to it are embedded by.
    embedding: bool = False
    embedding_model: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_alternate_key: bool = False
    scope: str = "domain"
    # REQ-1360: metadata-only discoverability flags — set when the owning table has
    # enable_aggregates/enable_group_by AND this column is eligible per the same
    # classification build_agg_fields_type (REQ-196) already applies.
    is_implicit_measure: bool = False
    is_implicit_dimension: bool = False
    domain_id: str | None = None
    # REQ-1908: epoch-number storage unit ("s" | "ms" | "us") of a temporal column; null otherwise.
    epoch_unit: str | None = None
    # REQ-1494: the column's kind of fake as declared, and whether it is stable.
    fake: str | None = None
    fake_stable: bool = False
    # REQ-1494: the portable definition version a stable fake is pinned to; set when saved.
    fake_stable_version: int | None = None
    # REQ-1494, REQ-1939: the column's synthetic rule, laid over its fake for generation only.
    synthetic_rule: str | None = None
    # REQ-2094: why the fake and synthetic rule are what they are, with who decided and when.
    fake_why: JsonScalar | None = None
    # REQ-1494: the column is tagged pii.
    is_pii: bool = False
    # REQ-2145: the glossary term the column is pii through, when it is; None when the column is
    # tagged itself or is not pii.
    pii_term: str | None = None


@strawberry.type
class InstanceApiType:  # REQ-1954
    """One of the APIs a ServiceNow instance publishes, and whether its source's companion OpenAPI
    source offers it."""

    namespace: str
    name: str
    route: str
    description: str
    scripted: bool
    added: bool


@strawberry.type
class CompanionImportSetType:  # REQ-1954
    """An import set added to a ServiceNow source's companion: its staging table and the table its
    transform map writes into."""

    staging: str
    table: str


@strawberry.type
class ImportSetType:  # REQ-1954
    """One of a ServiceNow instance's active transform maps into a table: its name, and the import
    set (staging table) a table bound to it takes its inserts through."""

    name: str
    import_set: str


@strawberry.type
class AvailableTableType:  # REQ-533
    name: str
    comment: str | None
    # REQ-1954: the heading the Register Table list shows the table under, where its source
    # groups its tables (a ServiceNow table's theme); None lists it ungrouped.
    group: str | None = None
    # REQ-1924: its source says nothing of what its rows hold, so the steward declares its
    # columns when registering it.
    columns_declared: bool = False
    # REQ-318: what would read the table page by page once registered ("endpoint" |
    # "connection"), and the paging its source suggests, for the steward to accept or edit.
    paging_kind: str | None = None
    pagination: PagingType | None = None
    paging_ceiling_rows: int | None = None  # graphql_remote.max_rows, for a connection table


@strawberry.type
class CrawledColumnType:  # REQ-1785
    name: str
    type: str
    nullable: bool


@strawberry.type
class CrawledTableType:  # REQ-1785
    name: str
    columns: list[CrawledColumnType]


@strawberry.type
class CrawledFileType:  # REQ-1785
    name: str
    path: str
    type: str
    tables: list[CrawledTableType]


@strawberry.type
class CrawlResultType:  # REQ-1785
    path: str
    total_files: int
    total_tables: int
    discovered: list[CrawledFileType]


@strawberry.type
class KaggleDatasetType:  # REQ-1783
    """One Kaggle datasets/list search result — the token-gated picker's row shape (ref/id,
    title, subtitle). Deliberately separate from AvailableTableType/AvailableSchema: those back
    the fixed-schema-picker pattern (a source's OWN catalog), whereas this backs a live search
    over Kaggle's entire public dataset catalog."""

    ref: str  # "owner/dataset-slug", passed to stageKaggleDataset — no Source stores it directly
    title: str
    subtitle: str


@strawberry.type
class KaggleStageResultType:  # REQ-1780/1781/1782
    """A staged dataset bundle, ready to register as ONE `files`-type Source pointed at
    `directory` (Amended 2026-09-19: was one plain csv/parquet Source PER FILE — see
    KaggleStageResultType's history in requirements.yaml for why that changed). v1 scope:
    CSV/Parquet only; a SQLite/.db-bearing bundle is rejected whole (success=False)."""

    success: bool
    message: str
    directory: str  # local directory path — becomes SourceInput.path verbatim (type "files")
    suggested_source_id: str


@strawberry.type
class AvailableColumnType:  # REQ-533
    name: str
    data_type: str
    comment: str | None
    native_filter_type: str | None = None
    # Whether the source requires a value for this parameter; None where it is not recorded.
    native_filter_required: bool | None = None
    is_primary_key: bool = False
    # REQ-1982: where a document source's nested field is read from (dot notation); None for a
    # top-level field.
    path: str | None = None
    # REQ-1965: the hidden attributes of a natively array/struct/map column, recorded with the
    # column at registration. Not part of the API: the registration form is not offered them.
    array_depth: strawberry.Private[int] = 0
    element_type: strawberry.Private[str | None] = None
    native_type: strawberry.Private[str | None] = None
    native_platform: strawberry.Private[str | None] = None


@strawberry.type
class RelationshipType:  # REQ-019, REQ-020, REQ-158, REQ-413
    id: str
    source_table_id: int
    target_table_id: int | None
    source_table_name: str
    source_domain_id: str
    target_table_name: str
    source_column: str
    target_column: str | None
    cardinality: str
    materialize: bool
    refresh_interval: int
    target_function_name: str | None
    function_arg: str | None
    alias: str | None = None
    graphql_alias: str | None = None
    computed_cypher_alias: str | None = None
    disable_cypher: bool = False
    # REQ-2141: the rows this edge joins are the same entity.
    same_entity: bool = False
    # REQ-1586: junction declaration. via_table_name names the associative table this edge
    # traverses; via_label_source names where the exposed Cypher type comes from. The ERD keys
    # its nodes by registered-table id, so it needs the id as well as the name (REQ-1588).
    via_table_id: int | None = None
    via_table_name: str | None = None
    via_source_column: str | None = None
    via_target_column: str | None = None
    via_type_column: str | None = None
    via_type_value: str | None = None
    via_label_source: str | None = None
    # REQ-2099: the target's parameters this edge feeds, each from a source column.
    parameters: list["RelationshipParameterType"] = strawberry.field(default_factory=list)

    @strawberry.field
    def auto_suggested(self) -> bool:
        return self.id.startswith("fk__")

    @strawberry.field
    def physical_name(self) -> str | None:  # REQ-471, REQ-1417
        """The relationship's name on the SQL plane — what ?include= and gRPC's include take.

        Derived by the naming authority, never by a client transliterating the GraphQL alias's
        casing: the convention is server configuration, so only the server can answer this.
        """
        from provisa.api.jsonapi.naming import physical_rel_name

        return physical_rel_name(self.graphql_alias) if self.graphql_alias else None


@strawberry.type
class RoleRateLimitType:  # REQ-1174
    """Per-role rate + query-complexity limits (None = unlimited on that dimension)."""

    requests_per_second: int | None = None
    max_query_complexity: int | None = None
    max_query_time_ms: int | None = None


@strawberry.type
class SessionVarType:  # REQ-1682: one of a role's constant session values
    name: str
    value: str


@strawberry.type
class RoleType:  # REQ-042
    id: str
    # REQ-2080: what the role is for. Shown to everyone who sees the role: it explains, it grants
    # nothing.
    description: str
    # Null for a caller who neither holds user_management nor holds the role itself: a role's
    # definition is the right-holder's, and everyone else sees only that it exists.
    capabilities: list[str] | None
    domain_access: list[str] | None
    demonstrated: list[str] | None = strawberry.field(default_factory=list)  # REQ-1602
    # REQ-2155: what the role holds -- its own capabilities and every ancestor's; null as
    # capabilities is. The client builds the caller's rights from it.
    effective_capabilities: list[str] | None = None
    rate_limit: RoleRateLimitType | None = None  # REQ-1174
    parents: list[str] = strawberry.field(default_factory=list)  # REQ-2155
    # REQ-2155: a standard role is Provisa's and locked: extended with child roles, never edited.
    standard: bool = False
    # REQ-1921: with data_residency, the region values its grant covers; null as capabilities is.
    residency_values: list[str] | None = strawberry.field(default_factory=list)
    # REQ-1682: the role's own constant session values, by name; null as capabilities is.
    session_vars: list[SessionVarType] | None = strawberry.field(default_factory=list)
    # REQ-2077: a grant of the role unused this many days is removed; null: never, or not shown.
    inactivity_days: int | None = None


@strawberry.type
class UserSummaryType:  # REQ-609/REQ-1634: resolves an owner_role/steward to the individuals it grants to
    user_id: str | None  # user_id and email: user_management only, null otherwise
    display_name: str | None = None
    email: str | None = None


@strawberry.type
class RLSRuleType:  # REQ-041, REQ-402, REQ-1679
    id: int
    table_id: int | None
    domain_id: str | None
    role_id: str
    filter_expr: str
    description: str  # REQ-2081: what the rule is for
    action_name: str | None = None  # REQ-1679
    tag_id: str | None = None  # REQ-1384: the rule applies to every table carrying this tag
    why: JsonScalar | None = None  # REQ-2094: reason, bases, review_on; who decided it and when


# --- Input types for mutations ---


@strawberry.input
class PagingInput:  # REQ-318
    type: str | None = None
    cursor_field: str | None = None
    cursor_param: str | None = None
    page_param: str | None = None
    page_size_param: str | None = None
    page_size: int | None = None
    max_pages: int | None = None
    max_rows: int | None = None
    rows_field: str | None = None  # REQ-316: where a REST answer's rows are


@strawberry.input
class RoleTtlInput:  # REQ-1907
    role: str
    ttl: int


@strawberry.input
class SourceCdcConfigInput:  # REQ-824
    bootstrap_servers: str
    topic_prefix: str
    schema_registry_url: str | None = None
    # REQ-1951: how the registry is reached. The password and the token are returned as the
    # vault references the row holds, never as credentials.
    schema_registry_auth: str = "none"
    schema_registry_username: str | None = None
    schema_registry_password: str | None = None
    schema_registry_token: str | None = None
    schema_registry_client_cert: str | None = None
    schema_registry_client_key: str | None = None
    schema_registry_ca: str | None = None
    consumer_group_id: str | None = (
        None  # REQ-931: None = inherit Provisa-level cdc_consumer_group_id
    )


@strawberry.input
class SourceInput:  # REQ-012
    id: str
    type: str
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""
    path: str | None = None
    description: str = ""
    allowed_domains: list[str] = strawberry.field(default_factory=list)
    mapping_json: str | None = None
    # Connection extras the standard args cannot carry — a warehouse's account/http_path, a
    # remote-schema override, an Exasol server-certificate fingerprint. The config path has always
    # been able to declare these (Source.federation_hints); this is the same channel through the
    # admin API, as a JSON object literal.
    federation_hints_json: str | None = None
    change_signal: str = "ttl"  # REQ-929: source default change signal
    load_protected: bool = False  # REQ-1141: scheduled-refresh-only load protection
    off_peak_window: str | None = None  # REQ-1141: "HH:MM-HH:MM" maintenance window
    off_peak_tz: str = "UTC"  # REQ-1141: IANA zone for the window
    # The "Load Management and Timeliness" panel's other settings (REQ-1909), carried by the same
    # create/update input as every other source field.
    cache_enabled: bool = True
    cache_ttl: int | None = None
    replicate: int | None = None  # REQ-826: None = Default; -1 never, N hot, 0 always
    # REQ-1921: the region the admin form starts this source's new tables in (None = the
    # connected one). Ignored by updateSource: it changes through setSourceRegion.
    region: str | None = None
    max_live_concurrency: int | None = None  # REQ-1909: None = no cap; else >= 1
    sentinel_path: str | None = None  # REQ-1148
    download_from: str | None = None  # REQ-2113
    # REQ-2113: a single-file CSV or Parquet source's table set up with it, as a draft, in this
    # domain, named and typed from the file as Register Table proposes it. Not stored on the source.
    register_table_draft: bool = False
    table_domain_id: str | None = None
    freshness_gate: bool = False  # REQ-860
    cdc: SourceCdcConfigInput | None = None  # REQ-824: source-level CDC transport


@strawberry.input
class DomainInput:  # REQ-533, REQ-609
    id: str
    description: str = ""
    steward: str | None = None  # REQ-609
    graphql_alias: str | None = None
    parent_domain_id: str | None = None  # REQ-1363


@strawberry.input
class DataProductInput:  # REQ-1634, REQ-1660
    id: str
    domain_id: str
    name: str
    owner_role: str | None = None
    team_role: str | None = None
    purpose: str = ""
    limitations: str = ""
    usage: str = ""
    version: str | None = None
    status: str | None = None
    sla: str | None = None
    support: str | None = None
    # Nullable, default null: a default of `{}` on the JSON scalar cannot be printed as a GraphQL
    # default value, and the standard introspection query (which asks every input's defaultValue)
    # then answered with an error. Absent or null means no custom properties.
    custom_properties: JsonScalar | None = None


@strawberry.input
class TagInput:  # REQ-1373
    id: str
    description: str = ""
    applies_to: list[str] = strawberry.field(default_factory=list)
    reason_policy: str = "optional"  # hidden | optional | required
    expires_policy: str = "optional"
    param_policy: str = "none"  # REQ-1467: none | required
    sensitive: bool = False  # REQ-1943: the Sensitive data option


@strawberry.input
class TagParamValueInput:  # REQ-1467
    """One permitted parameter value for a parameterized tag.

    ``tag_id`` is the base id ("entity"), never an assigned "entity:customer" — a value belongs
    to the tag, not to another value.
    """

    tag_id: str
    value: str
    description: str = ""


@strawberry.input
class TagAssignmentInput:  # REQ-1377
    tag_id: str
    object_type: str  # source | table | column | relationship
    source_id: str | None = None
    table_id: int | None = None
    column_name: str | None = None
    relationship_id: str | None = None
    command_name: str | None = None
    reason: str | None = None
    expires_on: str | None = None  # ISO date
    # REQ-2094: purpose, bases (ids) and review_on given; reason is the field above. Left out,
    # the stored ones stand.
    why: JsonScalar | None = None
    # REQ-2145: the glossary term an object_type "term" assignment is on.
    term_id: int | None = None
    # REQ-2145: on a column, put the tag on the column's term instead, so it covers every column
    # with the same meaning, those to come included.
    also_term: bool = False


@strawberry.input
class ColumnInput:  # REQ-040, REQ-041, REQ-393, REQ-399
    name: str
    visible_to: list[str]
    masks: list[MaskEntryInput] = strawberry.field(default_factory=list)  # REQ-2059
    alias: str | None = None
    description: str | None = None
    # Canonical IR data-type (REQ-846) a steward assigned during schema discovery. Authoritative for
    # a manually-defined column (a non-SQL source the engine can't introspect); the landing write
    # face maps IR → the store's physical type. Null when the type is filled by introspection.
    data_type: str | None = None
    # REQ-1739: dot-notation JSON extraction path (e.g. "payload.order_id") for an `ingest` push
    # source's per-column mapping. Mirrors the core Column model's own `path` field
    # (provisa/core/models.py) and provisa/ingest/router.py's `_extract_row`, which already reads
    # it — this input type just never carried it, so the Sources UI had no way to set it.
    path: str | None = None
    native_filter_type: str | None = None
    # Whether the source requires a value for this parameter; None where it is not recorded.
    native_filter_required: bool | None = None
    # REQ-1986: a steward made the parameter required for the table.
    required_by_steward: bool = False
    # REQ-421, REQ-2000: the column is an embedding of this registered model. The form carries
    # both, so an edit keeps or changes them.
    embedding: bool = False
    embedding_model: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_alternate_key: bool = False
    scope: str = "domain"
    # REQ-1908: the source stores this temporal column as an epoch number in this unit
    # ("s" | "ms" | "us"). Mirrors the core Column model's epoch_unit.
    epoch_unit: str | None = None
    # REQ-1494: the column's kind of fake as declared, and whether it is stable.
    fake: str | None = None
    fake_stable: bool = False
    # REQ-1494, REQ-1939: the column's synthetic rule, laid over its fake for generation only.
    synthetic_rule: str | None = None
    # REQ-2094: the reason, purpose, bases and review-by date given for the fake and rule.
    fake_why: JsonScalar | None = None


@strawberry.input
class UniqueConstraintInput:  # REQ-1093
    name: str
    columns: list[str]


@strawberry.input
class LiveOutputConfigInput:  # REQ-565
    type: str  # "sse" | "kafka"
    topic: str | None = None
    key_column: str | None = None
    bootstrap_servers: str | None = None
    role: str | None = None  # REQ-286: the role a Kafka output publishes as


@strawberry.input
class LiveKafkaParamsInput:  # REQ-813
    topic: str
    format: str = "json"
    key_column: str | None = None
    landing_time_column: str | None = None  # REQ-813 (amended 2026-10-21)


@strawberry.input
class LiveDeliveryConfigInput:  # REQ-565, REQ-813
    strategy: str = "poll"  # poll | native | debezium | kafka
    watermark_column: str | None = None
    poll_interval: int = 10
    kafka: LiveKafkaParamsInput | None = None
    query_id: str | None = None
    outputs: list[LiveOutputConfigInput] = strawberry.field(default_factory=list)


@strawberry.input
class DeltaConfigInput:  # REQ-874
    """The table form's incremental-reload (delta) declaration. The cursor field is the table's
    ``watermark_column``; a delta is defined only for a SQL source (refused by name otherwise)."""

    query: str | None = None  # source-native delta query; None = generated for a SQL source
    apply: str = "upsert"  # upsert | append
    deletes: str = "none"  # none | tombstone
    tombstone_column: str | None = None
    rebuild_every: int | None = None  # seconds between whole rebuilds; None = never on a clock


@strawberry.input
class TableInput:  # REQ-013, REQ-016, REQ-133, REQ-135, REQ-252
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    columns: list[ColumnInput]
    # REQ-318: the table's paging (the steward's, from what the source suggested); null declares
    # none. Left out, a registration declares none and an update keeps the stored paging.
    pagination: PagingInput | None = strawberry.UNSET
    alias: str | None = None
    description: str | None = None
    watermark_column: str | None = None
    unique_constraints: list[UniqueConstraintInput] = strawberry.field(
        default_factory=list
    )  # REQ-1093
    view_sql: str | None = None
    # REQ-1921: the region the table's data lives in. Left out, a new table starts in its
    # source's region, else the connected one (a view: the connected one); null = no region.
    # On updateTable a region given moves the table (residency judged as setTableRegion judges
    # it); left out, the table stays where it is.
    region: str | None = strawberry.UNSET
    # REQ-1443: the data-quality contract (soda contract / GX suite) this table's rows are the scan
    # results of. The contract names what it scans, so the observed target is DERIVED from it
    # (REQ-939) and the results columns are replaced by the shipped schema at load.
    dq_contract: str | None = None
    # REQ-1934: the Data Profiler source this table joins; None leaves (or keeps it out of) any.
    profiler_source_id: str | None = None
    # REQ-1670: the Cypher that produces a neo4j table's rows; required under a neo4j source.
    query_template: str | None = None
    # REQ-788: a files source table read as ONE logical table over the files this glob matches
    # under the source's path; source_file_column names an optional column carrying each row's
    # file path. None = a single-file table.
    file_glob: str | None = None
    source_file_column: str | None = None
    # REQ-1954: a ServiceNow table's import set: the staging table of the transform map its inserts
    # go through. None: its inserts are written to the table directly.
    import_set: str | None = None
    # REQ-2061: the arguments its source takes, fixed by name for every read. Nullable, default
    # null: a JSON scalar's `{}` default cannot be printed as a GraphQL default value.
    arguments: JsonScalar | None = None
    # REQ-2062: a SQL predicate over the table's own columns, applied when its replica lands.
    landing_filter: str | None = None
    # REQ-1318: declarative metric-composed view definition; mutually exclusive with view_sql.
    # The server generates (and regenerates on metric change) the view SELECT from this spec.
    view_metrics: ViewMetricsInput | None = None
    load_protected: bool | None = None  # REQ-1141: NULL = inherit source load protection
    off_peak_window: str | None = None  # REQ-1141: per-table "HH:MM-HH:MM" window override
    off_peak_tz: str | None = None  # REQ-1141: per-table window zone override
    change_signal: str | None = None  # REQ-929: override source change signal; None = inherit
    # REQ-1907: the table's own landing Cache TTL; null: none of its own (the source's applies).
    # Left out, a registration has none and an update keeps the stored one.
    cache_ttl: int | None = strawberry.UNSET
    # REQ-1865: replicated row by row, fetched by primary key as statements name the rows. Left
    # out, a registration is not and an update keeps what the table has.
    row_materialize: bool | None = strawberry.UNSET
    probe_query: str | None = None  # REQ-929: source-native freshness probe
    probe_type: str | None = None  # REQ-982: input-probe method; None = resolve per source class
    materialize: bool = False
    mv_refresh_interval: int = 300
    mv_debounce_quiet: float = 0.0  # REQ-963: seconds of quiet before firing; 0 = real-time
    mv_debounce_max_delay: float = 5.0  # REQ-963: staleness cap under continuous churn
    push_debounce_quiet: float = 0.0  # REQ-1733: kafka/websocket CDC landing debounce
    push_debounce_max_delay: float = 5.0  # REQ-1733: staleness cap under continuous message churn
    mv_consistency: str = (
        "shared"  # REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    )
    mv_preprocess: str | None = None  # REQ-957: inline preprocess(rows, ctx) hook source
    # REQ-2109: the bitemporal declaration, all three or none.
    business_key: list[str] = strawberry.field(default_factory=list)
    valid_time: str | None = None
    recorded_time: str | None = None
    mv_persist: str = "replace"  # REQ-965: replace | append | upsert
    mv_primary_key: list[str] = strawberry.field(default_factory=list)  # REQ-970: row identity
    mv_incremental: bool = False  # REQ-969: incremental maintenance
    mv_calendar: str | None = None  # REQ-962: periodic-snapshot calendar (None = not periodic)
    mv_grain: str | None = None  # REQ-962/1168: nesting grain ("daily".."annual") or "3WE"/"LFR"
    mv_allowed_lateness: float = 0.0  # REQ-961: seal-deadline slack (s)
    mv_expected_events: list[str] | None = None  # REQ-961: preflight freshness contract
    mv_business_day_grain: bool = False  # REQ-962: gate windows to business days
    modeling_role: str | None = None  # REQ-1320: "dimension" | "fact" | None
    modeling_history: str | None = None  # REQ-1320: "scd2" | "snapshot" | None
    product_id: str | None = None  # REQ-1634
    enable_aggregates: bool = False
    enable_group_by: bool = False
    discover: bool = False  # REQ-252: infer columns from the live NoSQL source at registration
    live: LiveDeliveryConfigInput | None = None  # REQ-565: live delivery config
    delta: DeltaConfigInput | None = None  # REQ-874: incremental-reload declaration
    # REQ-2109 (settled): the settings that also have mutations of their own travel with the table,
    # so one registerTable / updateTable applies all of them in one control-plane transaction and a
    # refusal of any one leaves nothing saved. Left out (UNSET), a registration takes a new table's
    # value and an update keeps the stored one.
    gql_naming_convention: str | None = strawberry.UNSET
    role_ttl: list[RoleTtlInput] | None = strawberry.UNSET  # REQ-1907: the whole list
    replicate: int | None = strawberry.UNSET  # REQ-826: null inherits the source's value
    write_grants: list[WriteGrantInput] | None = strawberry.UNSET  # REQ-2074: the whole set
    draft: bool | None = strawberry.UNSET  # REQ-1921: false releases it
    # REQ-1934, REQ-2109: row rules of this table (table_id is the table being saved), saved with
    # it: a profiler result table is registered with the rules its profiled table has.
    row_rules: list[RLSRuleInput] | None = strawberry.UNSET


@strawberry.type
class RelationshipParameterType:  # REQ-2099
    """A parameter of a relationship's target that the edge feeds, and the source column whose
    value it is given."""

    parameter: str
    source_column: str


@strawberry.input
class RelationshipParameterInput:  # REQ-2099
    parameter: str
    source_column: str


@strawberry.input
class RelationshipInput:  # REQ-019, REQ-020, REQ-158, REQ-413
    id: str
    source_table_id: str  # table name (resolved to ID)
    target_table_id: str = ""  # empty for computed relationships
    source_column: str
    target_column: str = ""  # empty for computed relationships
    cardinality: str
    materialize: bool = False
    refresh_interval: int = 300
    target_function_name: str | None = None
    function_arg: str | None = None
    alias: str | None = None  # e.g. WORKS_FOR; unique per (source_table, alias)
    graphql_alias: str | None = None
    disable_cypher: bool = False  # when True, exclude from Cypher graph edges
    # REQ-2141: mark the rows this edge joins as the same entity (repositories/identity.py).
    same_entity: bool = False
    # REQ-1586: junction declaration — the associative table this edge traverses, its two key
    # columns, an optional discriminator, and which source names the exposed Cypher type.
    via_table: str = ""  # registered table name of the junction; empty = FK/PK-backed edge
    via_source_column: str = ""
    via_target_column: str = ""
    via_type_column: str | None = None
    via_type_value: str | None = None
    via_label_source: str = ""
    # REQ-2099: the target's parameters this edge feeds, each from a source column.
    parameters: list[RelationshipParameterInput] = strawberry.field(default_factory=list)
    record_candidate: bool = False  # when True, also insert accepted relationship_candidates record
    # REQ-2145: a save that re-homes key refs off a tagged term takes the tags off those columns:
    # each lost tag confirmed, or carried to the term the column moves to.
    confirm_tag_loss: list[str] = strawberry.field(default_factory=list)
    carry_tags: bool = False


@strawberry.input
class EntityInput:  # REQ-1164: dimension / hub+satellite sugar → lowers to a (bitemporal) MV
    name: str
    source: str  # source relation the projection reads from (e.g. "raw.customers")
    domain_id: str
    key: list[str]
    attributes: list[str] = strawberry.field(default_factory=list)
    history: str = "none"  # none | scd2 | snapshot
    # REQ-2109: a historized entity's valid-time and recorded-time columns.
    valid_time: str | None = None
    recorded_time: str | None = None
    visible_to: list[str] = strawberry.field(default_factory=lambda: ["public"])


@strawberry.input
class MeasureInput:  # REQ-1164
    column: str
    agg: str = "sum"  # sum | avg | min | max | count


@strawberry.input
class DimRefInput:  # REQ-1164
    entity: str  # referenced Entity name
    via: str  # FK column on the fact source


@strawberry.input
class FactInput:  # REQ-1164: star fact / DV link sugar → lowers to an aggregate MV + relationships
    name: str
    source: str
    domain_id: str
    grain: list[str]
    measures: list[MeasureInput] = strawberry.field(default_factory=list)
    dimensions: list[DimRefInput] = strawberry.field(default_factory=list)
    visible_to: list[str] = strawberry.field(default_factory=lambda: ["public"])


@strawberry.type
class ViewMetricsType:  # REQ-1318: declarative metric-composed view spec (persisted alongside view_sql)
    metrics: list[str]
    dimensions: list[str]
    filters: list[str] = strawberry.field(default_factory=list)


@strawberry.input
class ViewMetricsInput:  # REQ-1318: declarative metric-composed view definition
    metrics: list[str]
    dimensions: list[str]
    filters: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class DqCheckType:  # REQ-1443: one check as the contract panel renders and edits it
    """``definition`` is the check's own ARGS as authored (a soda check body, a GX expectation's
    kwargs), not a normalized summary — which is what lets the panel round-trip a pasted contract
    without dropping a threshold it has no editor for. The dialect's envelope is not in it: the
    serializer restates it, so the row shows only what an operator can change. ``extra`` carries the
    keys GX's own serializer writes beside ``type``/``kwargs`` (``id``, ``meta``, ``notes``) verbatim
    so a pasted suite keeps them; it is empty for soda."""

    column_name: str  # "" for a dataset-level check
    check_type: str
    definition: str
    extra: str = ""


@strawberry.input
class DqCheckInput:  # REQ-1443
    column_name: str
    check_type: str
    definition: str
    extra: str = ""


@strawberry.type
class DqCheckParamType:  # REQ-1443 clause 7: one editable field of a check's body
    """``value_type`` is what the editor renders (number | string | number_list | string_list |
    column | enum), and ``choices`` is non-empty only for ``enum``."""

    name: str
    value_type: str
    required: bool
    choices: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class DqCheckKindType:  # REQ-1443 clause 7: one check the picker may offer
    """The vocabulary is the CHECKER's, served from :mod:`provisa.dq.catalog` rather than held in
    the browser — a picker carrying its own copy of soda's or GX's check list is a second dialect
    that drifts from the one the worker runs. Empty ``comparators`` means the check takes no
    threshold; ``levels`` is ``["fail"]`` alone for GX, which has no warn level."""

    check_type: str
    scope: str  # column | dataset
    params: list[DqCheckParamType] = strawberry.field(default_factory=list)
    comparators: list[str] = strawberry.field(default_factory=list)
    metrics: list[str] = strawberry.field(default_factory=list)
    levels: list[str] = strawberry.field(default_factory=list)
    threshold_units: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class DqCheckColumnType:  # REQ-1443 clause 7
    """One column of the observed dataset and the checks offerable on ITS type."""

    name: str
    data_type: str | None
    checks: list[DqCheckKindType] = strawberry.field(default_factory=list)


@strawberry.type
class DqCheckCatalogType:  # REQ-1443 clause 7: the picker's offer, scoped to the real dataset
    """``columns`` is empty and ``error`` non-null when the contract's dataset resolves to no
    governed table — the same failure the registration reports, surfaced while the operator is still
    editing rather than at scan time."""

    dataset_checks: list[DqCheckKindType] = strawberry.field(default_factory=list)
    columns: list[DqCheckColumnType] = strawberry.field(default_factory=list)
    error: str | None = None


@strawberry.input
class DqCheckBuildInput:  # REQ-1443 clause 7
    """What the picker, threshold editor and severity control produce for ONE check.

    ``params`` is JSON text because a check's body is per-check-type (soda's valid_values, GX's
    regex) — modelling it as typed fields would put the checker's vocabulary in the schema, which is
    the thing :mod:`provisa.dq.catalog` exists to keep in one place."""

    check_type: str
    column_name: str = ""
    params: str = ""
    comparator: str = ""
    threshold_value: float | None = None
    metric: str = ""
    unit: str = ""
    level: str = "fail"


@strawberry.type
class DqCheckDefinitionType:  # REQ-1443 clause 7
    """One built check's own text, or the reason it could not be built."""

    definition: str = ""
    error: str | None = None


@strawberry.type
class QueryPreviewColumnType:  # REQ-1670, REQ-1683
    name: str
    data_type: str
    # REQ-1980: where a pipeline's nested field is read from (dot notation); None for a top field.
    path: str | None = None


@strawberry.type
class QueryPreviewType:  # REQ-1670/REQ-1683: a query preview for Register Table on a query-API source
    """``error`` is non-null when the source is unreachable, the query fails, or a column returns
    a node/list instead of a scalar; the form keeps the operator's text and shows the message."""

    rows: list[JsonScalar] = strawberry.field(default_factory=list)
    columns: list[QueryPreviewColumnType] = strawberry.field(default_factory=list)
    error: str | None = None


@strawberry.type
class QdrantVectorType:  # REQ-1983: one vector of a Qdrant collection
    name: str  # "" for the collection's unnamed vector
    size: int
    distance: str  # Cosine, Dot, Euclid or Manhattan: what a search's score is
    # The registered models whose dimensions are the vector's size: what its column may name.
    models: list[str] = strawberry.field(default_factory=list)


@strawberry.type
class DqContractType:  # REQ-1443: the parsed contract behind the builder panel
    """``error`` is non-null when the raw text does not parse or names no dataset; the panel keeps
    the operator's text and shows the message rather than replacing it with an empty builder."""

    dataset: str | None = None
    checker: str = ""
    checks: list[DqCheckType] = strawberry.field(default_factory=list)
    error: str | None = None


@strawberry.type
class DqContractTextType:  # REQ-1443: edited rows serialized back into the contract's own dialect
    """The inverse of :class:`DqContractType`. Serialization is a server call so the dialect has one
    implementation; ``error`` carries a rejected build (an unparseable check body, a dataset that is
    not the three-part form) rather than emitting text the checker would refuse."""

    text: str = ""
    error: str | None = None


@strawberry.type
class DqDryRunType:  # REQ-1443 clause 7: outcomes WITHOUT landing them
    """The failure mode this prevents is a contract that lands nothing but passing rows because its
    dataset name resolved somewhere else — so ``rows_tested`` and the per-check outcomes are shown as
    the checker reported them, not summarized to a verdict."""

    success: bool
    message: str = ""
    checker_version: str | None = None
    checks: list["DqDryRunCheckType"] = strawberry.field(default_factory=list)


@strawberry.type
class DqDryRunCheckType:  # REQ-1443
    column_name: str | None
    check_type: str
    outcome: str
    rows_tested: int | None = None
    failed_rows: int | None = None
    value: float | None = None
    diagnostics: str | None = None  # the per-check-type jsonb block, as JSON text


@strawberry.type
class MetricType:  # REQ-1317: a governed, named aggregate definition (grain bound at query time)
    name: str
    expression: str
    datatype: str | None = None
    description: str | None = None
    ai_context: str | None = None  # REQ-1319: definition text for AI consumers
    # Null for a caller without view_governance (REQ-1134, REQ-1958).
    visible_to: list[str] | None = None
    from_fact: str | None = None  # REQ-1320: set when auto-registered from a fact measure


@strawberry.input
class MetricInput:  # REQ-1317
    name: str
    expression: str  # must parse under sqlglot and contain ≥1 aggregate function
    datatype: str | None = None
    description: str | None = None
    ai_context: str | None = None  # REQ-1319
    # REQ-1317: None at create is the acting role and Viewer; the roles named otherwise.
    visible_to: list[str] | None = None


@strawberry.input
class RoleRateLimitInput:  # REQ-1174
    requests_per_second: int | None = None
    max_query_complexity: int | None = None
    max_query_time_ms: int | None = None


@strawberry.input
class SessionVarInput:  # REQ-1682
    name: str
    value: str


@strawberry.input
class RoleInput:  # REQ-042
    id: str
    description: str  # REQ-2080: required; an empty one is refused
    capabilities: list[str]
    domain_access: list[str]
    rate_limit: RoleRateLimitInput | None = None  # REQ-1174
    parents: list[str] = strawberry.field(default_factory=list)  # REQ-2155: in declared order
    # REQ-1921: with data_residency, the region values its grant covers (regions, "no_region").
    residency_values: list[str] = strawberry.field(default_factory=list)
    # REQ-1682: the role's own constant session values; the whole set, as the form holds it.
    session_vars: list[SessionVarInput] = strawberry.field(default_factory=list)
    # REQ-2077: remove a grant of the role once unused this many days; null: never.
    inactivity_days: int | None = None


@strawberry.input
class WritePresetInput:  # REQ-2074
    column: str
    source: str  # session | literal | now
    name: str | None = None
    value: str | None = None


@strawberry.input
class WriteGrantInput:  # REQ-2074
    """One role's write grant on a table for one operation. ``rows`` / ``check`` null: the
    role's read row filter; ``TRUE``: every row."""

    role: str
    operation: str
    columns: list[str] = strawberry.field(default_factory=list)
    rows: str | None = None
    check: str | None = None
    presets: list[WritePresetInput] = strawberry.field(default_factory=list)
    # REQ-2094: reason, purpose, bases and review_on given; left out, the stored ones stand.
    why: JsonScalar | None = None


@strawberry.input
class RLSRuleInput:  # REQ-041, REQ-402
    table_id: str | None = None  # table name (resolved to ID); mutually exclusive with domain_id
    domain_id: str | None = None  # domain ID for domain-level rules
    action_name: str | None = None  # REQ-1679: a tracked function/webhook, by name
    tag_id: str | None = None  # REQ-1384: a user tag; the rule covers every table carrying it
    role_id: str = ""
    filter_expr: str = ""
    description: str = ""  # REQ-2081: required; an empty one is refused
    # REQ-2094: reason, bases and review_on given; left out, the stored ones stand.
    why: JsonScalar | None = None


@strawberry.type
class MVType:  # REQ-135, REQ-158, REQ-159, REQ-160
    id: str
    source_tables: list[str]
    target_table: str
    refresh_interval: int
    enabled: bool
    status: str
    last_refresh_at: float | None
    row_count: int | None
    last_error: str | None
    # REQ-2109: the table whose version log this view is (bitemporal_capture); None for any other.
    version_log_of: str | None = None
    allowed_lateness: float = 0.0  # REQ-961: how late a row its refresh reads may be committed


@strawberry.type
class CacheStatsType:
    total_keys: int
    hit_count: int
    miss_count: int
    store_type: str
    # Redis-only operational stats (None for the noop store, which exposes none of them).
    used_memory_bytes: int | None = None
    max_memory_bytes: int | None = None
    evicted_keys: int | None = None
    expired_keys: int | None = None
    connected_clients: int | None = None
    ops_per_sec: int | None = None


@strawberry.type
class CacheTableStatType:
    table_id: int
    cached_entries: int


@strawberry.type
class ClusterNodeType:  # REQ-1916
    """A node now in the cluster (the platform state store's node list)."""

    node_id: str
    host: str
    pid: int
    mode: str  # every | query | coordinator
    # REQ-1922: the platform region the node serves; null when the platform declares none (and
    # the admin then shows no region at all).
    region: str | None
    started_at: str
    last_seen: str


@strawberry.type
class HotTableStatType:
    table_name: str
    catalog: str
    schema_name: str
    row_count: int
    # What Provisa is keeping for this table, as one value rather than a tier/state pair:
    # "hot_candidate" (registered for promotion, nothing mirrored yet, so no row count),
    # "hot" (mirrored into the response store for JOIN inlining),
    # "replica" (past its Hot threshold and served from its replica, REQ-826),
    # "replica_building" (past its threshold, read live while its replica is built; no row
    # count yet). REQ-241 makes the tiers exclusive.
    kind: str
    # REQ-1922: the home region of the underlying table (null when none / no regions declared), so
    # the admin hot-tables list carries a region column and filters by region like the other lists.
    region: str | None = None


@strawberry.type
class ReplicaIndexType:  # REQ-2015
    """One index a replica's standing landing asked for, and how it stands."""

    name: str
    columns: list[str]
    primary: bool  # the table's declared key, enforced as a key
    state: str  # built | pending | failed
    # A failed one: its English text, and the code the UI words (replica.index_duplicate_key,
    # replica.index_failed).
    error: str | None
    error_code: str | None


@strawberry.type
class ReplicaBuildType:  # REQ-1915
    """One replica and its build, as an operator sees it."""

    source_id: str
    schema_name: str
    table_name: str
    # REQ-1922: the home region of this replica's table (null when none / no regions declared), so
    # the replica-status list carries a region column and filters by region like the other lists.
    region: str | None = None
    # idle (built, nothing pending) | requested | building | failed | retired (the model no
    # longer declares it; it is dropped after a grace period)
    state: str
    requested_reason: str | None
    method: str | None  # engine_statement | stream_batches, of the running or last build
    load_kind: str | None  # bulk_stream | row_copy
    started_at: str | None  # ISO 8601, UTC
    # A float: a GraphQL Int is 32 bits, and a replica can hold more than 2^31 rows.
    rows_copied: float | None
    rows_per_second: float | None  # a running build's rate so far
    # A running build waiting out a throttle: {"by", "seconds", "until"}; None when not.
    wait: JsonScalar | None
    # The rows of the last build that copied the whole table; None before the first. An
    # estimate of the table's size for a running build's progress, never a promise.
    completed_rows: float | None
    # A running build that lands a part at a time: {"done", "total", "unit", "first"}.
    progress: JsonScalar | None
    # The replica is read while its first build is still filling it.
    partial: bool
    completed_at: str | None  # None: there is no replica yet
    verified_at: str | None  # REQ-855: its probe last found the source unchanged; None: not since
    next_refresh_at: str | None
    # The last failure: its English text, and (REQ-1350) the code and params the UI renders
    # in its own language when the cause is one Provisa names.
    last_error: str | None
    last_error_code: str | None
    last_error_params: JsonScalar | None
    # Builds that have failed in a row since the last one that completed. A failing build is
    # tried again after ``replication.retry_interval``, doubled for each failure in a row up
    # to ``replication.retry_interval_max``; ``next_attempt_at`` is when (ISO 8601, UTC).
    failed_attempts: int
    next_attempt_at: str | None
    # Why a requested build has not started: English text and its code.
    waiting_on: str | None
    waiting_on_code: str | None
    # REQ-1861: a table that follows its source's change feed, while the listener is down:
    # since when (ISO 8601, UTC) and the server's own reason. None while it is watching.
    feed_down_since: str | None
    feed_error: str | None
    # REQ-874: the last build of a delta table. ``delta_skipped`` names why it was a whole rebuild
    # instead of a delta (a ``delta.SKIP_*`` code), or None when a delta was applied; ``delta_cursor``
    # is the stored watermark the next delta resumes from. Both None for a non-delta table.
    delta_skipped: str | None
    delta_cursor: JsonScalar | None
    # What the last completed build had to say of the copy it made: a list of {code, params},
    # each a code the UI words in its own language (``replication.unreadable_messages``) and
    # its particulars. Empty: nothing.
    build_notes: JsonScalar
    # REQ-2015: each index the standing landing asked for, and how it stands.
    indexes: list[ReplicaIndexType]


@strawberry.type
class ReplicaBuildsType:  # REQ-1915
    builds: list[ReplicaBuildType]
    # The last failure of the step that brings replicas in line with the model, for the acting
    # org; None when the last pass succeeded.
    convergence_error: str | None
    convergence_error_at: str | None


@strawberry.type
class MaterializeStoreInfoType:
    engine_name: str
    # None when the engine has no materialization store configured yet — the panel still shows the
    # engine + MV count; the UI renders storeRef only when present.
    store_ref: str | None
    mv_count: int
    # True when the resolved materialization store is INSTANCE-LOCAL (a local file store, e.g. the
    # embedded DuckDB/SQLite default) rather than a shared store. Behind a load balancer / multiple
    # instances, a local store means each instance keeps its own copy (eventual divergence). Derived
    # from the resolved store DSN, so it reflects the Settings override too, not just the engine.
    instance_local_store: bool


@strawberry.type
class ProtocolHealthType:
    """Liveness of a separate socket listener (gRPC, Arrow Flight, pgwire, bolt).

    ``status`` is one of "running" | "down" | "disabled". "disabled" means the protocol was
    never started (no bound port); "down"/"running" come from a TCP-connect liveness probe.
    """

    name: str
    status: str
    port: int | None


@strawberry.type
class SystemHealthType:
    engine_connected: bool
    engine_worker_count: int
    engine_active_workers: int
    metadata_pool_size: int  # tenant metadata-DB pool; -1 = pool doesn't track size
    metadata_pool_free: int
    metadata_dialect: str  # sqlalchemy dialect of the metadata DB (postgresql, sqlite, …)
    cache_mode: str  # "disabled" | "embedded" | "server"
    cache_connected: bool
    protocols: list[ProtocolHealthType]
    mv_refresh_loop_running: bool


@strawberry.type
class ScheduledTaskType:  # REQ-533, REQ-2054
    """A trigger: when it fires, what it runs, as which role, and how its last run ended."""

    id: str
    name: str
    # REQ-2054: what fires it -- "schedule" (``cron_expression``), "change" or "event" (on
    # ``on_table``, for ``on_event``: change | error | quarantine).
    fires_on: str
    cron_expression: str | None
    on_table: str | None
    on_event: str | None
    webhook_url: str | None
    kind: str  # REQ-1003, REQ-2054: "webhook" | "sql" | "command"
    sql: str | None  # REQ-1003: SQL statement for a SQL trigger
    command: str | None  # REQ-2054: the command a command trigger runs
    role: str | None  # the role a SQL or command trigger runs as
    enabled: bool
    # Its last run (REQ-2054): when, how it ended (succeeded | refused | failed), why when it
    # did not succeed, and what fired it ("change:<table>"; None for a schedule).
    last_run_at: str | None
    last_outcome: str | None
    last_error: str | None
    last_fired_by: str | None
    next_run_at: str | None  # a schedule's next firing; None for one on a table


@strawberry.enum
class GrantKind(enum.Enum):  # REQ-1918
    """An object whose grant list names roles: a role is taken off it one object at a time."""

    METRIC = "metric"
    COMMAND = "command"
    WEBHOOK = "webhook"


@strawberry.type
class RegionChoicesType:  # REQ-1921
    """The regions an object of the org may name, and the one the operator is connected to;
    both empty/null when the platform declares no regions (the admin then shows none)."""

    regions: list[str]
    connected: str | None


@strawberry.type
class MutationResult:  # REQ-533
    success: bool
    message: str
    # Hybrid server i18n (REQ-1350): stable code + params let the UI render
    # a localized message; English `message` remains the fallback.
    code: str | None = None
    params: JsonScalar | None = None


# --- Compile / Submit types ---


@strawberry.type
class ColumnAliasType:
    field_name: str
    column: str


@strawberry.type
class EnforcementType:  # REQ-038, REQ-040, REQ-041, REQ-263
    rls_filters_applied: list[str]
    columns_excluded: list[str]
    schema_scope: str
    masking_applied: list[str]
    ceiling_applied: str | None
    route: str


@strawberry.type
class CompileQueryResult:  # REQ-262, REQ-263, REQ-267
    sql: str
    semantic_sql: str
    engine_sql: str | None
    direct_sql: str | None
    route: str
    route_reason: str
    sources: list[str]
    root_field: str
    canonical_field: str
    column_aliases: list[ColumnAliasType]
    enforcement: EnforcementType
    optimizations: list[str]
    warnings: list[str]
    compiled_cypher: str | None
    cypher_error: str | None


@strawberry.input
class CompileQueryInput:
    query: str
    role: str
    variables: JsonScalar | None = None
    flat_sql: bool = False
    flat_cypher: bool = False
    node_only_cypher: bool = False
