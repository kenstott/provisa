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

import strawberry

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


@strawberry.type
class SourceCdcConfigType:  # REQ-824
    bootstrap_servers: str
    topic_prefix: str
    schema_registry_url: str | None = None
    consumer_group_id: str | None = (
        None  # REQ-931: None = inherit Provisa-level cdc_consumer_group_id
    )


@strawberry.type
class SourceType:  # REQ-012
    id: str
    type: str
    host: str
    port: int
    database: str
    username: str
    dialect: str
    cache_enabled: bool
    cache_ttl: int | None
    prefer_materialized: bool
    load_protected: bool = False  # REQ-1141: scheduled-refresh-only load protection
    off_peak_window: str | None = None  # REQ-1141: "HH:MM-HH:MM" maintenance window
    off_peak_tz: str = "UTC"  # REQ-1141: IANA zone for the window
    gql_naming_convention: str | None = None
    path: str | None = None
    allowed_domains: list[str] = strawberry.field(default_factory=list)
    description: str = ""
    mapping_json: str = "{}"
    change_signal: str = "ttl"  # REQ-929: source default change signal (inherited by its tables)
    cdc: SourceCdcConfigType | None = None  # REQ-824: source-level CDC transport


@strawberry.type
class DomainType:  # REQ-533, REQ-609
    id: str
    description: str
    steward: str | None = None  # REQ-609: None = pending, no designated steward
    graphql_alias: str | None = None


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


@strawberry.type
class ColumnPresetType:
    column: str
    source: str
    name: str | None
    value: str | None
    data_type: str | None = None


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


@strawberry.type
class LiveKafkaParamsType:  # REQ-813
    topic: str
    format: str = "json"
    key_column: str | None = None


@strawberry.type
class LiveDeliveryConfigType:  # REQ-565, REQ-813
    strategy: str = "poll"  # poll | native | debezium | kafka
    watermark_column: str | None = None
    poll_interval: int = 10
    kafka: LiveKafkaParamsType | None = None
    query_id: str | None = None
    outputs: list[LiveOutputConfigType] = strawberry.field(default_factory=list)


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
class RegisteredTableType:  # REQ-013, REQ-014, REQ-016, REQ-135
    id: int
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    alias: str | None
    description: str | None
    cache_ttl: int | None
    prefer_materialized: bool | None
    load_protected: bool | None  # REQ-1141: NULL = inherit source
    off_peak_window: str | None  # REQ-1141: per-table window override
    off_peak_tz: str | None  # REQ-1141: per-table window zone override
    gql_naming_convention: str | None
    watermark_column: str | None
    columns: list[TableColumnType]
    column_presets: list[ColumnPresetType] = strawberry.field(default_factory=list)
    unique_constraints: list[UniqueConstraintType] = strawberry.field(
        default_factory=list
    )  # REQ-1093
    api_endpoint: str | None = None
    view_sql: str | None = None
    view_metrics: ViewMetricsType | None = None  # REQ-1318: metric-composed view spec
    # REQ-1443: the data-quality contract this table's rows are the scan results of, verbatim.
    dq_contract: str | None = None
    change_signal: str | None = None  # REQ-929: override source change signal; None = inherit
    probe_query: str | None = None  # REQ-929: source-native freshness probe
    probe_type: str | None = None  # REQ-982: input-probe method; None = resolve per source class
    materialize: bool = False
    mv_refresh_interval: int = 300
    mv_debounce_quiet: float = 0.0  # REQ-963: seconds of quiet before firing; 0 = real-time
    mv_debounce_max_delay: float = 5.0  # REQ-963: staleness cap under continuous churn
    mv_consistency: str = (
        "shared"  # REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    )
    mv_preprocess: str | None = None  # REQ-957: inline preprocess(rows, ctx) hook source
    mv_bitemporal_mode: str | None = None  # REQ-1162: None | "snapshot" | "delta"
    mv_bitemporal_key: list[str] = strawberry.field(default_factory=list)  # REQ-1162: entity key
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
    data_product: bool = False
    enable_aggregates: bool = False
    enable_group_by: bool = False
    can_deploy_to_db: bool = False
    live: LiveDeliveryConfigType | None = None
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


@strawberry.type
class TableColumnType:  # REQ-040, REQ-041, REQ-393, REQ-399
    id: int
    column_name: str
    visible_to: list[str]
    writable_by: list[str]
    unmasked_to: list[str]
    mask_type: str | None
    mask_pattern: str | None
    mask_replace: str | None
    mask_value: str | None
    mask_precision: str | None
    alias: str | None
    computed_sql_alias: str
    description: str | None
    data_type: str | None = None
    native_filter_type: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_alternate_key: bool = False
    scope: str = "domain"
    # REQ-1360: metadata-only discoverability flags — set when the owning table has
    # enable_aggregates/enable_group_by AND this column is eligible per the same
    # classification build_agg_fields_type (REQ-196) already applies.
    is_implicit_measure: bool = False
    is_implicit_dimension: bool = False


@strawberry.type
class AvailableTableType:  # REQ-533
    name: str
    comment: str | None


@strawberry.type
class AvailableColumnType:  # REQ-533
    name: str
    data_type: str
    comment: str | None
    native_filter_type: str | None = None
    is_primary_key: bool = False


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
    max_query_depth: int | None = None
    max_query_nodes: int | None = None
    max_query_time_ms: int | None = None


@strawberry.type
class RoleType:  # REQ-042
    id: str
    capabilities: list[str]
    domain_access: list[str]
    rate_limit: RoleRateLimitType | None = None  # REQ-1174


@strawberry.type
class RLSRuleType:  # REQ-041, REQ-402
    id: int
    table_id: int | None
    domain_id: str | None
    role_id: str
    filter_expr: str


# --- Input types for mutations ---


@strawberry.input
class SourceCdcConfigInput:  # REQ-824
    bootstrap_servers: str
    topic_prefix: str
    schema_registry_url: str | None = None
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
    cdc: SourceCdcConfigInput | None = None  # REQ-824: source-level CDC transport


@strawberry.input
class DomainInput:  # REQ-533, REQ-609
    id: str
    description: str = ""
    steward: str | None = None  # REQ-609
    graphql_alias: str | None = None


@strawberry.input
class TagInput:  # REQ-1373
    id: str
    description: str = ""
    applies_to: list[str] = strawberry.field(default_factory=list)
    reason_policy: str = "optional"  # hidden | optional | required
    expires_policy: str = "optional"


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


@strawberry.input
class ColumnInput:  # REQ-040, REQ-041, REQ-393, REQ-399
    name: str
    visible_to: list[str]
    writable_by: list[str] = strawberry.field(default_factory=list)
    unmasked_to: list[str] = strawberry.field(default_factory=list)
    mask_type: str | None = None
    mask_pattern: str | None = None
    mask_replace: str | None = None
    mask_value: str | None = None
    mask_precision: str | None = None
    alias: str | None = None
    description: str | None = None
    # Canonical IR data-type (REQ-846) a steward assigned during schema discovery. Authoritative for
    # a manually-defined column (a non-SQL source the engine can't introspect); the landing write
    # face maps IR → the store's physical type. Null when the type is filled by introspection.
    data_type: str | None = None
    native_filter_type: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_alternate_key: bool = False
    scope: str = "domain"


@strawberry.input
class ColumnPresetInput:  # REQ-533
    column: str
    source: str
    name: str | None = None
    value: str | None = None
    data_type: str | None = None


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


@strawberry.input
class LiveKafkaParamsInput:  # REQ-813
    topic: str
    format: str = "json"
    key_column: str | None = None


@strawberry.input
class LiveDeliveryConfigInput:  # REQ-565, REQ-813
    strategy: str = "poll"  # poll | native | debezium | kafka
    watermark_column: str | None = None
    poll_interval: int = 10
    kafka: LiveKafkaParamsInput | None = None
    query_id: str | None = None
    outputs: list[LiveOutputConfigInput] = strawberry.field(default_factory=list)


@strawberry.input
class TableInput:  # REQ-013, REQ-016, REQ-133, REQ-135, REQ-252
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    columns: list[ColumnInput]
    alias: str | None = None
    description: str | None = None
    watermark_column: str | None = None
    column_presets: list[ColumnPresetInput] = strawberry.field(default_factory=list)
    unique_constraints: list[UniqueConstraintInput] = strawberry.field(
        default_factory=list
    )  # REQ-1093
    view_sql: str | None = None
    # REQ-1443: the data-quality contract (soda contract / GX suite) this table's rows are the scan
    # results of. The contract names what it scans, so the observed target is DERIVED from it
    # (REQ-939) and the results columns are replaced by the shipped schema at load.
    dq_contract: str | None = None
    # REQ-1318: declarative metric-composed view definition; mutually exclusive with view_sql.
    # The server generates (and regenerates on metric change) the view SELECT from this spec.
    view_metrics: ViewMetricsInput | None = None
    load_protected: bool | None = None  # REQ-1141: NULL = inherit source load protection
    off_peak_window: str | None = None  # REQ-1141: per-table "HH:MM-HH:MM" window override
    off_peak_tz: str | None = None  # REQ-1141: per-table window zone override
    change_signal: str | None = None  # REQ-929: override source change signal; None = inherit
    probe_query: str | None = None  # REQ-929: source-native freshness probe
    probe_type: str | None = None  # REQ-982: input-probe method; None = resolve per source class
    materialize: bool = False
    mv_refresh_interval: int = 300
    mv_debounce_quiet: float = 0.0  # REQ-963: seconds of quiet before firing; 0 = real-time
    mv_debounce_max_delay: float = 5.0  # REQ-963: staleness cap under continuous churn
    mv_consistency: str = (
        "shared"  # REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    )
    mv_preprocess: str | None = None  # REQ-957: inline preprocess(rows, ctx) hook source
    mv_bitemporal_mode: str | None = None  # REQ-1162: None | "snapshot" | "delta"
    mv_bitemporal_key: list[str] = strawberry.field(default_factory=list)  # REQ-1162: entity key
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
    data_product: bool = False
    enable_aggregates: bool = False
    enable_group_by: bool = False
    discover: bool = False  # REQ-252: infer columns from the live NoSQL source at registration
    live: LiveDeliveryConfigInput | None = None  # REQ-565: live delivery config


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
    record_candidate: bool = False  # when True, also insert accepted relationship_candidates record


@strawberry.input
class EntityInput:  # REQ-1164: dimension / hub+satellite sugar → lowers to a (bitemporal) MV
    name: str
    source: str  # source relation the projection reads from (e.g. "raw.customers")
    domain_id: str
    key: list[str]
    attributes: list[str] = strawberry.field(default_factory=list)
    history: str = "none"  # none | scd2 | snapshot
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
    visible_to: list[str] = strawberry.field(default_factory=lambda: ["*"])
    from_fact: str | None = None  # REQ-1320: set when auto-registered from a fact measure


@strawberry.input
class MetricInput:  # REQ-1317
    name: str
    expression: str  # must parse under sqlglot and contain ≥1 aggregate function
    datatype: str | None = None
    description: str | None = None
    ai_context: str | None = None  # REQ-1319
    visible_to: list[str] = strawberry.field(default_factory=lambda: ["*"])


@strawberry.input
class RoleRateLimitInput:  # REQ-1174
    requests_per_second: int | None = None
    max_query_depth: int | None = None
    max_query_nodes: int | None = None
    max_query_time_ms: int | None = None


@strawberry.input
class RoleInput:  # REQ-042
    id: str
    capabilities: list[str]
    domain_access: list[str]
    rate_limit: RoleRateLimitInput | None = None  # REQ-1174


@strawberry.input
class RLSRuleInput:  # REQ-041, REQ-402
    table_id: str | None = None  # table name (resolved to ID); mutually exclusive with domain_id
    domain_id: str | None = None  # domain ID for domain-level rules
    role_id: str = ""
    filter_expr: str = ""


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
class HotTableStatType:
    table_name: str
    catalog: str
    schema_name: str
    row_count: int
    is_api: bool
    loaded: bool


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
class ScheduledTaskType:  # REQ-533
    id: str
    name: str
    cron_expression: str
    webhook_url: str | None
    kind: str  # REQ-1003: "webhook" | "sql"
    sql: str | None  # REQ-1003: SQL statement for a SQL trigger
    enabled: bool
    last_run_at: str | None
    next_run_at: str | None


@strawberry.type
class MutationResult:  # REQ-533
    success: bool
    message: str
    # Hybrid server i18n (REQ-1350): stable code + params let the UI render
    # a localized message; English `message` remains the fallback.
    code: str | None = None
    params: strawberry.scalars.JSON | None = None


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
    variables: strawberry.scalars.JSON | None = None
    flat_sql: bool = False
    flat_cypher: bool = False
    node_only_cypher: bool = False
