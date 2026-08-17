// Copyright (c) 2026 Kenneth Stott
// Canary: 3f6ff1aa-c2c5-41f0-8215-28042c85bd12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Types matching provisa/api/admin/types.py */

// REQ-824: source-level CDC transport (Debezium/Kafka), entered once per source.
export interface SourceCdcConfig {
  bootstrapServers: string;
  topicPrefix: string;
  schemaRegistryUrl?: string | null;
  consumerGroupId?: string | null; // REQ-931: null/omitted = inherit Provisa-level default
}

export interface Source {
  id: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  dialect: string | null;
  cacheEnabled: boolean;
  cacheTtl: number | null;
  preferMaterialized: boolean;
  loadProtected: boolean; // REQ-1141: scheduled-refresh-only load protection
  offPeakWindow: string | null; // REQ-1141: "HH:MM-HH:MM" maintenance window
  offPeakTz: string; // REQ-1141: IANA zone for the window
  gqlNamingConvention: string | null;
  path: string | null;
  allowedDomains: string[];
  description: string;
  mappingJson?: string | null;
  changeSignal: string; // REQ-929: source default change signal, inherited by its tables
  cdc?: SourceCdcConfig | null;
}

// REQ-1143: server-derived plain-English summary of a table's effective refresh/serving policy.
export interface RefreshPolicySummary {
  text: string;
  serving: "live" | "scheduled" | "cache" | "frozen";
  warning: string | null;
}

export interface Domain {
  id: string;
  description: string;
  graphqlAlias?: string | null;
}

// REQ-1373: one org-level tag registry; appliesTo scopes which object types a tag may attach to.
export type TagObjectType = "source" | "table" | "column" | "relationship" | "command";

export type TagFieldPolicy = "hidden" | "optional" | "required";

// REQ-1467: "none" is every tag that existed before; "required" refuses a bare assignment.
// There is deliberately no "optional" — a bare form would need a reading, and for `entity`
// that reading is a guessed entity type.
export type TagParamPolicy = "none" | "required";

// REQ-1467: one permitted parameter value. The list is closed and maintainer-owned: an open
// one would accept a misspelt entity type, and the type nothing queries then reads as absence.
export interface TagParamValue {
  value: string;
  description: string;
}

export interface Tag {
  id: string;
  description: string;
  appliesTo: TagObjectType[];
  isSystem: boolean;
  // REQ-1443: the tag names what the object already IS, read off its own registration. It is
  // never assigned or unassigned — the picker does not offer it and the server refuses it.
  derived: boolean;
  // Whether the picker shows reason/expiresOn for this tag, and whether they're demanded.
  reasonPolicy: TagFieldPolicy;
  expiresPolicy: TagFieldPolicy;
  // REQ-1467: a parameterized tag is assigned as "{id}:{value}", e.g. "entity:customer".
  paramPolicy: TagParamPolicy;
  paramValues: TagParamValue[];
}

// REQ-1467: the registry id an assigned tag id refers to. Mirrors provisa.core.models.
// split_tag_id — leftmost separator wins, so a value may itself contain a colon.
export const TAG_PARAM_SEPARATOR = ":";

export function baseTagId(tagId: string): string {
  const at = tagId.indexOf(TAG_PARAM_SEPARATOR);
  return at === -1 ? tagId : tagId.slice(0, at);
}

export function tagParam(tagId: string): string | null {
  const at = tagId.indexOf(TAG_PARAM_SEPARATOR);
  return at === -1 ? null : tagId.slice(at + 1);
}

// REQ-1377: one tag on one object; exactly the fields implied by objectType are set.
export interface TagAssignment {
  tagId: string;
  objectType: TagObjectType;
  sourceId?: string | null;
  tableId?: number | null;
  columnName?: string | null;
  relationshipId?: string | null;
  // Tracked function/webhook name (commands are named, not serial-keyed).
  commandName?: string | null;
  tableRef?: string | null;
  // Why this tag is on this object; required for 'deprecated'.
  reason?: string | null;
  // ISO date; for 'deprecated' the planned removal date (typed for reporting).
  expiresOn?: string | null;
}

export function domainGqlAlias(domain: Domain): string {
  if (domain.graphqlAlias) return domain.graphqlAlias.toLowerCase();
  if (!domain.id) return "";
  const parts = domain.id.split(/[^a-zA-Z0-9]+/);
  const acronym = parts
    .filter((p) => p && /[a-zA-Z]/.test(p[0]))
    .map((p) => p[0])
    .join("")
    .toLowerCase();
  return acronym || domain.id[0]?.toLowerCase() || "";
}

export interface TableColumn {
  id: number;
  columnName: string;
  visibleTo: string[];
  writableBy: string[];
  unmaskedTo: string[];
  maskType: string | null;
  maskPattern: string | null;
  maskReplace: string | null;
  maskValue: string | null;
  maskPrecision: string | null;
  alias: string | null;
  computedSqlAlias: string;
  description: string | null;
  dataType: string | null;
  nativeFilterType: string | null;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  isAlternateKey: boolean;
  scope: string;
  // REQ-1360: metadata-only discoverability flags — set when the owning table has
  // enableAggregates/enableGroupBy AND this column is classification-eligible.
  isImplicitMeasure: boolean;
  isImplicitDimension: boolean;
}

// REQ-1360: metadata-only Kimball measure annotation for a table's implicit_measures.
export interface ImplicitMeasure {
  column: string;
  aggFuncs: string[];
}

export interface ColumnPreset {
  column: string;
  source: "now" | "header" | "literal";
  name: string | null;
  value: string | null;
  dataType: string | null;
}

// REQ-1093: a declared UNIQUE constraint (single-column or composite).
export interface UniqueConstraint {
  name: string;
  columns: string[];
}

export interface LiveOutputConfig {
  type: "sse" | "kafka";
  topic: string | null;
  keyColumn: string | null;
  bootstrapServers: string | null;
}

export interface LiveKafkaConfig {
  topic: string;
  format?: string;
  keyColumn?: string | null;
}

export interface LiveDeliveryConfig {
  queryId?: string | null;
  watermarkColumn?: string | null;
  pollInterval: number;
  strategy: "poll" | "native" | "debezium" | "kafka";
  kafka?: LiveKafkaConfig | null;
  outputs: LiveOutputConfig[];
}

export interface RegisteredTable {
  id: number;
  sourceId: string;
  domainId: string;
  schemaName: string;
  tableName: string;
  alias: string | null;
  description: string | null;
  cacheTtl: number | null;
  preferMaterialized: boolean | null;
  loadProtected: boolean | null; // REQ-1141: null = inherit source
  offPeakWindow: string | null; // REQ-1141: "HH:MM-HH:MM" window override
  offPeakTz: string | null; // REQ-1141: window zone override
  refreshPolicySummary: RefreshPolicySummary | null; // REQ-1143: server-derived effective policy
  gqlNamingConvention: string | null;
  watermarkColumn: string | null;
  changeSignal: string | null;
  probeQuery: string | null;
  probeType: string | null;
  columns: TableColumn[];
  columnPresets: ColumnPreset[];
  uniqueConstraints: UniqueConstraint[]; // REQ-1093
  apiEndpoint: string | null;
  viewSql: string | null;
  dqContract: string | null; // REQ-1443: the checker contract this results table lands the scans of
  materialize: boolean;
  mvRefreshInterval: number;
  mvDebounceQuiet: number; // REQ-963: seconds of quiet before firing; 0 = real-time
  mvDebounceMaxDelay: number; // REQ-963: staleness cap under continuous churn
  mvConsistency: string; // REQ-879: "shared" (fleet-coordinated) | "distributed" (per-instance)
  mvPreprocess: string | null; // REQ-1165: inline preflight(rows, ctx) check source; null = no check
  mvBitemporalMode: string | null; // REQ-1162: null | "snapshot" | "delta" (append-only time travel)
  mvBitemporalKey: string[]; // REQ-1162: entity key a version belongs to (required for delta)
  mvPersist: string; // REQ-965: replace | append | upsert
  mvPrimaryKey: string[]; // REQ-970: row identity (required for upsert / incremental)
  mvIncremental: boolean; // REQ-969: incremental maintenance
  mvCalendar: string | null; // REQ-962: snapshot-schedule calendar name (null = not periodic)
  mvGrain: string | null; // REQ-962/1168: nesting grain ("daily".."annual") or nth-weekday ("3WE"/"LFR")
  mvAllowedLateness: number; // REQ-961: seal-deadline slack in seconds
  mvExpectedEvents: string[] | null; // REQ-961: preflight freshness inputs (null = all lineage inputs)
  mvBusinessDayGrain: boolean; // REQ-962: gate snapshot windows to business days
  dataProduct: boolean;
  enableAggregates: boolean;
  enableGroupBy: boolean;
  canDeployToDb: boolean;
  live: LiveDeliveryConfig | null;
  modelingRole?: "fact" | "dimension" | null; // REQ-1322: star-schema role for the Explore browser
  modelingHistory?: unknown; // REQ-1322: server-owned modeling audit trail (shape not consumed by UI)
  viewMetrics?: ViewMetricsSpec | null; // REQ-1318: declarative metric-composed view (null = free-hand SQL)
  implicitMeasures: ImplicitMeasure[]; // REQ-1360: metadata-only, populated only when enableAggregates
  implicitDimensions: string[]; // REQ-1360: metadata-only, populated only when enableGroupBy
}

// REQ-1318: declarative metric-composed view definition. Mutually exclusive with
// viewSql — the server generates (and regenerates on metric change) the view SQL.
export interface ViewMetricsSpec {
  metrics: string[];
  dimensions: string[];
  filters: string[];
}

// REQ-1443: the data-quality contract as the builder panel holds it. The contract TEXT stays the
// source of truth — these rows are a parse of it, returned by the server so the soda / Great
// Expectations dialects have exactly one implementation and the panel has none.
export interface DqCheck {
  columnName: string; // "" for a dataset-level check
  checkType: string;
  definition: string; // the check's own args as authored, not a normalized summary
  // Keys GX writes beside type/kwargs (id, meta, notes), kept verbatim; "" for soda.
  extra: string;
}

export interface DqContract {
  dataset: string | null;
  checker: string;
  checks: DqCheck[];
  error: string | null; // non-null = the text does not parse; the editor keeps what was typed
}

export interface DqContractText {
  text: string;
  error: string | null;
}

// REQ-1443 clause 7: a dry run's per-check outcome. Nothing is landed — this is what proves the
// contract's dataset resolved to the table the operator meant.
export interface DqDryRunCheck {
  columnName: string | null;
  checkType: string;
  outcome: string; // pass | fail | warn | error | skipped
  rowsTested: number | null;
  failedRows: number | null;
  value: number | null;
  diagnostics: string | null; // the per-check-type jsonb block, as JSON text
}

export interface DqDryRun {
  success: boolean;
  message: string;
  checkerVersion: string | null;
  checks: DqDryRunCheck[];
}

// REQ-1443 clause 7: the check catalog the picker renders. The vocabulary is the server's — the
// panel offers what provisa.dq.catalog says a checker can express against a column of that type,
// and knows no check names of its own.
export interface DqCheckParam {
  name: string;
  valueType: string; // string | number | integer | boolean | column | sql
  required: boolean;
  choices: string[]; // empty = free text
}

export interface DqCheckKind {
  checkType: string;
  scope: string; // dataset | column
  params: DqCheckParam[];
  comparators: string[]; // empty = the check takes no threshold
  metrics: string[];
  levels: string[]; // the severities this check can carry, e.g. warn | fail
  thresholdUnits: string[]; // empty = the threshold is a bare number
}

export interface DqCheckColumn {
  name: string;
  dataType: string | null;
  checks: DqCheckKind[]; // already scoped to this column's type
}

export interface DqCheckCatalog {
  datasetChecks: DqCheckKind[];
  columns: DqCheckColumn[];
  error: string | null; // non-null = the dataset resolves nowhere; no columns to scope by
}

export interface DqCheckCatalogVars {
  checker: string;
  dataset: string;
}

// The arguments the picker, threshold and severity editors produce for ONE check. `params` is JSON
// text on purpose: typing it here would put the checker's vocabulary in the browser, which is the
// thing provisa.dq.catalog exists to keep in one place.
export interface DqCheckBuildInput {
  checkType: string;
  columnName: string;
  params: string;
  comparator: string;
  thresholdValue: number | null;
  metric: string;
  unit: string;
  level: string;
}

export interface DqCheckDefinition {
  definition: string;
  error: string | null;
}

export interface DqCheckDefinitionVars {
  checker: string;
  check: DqCheckBuildInput;
}

export interface DqContractParseVars {
  checker: string;
  contractText: string;
}

export interface DqContractBuildVars {
  checker: string;
  dataset: string;
  checks: DqCheck[];
}

export interface DqDryRunVars {
  sourceId: string;
  contractText: string;
}

// The checker source types REQ-1443 ships. Soda is Elastic-Licence (self-host and desktop only);
// Great Expectations is Apache-2.0. Mirrors provisa.dq.contract.CHECKERS.
export const DQ_CHECKERS = ["soda", "great_expectations"] as const;

// The sentinel source id for DERIVED relations — defined by their declaration (view_sql,
// entity/fact lowering), not scanned from an external system. Mirrors the backend
// provisa.core.models.DERIVED_SOURCE_ID; provenance is the definition's lineage.
export const DERIVED_SOURCE_ID = "__derived__";

// REQ-1317: a registered semantic metric. `fromFact` names the source fact when the
// metric was auto-derived from a fact registration (REQ-1320); null for hand-authored.
export interface Metric {
  name: string;
  expression: string;
  datatype: string | null;
  description: string | null;
  aiContext: string | null;
  visibleTo: string[];
  fromFact: string | null;
}

export interface Relationship {
  id: number;
  sourceTableId: number;
  targetTableId: number | null;
  sourceTableName: string;
  sourceDomainId: string;
  targetTableName: string;
  sourceColumn: string;
  targetColumn: string | null;
  cardinality: string;
  materialize: boolean;
  refreshInterval: number;
  targetFunctionName: string | null;
  functionArg: string | null;
  alias: string | null;
  graphqlAlias: string | null;
  // REQ-1417: the relationship as the SQL plane spells it — what ?include= takes. Server-derived
  // by the naming authority; the client must never transliterate graphqlAlias itself.
  physicalName: string | null;
  computedCypherAlias: string | null;
  autoSuggested: boolean;
  disableCypher: boolean;
  ownerDomainId: string | null;
}

export interface RLSRule {
  id: number;
  tableId: number | null;
  domainId: string | null;
  roleId: string;
  filterExpr: string;
}

// REQ-1484: an artifact that references a column the administrator is about to rename or drop.
// breaksOn is "rename" for anything authored against the column's exposed SQL name (views, MVs,
// metric expressions, RLS predicates, DQ contracts) and "remove" for stores of the physical
// column_name (relationships, glossary bindings, tags), which survive a rename.
export interface ColumnDependent {
  kind: string;
  name: string;
  detail: string;
  breaksOn: string;
}

export interface ColumnDependentsResult {
  columnName: string;
  dependents: ColumnDependent[];
}

export interface MutationResult {
  success: boolean;
  message: string;
  // REQ-1350: stable server error/status code + interpolation params for client-side i18n.
  code?: string | null;
  params?: Record<string, unknown> | null;
}
