// Copyright (c) 2026 Kenneth Stott
// Canary: 55253d74-4fef-405a-ba19-d151aa4d8f6e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109 (settled): every setting of a table or view is offered where it is created. The
// create step names what is registered (its source, table, columns, or a view's definition);
// the same sections as the editor then hold every setting, on the table as it will be
// registered, and one Save registers it with all of them.

import type { ProfilerRowRule } from "../../api/profiler";
import type {
  Paging,
  PagingKind,
  RegisteredTable,
  TableColumn,
  ViewMetricsSpec,
} from "../../types/admin";
import type { MaskEntry } from "../../lib/masks";

/** What the create step chose: the registration as it would be sent, and what goes with it. */
export interface Registration {
  /** The TableInput the create step built: what names the table, and its columns. */
  input: Record<string, unknown>;
  /** REQ-318: the table's paging as the step declared it, and what pages it. */
  pagination: Paging | null;
  pagingKind: PagingKind | null;
  pagingCeilingRows: number | null;
  /** REQ-1934: a profiler result table's row rules, saved once the table exists. */
  rowRules: ProfilerRowRule[];
  /** REQ-1924: an OpenAPI table with no response schema, whose columns the panel declares. */
  declaresColumns?: boolean;
}

/** One column of a registration input, as the create step builds it. */
interface ColumnInput {
  name: string;
  dataType?: string;
  /** REQ-2171: the subtype of dataType the column registers as. */
  subtype?: string;
  visibleTo?: string[];
  masks?: MaskEntry[];
  alias?: string;
  description?: string;
  nativeFilterType?: string;
  nativeFilterRequired?: boolean;
  requiredBySteward?: boolean;
  isPrimaryKey?: boolean;
  scope?: string;
  path?: string;
  embedding?: boolean;
  embeddingModel?: string;
}

/** The id a table has until it is registered: the server numbers tables from 1. */
export const UNREGISTERED_ID = 0;

const str = (v: unknown): string | null => (typeof v === "string" && v !== "" ? v : null);

export function draftColumn(c: ColumnInput, i: number): TableColumn {
  return {
    // Its position until the server gives it an id: what the form keys its rows on.
    id: i + 1,
    columnName: c.name,
    visibleTo: c.visibleTo ?? [],
    masks: c.masks ?? [],
    alias: c.alias ?? null,
    // The name the server computes from it is not known until it is registered: the form shows
    // the column's own name where it shows the computed one.
    computedSqlAlias: c.alias || c.name,
    computedGqlAlias: c.alias || c.name,
    description: c.description ?? null,
    dataType: c.dataType ?? null,
    subtype: c.subtype ?? null,
    nativeFilterType: c.nativeFilterType ?? null,
    nativeFilterRequired: c.nativeFilterRequired ?? null,
    requiredBySteward: c.requiredBySteward ?? false,
    path: c.path ?? null,
    embedding: c.embedding ?? false,
    embeddingModel: c.embeddingModel ?? null,
    isPrimaryKey: c.isPrimaryKey ?? false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: c.scope ?? "domain",
    isImplicitMeasure: false,
    isImplicitDimension: false,
  };
}

/**
 * The table a registration would make, with every setting the create step did not ask at the
 * value the server gives a new registration (TableInput's defaults; registered through the
 * admin, a table starts as draft -- REQ-1921). Its sections edit it; Save registers it.
 */
export function draftFromRegistration(r: Registration): RegisteredTable {
  const i = r.input;
  const columns = (i.columns as ColumnInput[] | undefined) ?? [];
  return {
    id: UNREGISTERED_ID,
    sourceId: i.sourceId as string,
    domainId: i.domainId as string,
    schemaName: i.schemaName as string,
    tableName: i.tableName as string,
    alias: str(i.alias),
    description: str(i.description),
    cacheTtl: typeof i.cacheTtl === "number" ? i.cacheTtl : null,
    roleTtl: [],
    pagingKind: r.pagingKind,
    pagination: r.pagination,
    pagingCeilingRows: r.pagingCeilingRows,
    replicate: null,
    region: str(i.region),
    draft: true,
    rowMaterialize: false, // REQ-1865: chosen on the Load and timeliness tab
    loadProtected: null,
    offPeakWindow: null,
    offPeakTz: null,
    refreshPolicySummary: null,
    gqlNamingConvention: null,
    graphqlFieldName: null,
    timeTravel: null,
    dqDataset: null,
    qualityCheckers: [],
    watermarkColumn: str(i.watermarkColumn),
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: columns.map(draftColumn),
    writeGrants: [],
    uniqueConstraints: (i.uniqueConstraints as RegisteredTable["uniqueConstraints"]) ?? [],
    apiEndpoint: null,
    apiEndpointError: null,
    viewSql: str(i.viewSql),
    dqContract: str(i.dqContract),
    profilerSourceId: null,
    queryTemplate: str(i.queryTemplate),
    fileGlob: str(i.fileGlob),
    sourceFileColumn: str(i.sourceFileColumn),
    importSet: str(i.importSet),
    discover: i.discover === true,
    materialize: i.materialize === true,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvHoldOnFailingChecks: false,
    businessKey: [],
    validTime: null,
    recordedTime: null,
    mvPersist: "replace",
    mvPrimaryKey: [],
    mvIncremental: false,
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvExpectedEvents: null,
    mvBusinessDayGrain: false,
    productIds: [],
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: [],
    live: null,
    delta: null,
    modelingRole: null,
    viewMetrics: (i.viewMetrics as ViewMetricsSpec | undefined) ?? null,
    implicitMeasures: [],
    implicitDimensions: [],
  };
}

/** What a duplicate of `table` is created from: the create step's registration and the draft. */
export interface TableDuplicate {
  registration: Registration;
  draft: RegisteredTable;
}

/** The name a duplicate is offered under: editable, and refused on save if it exists. A table's
 * alias and a view's name are SQL names, which take no hyphen unquoted: `_copy`. */
export const duplicateName = (name: string) => `${name}_copy`;

/**
 * A table's Duplicate: the create form's table, filled with the table's settings and columns.
 * Not copied: the table's data, its relationships (they are the relationships' own), the
 * products it is in, and who may write to it. A view is named by its own name, so a view's copy
 * takes the new name there; a registered table is the source's table, so its copy is told
 * apart by its SQL alias. A name that exists is refused by the create on save.
 */
export function duplicateOfTable(table: RegisteredTable): TableDuplicate {
  const draft: RegisteredTable = {
    ...(JSON.parse(JSON.stringify(table)) as RegisteredTable),
    id: UNREGISTERED_ID,
    productIds: [],
    writeGrants: [],
    draft: true,
  };
  if (table.viewSql != null || table.viewMetrics != null) {
    draft.tableName = duplicateName(table.tableName);
  } else {
    draft.alias = duplicateName(table.alias || table.tableName);
  }
  const registration: Registration = {
    input: {
      sourceId: draft.sourceId,
      domainId: draft.domainId,
      schemaName: draft.schemaName,
      tableName: draft.tableName,
      alias: draft.alias ?? undefined,
      description: draft.description ?? undefined,
      viewSql: draft.viewSql ?? undefined,
      viewMetrics: draft.viewMetrics ?? undefined,
      ...(draft.region !== null ? { region: draft.region } : {}),
      columns: draft.columns.map((c) => ({ name: c.columnName })),
    },
    pagination: draft.pagination,
    pagingKind: draft.pagingKind,
    pagingCeilingRows: draft.pagingCeilingRows,
    rowRules: [],
  };
  return { registration, draft };
}
