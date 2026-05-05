// Copyright (c) 2026 Kenneth Stott
// Canary: e5395602-9100-410a-a4d5-a267fc787a3e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Role, RoleAssignment, OrgMembership } from "../types/auth";
import type {
  Source,
  Domain,
  RegisteredTable,
  Relationship,
  RLSRule,
  MutationResult,
} from "../types/admin";

const API_BASE = import.meta.env.VITE_API_BASE || "";

async function gql<T>(query: string, variables?: Record<string, unknown>): Promise<T> {
  const resp = await fetch(`${API_BASE}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`API error ${resp.status}: ${text.slice(0, 200)}`);
  }
  const json = await resp.json();
  if (json.errors) throw new Error(json.errors[0].message);
  return json.data;
}

export async function fetchMe(): Promise<{
  user_id: string;
  email: string | null;
  display_name: string | null;
  dev_mode: boolean;
  active_org_id: string | null;
  org_memberships: OrgMembership[];
  assignments: RoleAssignment[];
}> {
  const res = await fetch("/auth/me");
  if (!res.ok) throw new Error("auth/me failed");
  return res.json();
}

export async function fetchProviderType(): Promise<string | null> {
  const res = await fetch("/auth/provider-type");
  if (!res.ok) return null;
  const data = await res.json();
  return data.provider ?? null;
}

export async function registerAccount(body: {
  username: string;
  password: string;
  email?: string;
  display_name?: string;
  invite_token?: string;
}): Promise<{ user_id: string; username: string }> {
  const res = await fetch("/auth/register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || `Registration failed: ${res.status}`);
  }
  return res.json();
}

export interface Org {
  id: string;
  name: string;
  created_by: string | null;
  created_at: string;
}

export async function fetchOrgs(): Promise<Org[]> {
  const res = await fetch(`${API_BASE}/admin/orgs`);
  if (!res.ok) throw new Error(`fetchOrgs failed: ${res.status}`);
  return res.json();
}

export async function createOrg(id: string, name: string): Promise<Org> {
  const res = await fetch(`${API_BASE}/admin/orgs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ id, name }),
  });
  if (!res.ok) throw new Error(`createOrg failed: ${res.status}`);
  return res.json();
}

export async function deleteOrg(orgId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`deleteOrg failed: ${res.status}`);
}

export interface OrgMember {
  user_id: string;
  email: string | null;
  display_name: string | null;
  provider: string | null;
}

export async function fetchOrgMembers(orgId: string): Promise<OrgMember[]> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members`);
  if (!res.ok) throw new Error(`fetchOrgMembers failed: ${res.status}`);
  return res.json();
}

export async function addOrgMember(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId }),
  });
  if (!res.ok) throw new Error(`addOrgMember failed: ${res.status}`);
}

export async function removeOrgMember(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members/${userId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`removeOrgMember failed: ${res.status}`);
}

export async function fetchOrgRoles(orgId: string): Promise<Role[]> {
  const res = await fetch(`${API_BASE}/admin/roles`, {
    headers: { "X-Org-Id": orgId },
  });
  if (!res.ok) throw new Error(`fetchOrgRoles failed: ${res.status}`);
  const rows: Array<{ id: string; capabilities: string[]; domain_access: string[] }> = await res.json();
  return rows.map((r) => ({
    id: r.id,
    capabilities: r.capabilities as import("../types/auth").Capability[],
    domain_access: r.domain_access,
  }));
}

export async function createOrgRole(
  orgId: string,
  id: string,
  capabilities: string[],
  domain_access: string[],
): Promise<Role> {
  const res = await fetch(`${API_BASE}/admin/roles`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Org-Id": orgId },
    body: JSON.stringify({ id, capabilities, domain_access }),
  });
  if (!res.ok) throw new Error(`createOrgRole failed: ${res.status}`);
  return res.json();
}

export async function deleteOrgRole(orgId: string, roleId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/roles/${roleId}`, {
    method: "DELETE",
    headers: { "X-Org-Id": orgId },
  });
  if (!res.ok) throw new Error(`deleteOrgRole failed: ${res.status}`);
}

export async function fetchRoles(): Promise<Role[]> {
  const data = await gql<{ roles: Role[] }>(`{ roles { id capabilities domainAccess } }`);
  return data.roles.map((r) => ({
    ...r,
    domain_access: (r as any).domainAccess ?? r.domain_access,
  }));
}

export async function fetchSources(): Promise<Source[]> {
  const data = await gql<{ sources: Source[] }>(
    `{ sources { id type host port database username dialect cacheEnabled cacheTtl namingConvention allowedDomains description } }`
  );
  return data.sources;
}

export async function fetchDomains(): Promise<Domain[]> {
  const data = await gql<{ domains: Domain[] }>(`{ domains { id description graphqlAlias } }`);
  return data.domains;
}

export async function createDomain(id: string, description: string, graphqlAlias?: string | null): Promise<void> {
  const aliasArg = graphqlAlias ? `, graphqlAlias: ${JSON.stringify(graphqlAlias)}` : "";
  await gql(`mutation { createDomain(input: { id: ${JSON.stringify(id)}, description: ${JSON.stringify(description)}${aliasArg} }) { success message } }`);
}

export async function deleteDomain(id: string): Promise<void> {
  await gql(`mutation { deleteDomain(id: ${JSON.stringify(id)}) { success message } }`);
}


export async function fetchTables(): Promise<RegisteredTable[]> {
  const data = await gql<{ tables: RegisteredTable[] }>(
    `{ tables { id sourceId domainId schemaName tableName governance alias description cacheTtl namingConvention watermarkColumn apiEndpoint viewSql dataProduct columns { id columnName visibleTo writableBy unmaskedTo maskType maskPattern maskReplace maskValue maskPrecision alias description nativeFilterType isPrimaryKey isForeignKey isAlternateKey scope } columnPresets { column source name value dataType } } }`
  );
  return data.tables;
}

export async function fetchRelationships(): Promise<Relationship[]> {
  const data = await gql<{ relationships: Relationship[] }>(
    `{ relationships { id sourceTableId targetTableId sourceTableName targetTableName sourceColumn targetColumn cardinality materialize refreshInterval targetFunctionName functionArg alias graphqlAlias computedCypherAlias autoSuggested disableCypher } }`
  );
  return data.relationships;
}

export async function upsertRelationship(input: {
  id: string;
  sourceTableId: string;
  targetTableId?: string;
  sourceColumn: string;
  targetColumn?: string;
  cardinality: string;
  materialize: boolean;
  refreshInterval: number;
  targetFunctionName?: string | null;
  functionArg?: string | null;
  alias?: string | null;
  graphqlAlias?: string | null;
  disableCypher?: boolean;
  recordCandidate?: boolean;
}): Promise<MutationResult> {
  const data = await gql<{ upsertRelationship: MutationResult }>(
    `mutation($input: RelationshipInput!) { upsertRelationship(input: $input) { success message } }`,
    { input }
  );
  return data.upsertRelationship;
}

export async function deleteRelationship(id: string): Promise<MutationResult> {
  const data = await gql<{ deleteRelationship: MutationResult }>(
    `mutation($id: String!) { deleteRelationship(id: $id) { success message } }`,
    { id }
  );
  return data.deleteRelationship;
}

export async function fetchRlsRules(): Promise<RLSRule[]> {
  const data = await gql<{ rlsRules: RLSRule[] }>(
    `{ rlsRules { id tableId domainId roleId filterExpr } }`
  );
  return data.rlsRules;
}

export async function upsertRlsRule(input: {
  tableId?: string | null;
  domainId?: string | null;
  roleId: string;
  filterExpr: string;
}): Promise<MutationResult> {
  const data = await gql<{ upsertRlsRule: MutationResult }>(
    `mutation($input: RLSRuleInput!) { upsertRlsRule(input: $input) { success message } }`,
    { input }
  );
  return data.upsertRlsRule;
}

export async function deleteRlsRule(
  roleId: string,
  tableId?: number | null,
  domainId?: string | null,
): Promise<MutationResult> {
  const data = await gql<{ deleteRlsRule: MutationResult }>(
    `mutation($roleId: String!, $tableId: Int, $domainId: String) { deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) { success message } }`,
    { roleId, tableId: tableId ?? null, domainId: domainId ?? null }
  );
  return data.deleteRlsRule;
}

export async function upsertRole(input: {
  id: string;
  capabilities: string[];
  domainAccess: string[];
}): Promise<MutationResult> {
  const data = await gql<{ createRole: MutationResult }>(
    `mutation($input: RoleInput!) { createRole(input: $input) { success message } }`,
    { input }
  );
  return data.createRole;
}

export async function deleteRole(id: string): Promise<MutationResult> {
  const data = await gql<{ deleteRole: MutationResult }>(
    `mutation($id: String!) { deleteRole(id: $id) { success message } }`,
    { id }
  );
  return data.deleteRole;
}

export async function createSource(input: {
  id: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  path?: string | null;
  description?: string;
}): Promise<MutationResult> {
  const data = await gql<{ createSource: MutationResult }>(
    `mutation($input: SourceInput!) { createSource(input: $input) { success message } }`,
    { input }
  );
  return data.createSource;
}

export async function registerTable(input: {
  sourceId: string;
  domainId: string;
  schemaName: string;
  tableName: string;
  governance: string;
  alias?: string;
  description?: string;
  watermarkColumn?: string | null;
  viewSql?: string;
  dataProduct?: boolean;
  columns: { name: string; visibleTo: string[]; writableBy?: string[]; unmaskedTo?: string[]; maskType?: string; maskPattern?: string; maskReplace?: string; maskValue?: string; maskPrecision?: string; alias?: string; description?: string; nativeFilterType?: string | null; isPrimaryKey?: boolean; isForeignKey?: boolean; isAlternateKey?: boolean; scope?: string }[];
  columnPresets?: { column: string; source: string; name?: string | null; value?: string | null; dataType?: string | null }[];
}): Promise<MutationResult> {
  const data = await gql<{ registerTable: MutationResult }>(
    `mutation($input: TableInput!) { registerTable(input: $input) { success message } }`,
    { input }
  );
  return data.registerTable;
}

export async function updateTable(input: {
  sourceId: string;
  domainId: string;
  schemaName: string;
  tableName: string;
  governance: string;
  alias?: string;
  description?: string;
  watermarkColumn?: string | null;
  columns: { name: string; visibleTo: string[]; writableBy?: string[]; unmaskedTo?: string[]; maskType?: string; maskPattern?: string; maskReplace?: string; maskValue?: string; maskPrecision?: string; alias?: string; description?: string; nativeFilterType?: string | null; isPrimaryKey?: boolean; isForeignKey?: boolean; isAlternateKey?: boolean; scope?: string }[];
  columnPresets?: { column: string; source: string; name?: string | null; value?: string | null; dataType?: string | null }[];
  dataProduct?: boolean;
}): Promise<MutationResult> {
  const data = await gql<{ updateTable: MutationResult }>(
    `mutation($input: TableInput!) { updateTable(input: $input) { success message } }`,
    { input }
  );
  return data.updateTable;
}

export async function deleteTable(id: number): Promise<MutationResult> {
  const data = await gql<{ deleteTable: MutationResult }>(
    `mutation($id: Int!) { deleteTable(id: $id) { success message } }`,
    { id }
  );
  return data.deleteTable;
}

export async function profileTable(tableId: number): Promise<{ columns: string[]; rows: Record<string, unknown>[]; rowCount: number }> {
  const resp = await fetch(`${API_BASE}/admin/tables/${tableId}/profile`, { method: "POST" });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}

export async function generateColumnDescription(tableId: number, columnName: string): Promise<string> {
  const data = await gql<{ generateColumnDescription: string }>(
    `query($tableId: String!, $columnName: String!) { generateColumnDescription(tableId: $tableId, columnName: $columnName) }`,
    { tableId: String(tableId), columnName }
  );
  return data.generateColumnDescription;
}

export async function generateTableDescription(tableId: number): Promise<string> {
  const data = await gql<{ generateTableDescription: string }>(
    `query($tableId: String!) { generateTableDescription(tableId: $tableId) }`,
    { tableId: String(tableId) }
  );
  return data.generateTableDescription;
}

export async function fetchAvailableSchemas(sourceId: string): Promise<string[]> {
  const data = await gql<{ availableSchemas: string[] }>(
    `query($sourceId: String!) { availableSchemas(sourceId: $sourceId) }`,
    { sourceId }
  );
  return data.availableSchemas;
}

export interface TableMetadata {
  name: string;
  comment: string | null;
}

export async function fetchAvailableTables(sourceId: string, schemaName: string = "public"): Promise<TableMetadata[]> {
  const data = await gql<{ availableTables: TableMetadata[] }>(
    `query($sourceId: String!, $schemaName: String!) {
      availableTables(sourceId: $sourceId, schemaName: $schemaName) { name comment }
    }`,
    { sourceId, schemaName }
  );
  return data.availableTables;
}

export async function fetchAvailableColumns(sourceId: string, schemaName: string, tableName: string): Promise<string[]> {
  const data = await gql<{ availableColumns: string[] }>(
    `query($sourceId: String!, $schemaName: String!, $tableName: String!) { availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) }`,
    { sourceId, schemaName, tableName }
  );
  return data.availableColumns;
}

export interface ColumnMetadata {
  name: string;
  dataType: string;
  comment: string | null;
  nativeFilterType: string | null;
  isPrimaryKey: boolean;
}

export async function fetchAvailableColumnsMetadata(
  sourceId: string,
  schemaName: string,
  tableName: string,
): Promise<ColumnMetadata[]> {
  const data = await gql<{ availableColumnsMetadata: ColumnMetadata[] }>(
    `query($sourceId: String!, $schemaName: String!, $tableName: String!) {
      availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) {
        name dataType comment nativeFilterType isPrimaryKey
      }
    }`,
    { sourceId, schemaName, tableName }
  );
  return data.availableColumnsMetadata;
}

export async function fetchAvailableFunctions(sourceId: string, schemaName = "openapi"): Promise<TableMetadata[]> {
  const data = await gql<{ availableFunctions: TableMetadata[] }>(
    `query($sourceId: String!, $schemaName: String!) {
      availableFunctions(sourceId: $sourceId, schemaName: $schemaName) { name comment }
    }`,
    { sourceId, schemaName }
  );
  return data.availableFunctions;
}

export async function updateSource(input: {
  id: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  path?: string | null;
  description?: string;
}): Promise<MutationResult> {
  const data = await gql<{ updateSource: MutationResult }>(
    `mutation($input: SourceInput!) { updateSource(input: $input) { success message } }`,
    { input }
  );
  return data.updateSource;
}

export async function renameSource(oldId: string, newId: string): Promise<MutationResult> {
  const data = await gql<{ renameSource: MutationResult }>(
    `mutation($oldId: String!, $newId: String!) { renameSource(oldId: $oldId, newId: $newId) { success message } }`,
    { oldId, newId }
  );
  return data.renameSource;
}

export async function deleteSource(id: string): Promise<MutationResult> {
  const data = await gql<{ deleteSource: MutationResult }>(
    `mutation($id: String!) { deleteSource(id: $id) { success message } }`,
    { id }
  );
  return data.deleteSource;
}

export async function fetchSdl(roleId: string): Promise<string> {
  const resp = await fetch(`${API_BASE}/data/sdl`, {
    headers: { "X-Role": roleId },
  });
  if (!resp.ok) throw new Error(`SDL fetch failed: ${resp.status}`);
  return resp.text();
}

// --- Discovery ---

const API_BASE_RAW = import.meta.env.VITE_API_BASE || "";

export async function discoverRelationships(
  scope: string,
  tableId?: number,
  domainId?: string,
): Promise<{ candidates_found: number; stored_ids: number[] }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/relationships`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      scope,
      table_id: tableId,
      domain_id: domainId,
    }),
  });
  if (!resp.ok) throw new Error(`Discovery failed: ${resp.status}`);
  return resp.json();
}

export async function fetchCandidates(): Promise<any[]> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates`);
  if (!resp.ok) throw new Error(`Fetch candidates failed: ${resp.status}`);
  return resp.json();
}

export async function acceptCandidate(id: number, name?: string): Promise<any> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/accept`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: name ?? null }),
  });
  if (!resp.ok) throw new Error(`Accept failed: ${resp.status}`);
  return resp.json();
}

export async function fetchRejectedCount(): Promise<number> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/rejected/count`);
  if (!resp.ok) throw new Error(`Fetch rejected count failed: ${resp.status}`);
  const data = await resp.json();
  return data.count;
}

export async function clearRejectedCandidates(): Promise<{ deleted: number }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/rejected`, {
    method: "DELETE",
  });
  if (!resp.ok) throw new Error(`Clear rejections failed: ${resp.status}`);
  return resp.json();
}

export async function rejectCandidate(id: number, reason: string): Promise<void> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/reject`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason }),
  });
  if (!resp.ok) throw new Error(`Reject failed: ${resp.status}`);
}

// --- Schema Discovery ---

export interface DiscoveredColumn {
  name: string;
  type: string;
  nullable: boolean;
  description: string;
  source_path: string;
}

export interface DiscoverSchemaResponse {
  source_id: string;
  source_type: string;
  columns: DiscoveredColumn[];
}

export async function discoverSourceSchema(
  sourceId: string,
  hints?: {
    collection?: string;
    index?: string;
    keyspace?: string;
    table?: string;
    metric?: string;
    sample_limit?: number;
  },
): Promise<DiscoverSchemaResponse> {
  const resp = await fetch(`${API_BASE_RAW}/admin/schema-discovery/discover/${sourceId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(hints ?? {}),
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}

// --- Config ---

export async function downloadConfig(): Promise<string> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config`);
  if (!resp.ok) throw new Error(`Config download failed: ${resp.status}`);
  return resp.text();
}

export async function uploadConfig(yaml: string): Promise<{ success: boolean; message: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/x-yaml" },
    body: yaml,
  });
  if (!resp.ok) throw new Error(`Config upload failed: ${resp.status}`);
  return resp.json();
}

// --- Platform Settings ---

export interface PlatformSettings {
  redirect: {
    enabled: boolean;
    threshold: number;
    default_format: string;
    ttl: number;
  };
  sampling: {
    default_sample_size: number;
  };
  cache: {
    default_ttl: number;
  };
  naming: {
    domain_prefix: boolean;
    convention: string;
  };
  otel: {
    endpoint: string;
    service_name: string;
    sample_rate: number;
  };
}

export async function fetchSettings(): Promise<PlatformSettings> {
  const resp = await fetch(`${API_BASE_RAW}/admin/settings`);
  if (!resp.ok) throw new Error(`Settings fetch failed: ${resp.status}`);
  return resp.json();
}

export async function updateSettings(
  settings: Partial<PlatformSettings>,
): Promise<{ success: boolean; updated: string[] }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/settings`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(settings),
  });
  if (!resp.ok) throw new Error(`Settings update failed: ${resp.status}`);
  return resp.json();
}

// --- Views ---

// --- Query compilation and submission ---

export interface CompileResult {
  sql: string;
  semantic_sql: string;
  trino_sql: string | null;
  direct_sql: string | null;
  params: unknown[];
  route: string;
  route_reason: string;
  sources: string[];
  root_field: string;
  canonical_field: string;
  column_aliases: { field_name: string; column: string }[];
  optimizations?: string[];
  warnings?: string[];
  compiled_cypher?: string | null;
  cypher_error?: string | null;
}

export async function compileQuery(
  roleId: string,
  query: string,
  variables?: Record<string, unknown>,
): Promise<CompileResult | { queries: CompileResult[] }> {
  const data = await gql<{ compileQuery: CompileResult[] }>(
    `mutation CompileQuery($input: CompileQueryInput!) {
      compileQuery(input: $input) {
        sql semanticSql trinoSql directSql route routeReason sources
        rootField canonicalField compiledCypher cypherError optimizations warnings
        columnAliases { fieldName column }
        enforcement { rlsFiltersApplied columnsExcluded schemaScope maskingApplied ceilingApplied route }
      }
    }`,
    { input: { query, role: roleId, variables: variables ?? null } },
  );
  const results = data.compileQuery.map((r: any) => ({
    ...r,
    semantic_sql: r.semanticSql ?? r.semantic_sql,
    trino_sql: r.trinoSql ?? r.trino_sql,
    direct_sql: r.directSql ?? r.direct_sql,
    route_reason: r.routeReason ?? r.route_reason,
    root_field: r.rootField ?? r.root_field,
    canonical_field: r.canonicalField ?? r.canonical_field,
    compiled_cypher: r.compiledCypher ?? r.compiled_cypher,
    cypher_error: r.cypherError ?? r.cypher_error,
    column_aliases: (r.columnAliases ?? r.column_aliases ?? []).map((ca: any) => ({
      field_name: ca.fieldName ?? ca.field_name,
      column: ca.column,
    })),
  })) as CompileResult[];
  if (results.length === 1) return results[0];
  return { queries: results };
}

export interface SubmitMetadata {
  business_purpose?: string;
  use_cases?: string;
  data_sensitivity?: string;
  refresh_frequency?: string;
  expected_row_count?: string;
  owner_team?: string;
  sink?: { topic: string; trigger: string; key_column?: string };
}

export interface ScheduleDelivery {
  cron: string;
  output_type: string;
  output_format?: string;
  destination?: string;
}

export async function submitQuery(
  roleId: string,
  query: string,
  variables?: Record<string, unknown>,
  sink?: { topic: string; trigger: string; key_column?: string },
  metadata?: SubmitMetadata,
  schedule?: ScheduleDelivery,
  compiledCypher?: string,
): Promise<{ query_id: number; operation_name: string; message: string }> {
  const input: Record<string, unknown> = { query, role: roleId, variables: variables ?? null };
  if (compiledCypher) input.compiledCypher = compiledCypher;
  if (sink) input.sink = { topic: sink.topic, trigger: sink.trigger, keyColumn: sink.key_column };
  if (schedule) input.schedule = { cron: schedule.cron, outputType: schedule.output_type, outputFormat: schedule.output_format, destination: schedule.destination };
  if (metadata?.business_purpose) input.businessPurpose = metadata.business_purpose;
  if (metadata?.use_cases) input.useCases = metadata.use_cases;
  if (metadata?.data_sensitivity) input.dataSensitivity = metadata.data_sensitivity;
  if (metadata?.refresh_frequency) input.refreshFrequency = metadata.refresh_frequency;
  if (metadata?.expected_row_count) input.expectedRowCount = metadata.expected_row_count;
  if (metadata?.owner_team) input.ownerTeam = metadata.owner_team;
  const data = await gql<{ submitQuery: { queryId: number; operationName: string; message: string } }>(
    `mutation SubmitQuery($input: SubmitQueryInput!) {
      submitQuery(input: $input) { queryId operationName message }
    }`,
    { input },
  );
  const r = data.submitQuery;
  return { query_id: r.queryId, operation_name: r.operationName, message: r.message };
}

export async function runSql(
  sqlText: string,
  role: string = "admin",
  discoveryMode: boolean = false,
): Promise<{ columns: string[]; rows: Record<string, unknown>[]; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "application/json" },
      body: JSON.stringify({ sql: sqlText, role, ...(discoveryMode && { discovery_mode: true }) }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      return { columns: [], rows: [], error: text };
    }
    const json = await resp.json();
    const rows: Record<string, unknown>[] = json?.data?.sql ?? [];
    const columns = rows.length > 0 && rows[0] != null ? Object.keys(rows[0] as object) : [];
    return { columns, rows };
  } catch (e: any) {
    return { columns: [], rows: [], error: e.message };
  }
}

export async function executeQuery(
  roleId: string,
  query: string,
  variables?: Record<string, unknown>
): Promise<any> {
  const resp = await fetch(`${API_BASE}/data/graphql`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Provisa-Role": roleId,
    },
    body: JSON.stringify({ query, variables }),
  });
  return resp.json();
}

// --- Admin: MV, Cache, Health ---

export interface MVInfo {
  id: string;
  sourceTables: string[];
  targetTable: string;
  refreshInterval: number;
  enabled: boolean;
  status: string;
  lastRefreshAt: number | null;
  rowCount: number | null;
  lastError: string | null;
}

export interface CacheStats {
  totalKeys: number;
  hitCount: number;
  missCount: number;
  storeType: string;
}

export interface SystemHealth {
  trinoConnected: boolean;
  trinoWorkerCount: number;
  trinoActiveWorkers: number;
  pgPoolSize: number;
  pgPoolFree: number;
  cacheConnected: boolean;
  flightServerRunning: boolean;
  mvRefreshLoopRunning: boolean;
}

export async function fetchMVList(): Promise<MVInfo[]> {
  const data = await gql<{ mvList: MVInfo[] }>(
    `{ mvList { id sourceTables targetTable refreshInterval enabled status lastRefreshAt rowCount lastError } }`
  );
  return data.mvList;
}

export async function fetchCacheStats(): Promise<CacheStats> {
  const data = await gql<{ cacheStats: CacheStats }>(
    `{ cacheStats { totalKeys hitCount missCount storeType } }`
  );
  return data.cacheStats;
}

export async function fetchSystemHealth(): Promise<SystemHealth> {
  const data = await gql<{ systemHealth: SystemHealth }>(
    `{ systemHealth { trinoConnected trinoWorkerCount trinoActiveWorkers pgPoolSize pgPoolFree cacheConnected flightServerRunning mvRefreshLoopRunning } }`
  );
  return data.systemHealth;
}

export async function refreshMV(mvId: string): Promise<MutationResult> {
  const data = await gql<{ refreshMv: MutationResult }>(
    `mutation($mvId: String!) { refreshMv(mvId: $mvId) { success message } }`,
    { mvId }
  );
  return data.refreshMv;
}

export async function toggleMV(mvId: string, enabled: boolean): Promise<MutationResult> {
  const data = await gql<{ toggleMv: MutationResult }>(
    `mutation($mvId: String!, $enabled: Boolean!) { toggleMv(mvId: $mvId, enabled: $enabled) { success message } }`,
    { mvId, enabled }
  );
  return data.toggleMv;
}

export async function purgeCache(): Promise<MutationResult> {
  const data = await gql<{ purgeCache: MutationResult }>(
    `mutation { purgeCache { success message } }`
  );
  return data.purgeCache;
}

export async function updateSourceCache(sourceId: string, cacheEnabled: boolean, cacheTtl: number | null): Promise<MutationResult> {
  const data = await gql<{ updateSourceCache: MutationResult }>(
    `mutation($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) { updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) { success message } }`,
    { sourceId, cacheEnabled, cacheTtl }
  );
  return data.updateSourceCache;
}

export async function updateTableCache(tableId: number, cacheTtl: number | null): Promise<MutationResult> {
  const data = await gql<{ updateTableCache: MutationResult }>(
    `mutation($tableId: Int!, $cacheTtl: Int) { updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) { success message } }`,
    { tableId, cacheTtl }
  );
  return data.updateTableCache;
}

export async function updateSourceNaming(sourceId: string, namingConvention: string | null): Promise<MutationResult> {
  const data = await gql<{ updateSourceNaming: MutationResult }>(
    `mutation($sourceId: String!, $namingConvention: String) { updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) { success message } }`,
    { sourceId, namingConvention }
  );
  return data.updateSourceNaming;
}

export async function updateSourceAllowedDomains(sourceId: string, allowedDomains: string[]): Promise<MutationResult> {
  const data = await gql<{ updateSourceAllowedDomains: MutationResult }>(
    `mutation($sourceId: String!, $allowedDomains: [String!]!) { updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) { success message } }`,
    { sourceId, allowedDomains }
  );
  return data.updateSourceAllowedDomains;
}

export async function updateTableNaming(tableId: number, namingConvention: string | null): Promise<MutationResult> {
  const data = await gql<{ updateTableNaming: MutationResult }>(
    `mutation($tableId: Int!, $namingConvention: String) { updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) { success message } }`,
    { tableId, namingConvention }
  );
  return data.updateTableNaming;
}

// --- Admin: Scheduled Tasks ---

export interface ScheduledTask {
  id: string;
  name: string;
  cronExpression: string;
  webhookUrl: string | null;
  enabled: boolean;
  lastRunAt: string | null;
  nextRunAt: string | null;
}

export async function fetchScheduledTasks(): Promise<ScheduledTask[]> {
  const data = await gql<{ scheduledTasks: ScheduledTask[] }>(
    `{ scheduledTasks { id name cronExpression webhookUrl enabled lastRunAt nextRunAt } }`
  );
  return data.scheduledTasks;
}

export async function toggleScheduledTask(taskId: string, enabled: boolean): Promise<MutationResult> {
  const data = await gql<{ toggleScheduledTask: MutationResult }>(
    `mutation($taskId: String!, $enabled: Boolean!) { toggleScheduledTask(taskId: $taskId, enabled: $enabled) { success message } }`,
    { taskId, enabled }
  );
  return data.toggleScheduledTask;
}

export async function purgeCacheByTable(tableId: number): Promise<MutationResult> {
  const data = await gql<{ purgeCacheByTable: MutationResult }>(
    `mutation($tableId: Int!) { purgeCacheByTable(tableId: $tableId) { success message } }`,
    { tableId }
  );
  return data.purgeCacheByTable;
}

export async function invalidateFileSource(tableId: number): Promise<MutationResult> {
  const data = await gql<{ invalidateFileSource: MutationResult }>(
    `mutation($tableId: Int!) { invalidateFileSource(tableId: $tableId) { success message } }`,
    { tableId }
  );
  return data.invalidateFileSource;
}

export interface LocalUser {
  id: string;
  username: string;
  email: string | null;
  display_name: string | null;
  roles: string[];
  attributes: Record<string, unknown>;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export async function fetchLocalUsers(): Promise<LocalUser[]> {
  const res = await fetch(`${API_BASE}/admin/users`);
  if (!res.ok) throw new Error(`Failed to fetch users: ${res.status}`);
  return res.json();
}

export async function createLocalUser(body: {
  username: string;
  password: string;
  email?: string;
  display_name?: string;
  roles?: string[];
}): Promise<LocalUser> {
  const res = await fetch(`${API_BASE}/admin/users`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create user: ${text.slice(0, 200)}`);
  }
  return res.json();
}

export async function deleteLocalUser(userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`Failed to delete user: ${res.status}`);
}

export interface UserAssignment {
  id: number;
  role_id: string;
  domain_id: string;
  created_at: string;
}

export async function fetchUserAssignments(userId: string): Promise<UserAssignment[]> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments`);
  if (!res.ok) throw new Error(`Failed to fetch assignments: ${res.status}`);
  return res.json();
}

export async function addUserAssignment(userId: string, roleId: string, domainId: string): Promise<UserAssignment> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ role_id: roleId, domain_id: domainId }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to add assignment: ${text.slice(0, 200)}`);
  }
  return res.json();
}

export async function removeUserAssignment(userId: string, assignmentId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments/${assignmentId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`Failed to remove assignment: ${res.status}`);
}

export interface OrgInvite {
  token: string;
  org_id: string;
  org_name: string;
  role_id: string | null;
  created_by: string;
  expires_at: string;
  used_at: string | null;
  used_by: string | null;
}

export interface InviteInfo {
  token: string;
  org_id: string;
  org_name: string;
  role_id: string | null;
  valid: boolean;
}

export async function fetchInvites(): Promise<OrgInvite[]> {
  const res = await fetch(`${API_BASE}/admin/invites`);
  if (!res.ok) throw new Error(`fetchInvites failed: ${res.status}`);
  return res.json();
}

export async function createInvite(orgId: string, roleId?: string, expiresInDays = 7): Promise<OrgInvite> {
  const res = await fetch(`${API_BASE}/admin/invites`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, role_id: roleId ?? null, expires_in_days: expiresInDays }),
  });
  if (!res.ok) throw new Error(`createInvite failed: ${res.status}`);
  return res.json();
}

export async function revokeInvite(token: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/invites/${token}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`revokeInvite failed: ${res.status}`);
}

export async function fetchInviteInfo(token: string): Promise<InviteInfo> {
  const res = await fetch(`/auth/invite/${token}`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || `Invalid invite: ${res.status}`);
  }
  return res.json();
}
