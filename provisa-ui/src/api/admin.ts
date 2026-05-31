// Copyright (c) 2026 Kenneth Stott
// Canary: e5395602-9100-410a-a4d5-a267fc787a3e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { gql as gqlTag } from '@apollo/client';
import type { Role, RoleAssignment, OrgMembership } from '../types/auth';
import type {
  Source,
  Domain,
  RegisteredTable,
  Relationship,
  RLSRule,
  MutationResult,
} from '../types/admin';
import { client } from '../apolloClient';

const RolesQuery = gqlTag`query { roles { id capabilities domainAccess } }`;
const SourcesQuery = gqlTag`query { sources { id type host port database username dialect cacheEnabled cacheTtl namingConvention allowedDomains description } }`;
const DomainsQuery = gqlTag`query { domains { id description graphqlAlias } }`;
const TablesQuery = gqlTag`query { tables { id sourceId domainId schemaName tableName governance alias description cacheTtl namingConvention watermarkColumn apiEndpoint viewSql dataProduct columns { id columnName visibleTo writableBy unmaskedTo maskType maskPattern maskReplace maskValue maskPrecision alias description nativeFilterType isPrimaryKey isForeignKey isAlternateKey scope } columnPresets { column source name value dataType } } }`;
const RelationshipsQuery = gqlTag`query { relationships { id sourceTableId targetTableId sourceTableName targetTableName sourceColumn targetColumn cardinality materialize refreshInterval targetFunctionName functionArg alias graphqlAlias computedCypherAlias autoSuggested disableCypher } }`;
const RLSRulesQuery = gqlTag`query { rlsRules { id roleId domain table filters { id role_id domain table filter_type filter_sql columns } } }`;
const AvailableSchemas = gqlTag`query AvailableSchemas($sourceId: String!) { availableSchemas(sourceId: $sourceId) }`;
const AvailableTables = gqlTag`query AvailableTables($sourceId: String!, $schemaName: String!) { availableTables(sourceId: $sourceId, schemaName: $schemaName) { name comment } }`;
const AvailableColumns = gqlTag`query AvailableColumns($sourceId: String!, $schemaName: String!, $tableName: String!) { availableColumns(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) }`;
const AvailableColumnsMetadata = gqlTag`query AvailableColumnsMetadata($sourceId: String!, $schemaName: String!, $tableName: String!) { availableColumnsMetadata(sourceId: $sourceId, schemaName: $schemaName, tableName: $tableName) { name dataType comment nativeFilterType isPrimaryKey } }`;
const AvailableFunctions = gqlTag`query AvailableFunctions($sourceId: String!, $schemaName: String!) { availableFunctions(sourceId: $sourceId, schemaName: $schemaName) { name comment } }`;
const GenerateColumnDescription = gqlTag`query GenerateColumnDescription($tableId: String!, $columnName: String!) { generateColumnDescription(tableId: $tableId, columnName: $columnName) }`;
const GenerateTableDescription = gqlTag`query GenerateTableDescription($tableId: String!) { generateTableDescription(tableId: $tableId) }`;
const CompileQuery = gqlTag`query CompileQuery($input: CompileQueryInput!) { compileQuery(input: $input) { sql semanticSql trinoSql directSql route routeReason sources rootField canonicalField compiledCypher cypherError optimizations warnings columnAliases { fieldName column } enforcement { rlsFiltersApplied columnsExcluded schemaScope maskingApplied ceilingApplied route } } }`;
const MVList = gqlTag`query { mvList { id sourceTables targetTable refreshInterval enabled status lastRefreshAt rowCount lastError } }`;
const CacheStats = gqlTag`query { cacheStats { totalKeys hitCount missCount storeType } }`;
const SystemHealth = gqlTag`query { systemHealth { trinoConnected trinoWorkerCount trinoActiveWorkers pgPoolSize pgPoolFree cacheConnected flightServerRunning mvRefreshLoopRunning } }`;
const ScheduledTasks = gqlTag`query { scheduledTasks { id name cronExpression webhookUrl enabled lastRunAt nextRunAt } }`;
const CreateDomain = gqlTag`mutation CreateDomain($id: String!, $description: String!, $graphqlAlias: String) { createDomain(input: { id: $id, description: $description, graphqlAlias: $graphqlAlias }) { success message } }`;
const DeleteDomain = gqlTag`mutation DeleteDomain($id: String!) { deleteDomain(id: $id) { success message } }`;
const RegisterTable = gqlTag`mutation RegisterTable($input: TableInput!) { registerTable(input: $input) { success message } }`;
const UpdateTable = gqlTag`mutation UpdateTable($input: TableInput!) { updateTable(input: $input) { success message } }`;
const DeleteTable = gqlTag`mutation DeleteTable($id: Int!) { deleteTable(id: $id) { success message } }`;
const UpsertRelationship = gqlTag`mutation UpsertRelationship($input: RelationshipInput!) { upsertRelationship(input: $input) { success message } }`;
const DeleteRelationship = gqlTag`mutation DeleteRelationship($id: String!) { deleteRelationship(id: $id) { success message } }`;
const CreateSource = gqlTag`mutation CreateSource($input: SourceInput!) { createSource(input: $input) { success message } }`;
const UpdateSource = gqlTag`mutation UpdateSource($input: SourceInput!) { updateSource(input: $input) { success message } }`;
const DeleteSource = gqlTag`mutation DeleteSource($id: String!) { deleteSource(id: $id) { success message } }`;
const RenameSource = gqlTag`mutation RenameSource($oldId: String!, $newId: String!) { renameSource(oldId: $oldId, newId: $newId) { success message } }`;
const DeployViewToDb = gqlTag`mutation DeployViewToDb($tableId: Int!) { deployViewToDb(tableId: $tableId) { success message } }`;
const RefreshMv = gqlTag`mutation RefreshMv($mvId: String!) { refreshMv(mvId: $mvId) { success message } }`;
const ToggleMv = gqlTag`mutation ToggleMv($mvId: String!, $enabled: Boolean!) { toggleMv(mvId: $mvId, enabled: $enabled) { success message } }`;
const ToggleScheduledTask = gqlTag`mutation ToggleScheduledTask($taskId: String!, $enabled: Boolean!) { toggleScheduledTask(taskId: $taskId, enabled: $enabled) { success message } }`;
const PurgeCacheByTable = gqlTag`mutation PurgeCacheByTable($tableId: Int!) { purgeCacheByTable(tableId: $tableId) { success message } }`;
const InvalidateFileSource = gqlTag`mutation InvalidateFileSource($tableId: Int!) { invalidateFileSource(tableId: $tableId) { success message } }`;

const API_BASE = import.meta.env.VITE_API_BASE || '';

export async function fetchMe(): Promise<{
  user_id: string;
  email: string | null;
  display_name: string | null;
  dev_mode: boolean;
  active_org_id: string | null;
  org_memberships: OrgMembership[];
  assignments: RoleAssignment[];
}> {
  const res = await fetch('/auth/me');
  if (!res.ok) throw new Error('auth/me failed');
  return res.json();
}

export async function fetchProviderType(): Promise<string | null> {
  const res = await fetch('/auth/provider-type');
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
  const res = await fetch('/auth/register', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
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
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id, name }),
  });
  if (!res.ok) throw new Error(`createOrg failed: ${res.status}`);
  return res.json();
}

export async function deleteOrg(orgId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}`, { method: 'DELETE' });
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
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_id: userId }),
  });
  if (!res.ok) throw new Error(`addOrgMember failed: ${res.status}`);
}

export async function removeOrgMember(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members/${userId}`, {
    method: 'DELETE',
  });
  if (!res.ok) throw new Error(`removeOrgMember failed: ${res.status}`);
}

export async function fetchOrgRoles(orgId: string): Promise<Role[]> {
  const res = await fetch(`${API_BASE}/admin/roles`, {
    headers: { 'X-Org-Id': orgId },
  });
  if (!res.ok) throw new Error(`fetchOrgRoles failed: ${res.status}`);
  const rows: Array<{ id: string; capabilities: string[]; domain_access: string[] }> =
    await res.json();
  return rows.map((r) => ({
    id: r.id,
    capabilities: r.capabilities as import('../types/auth').Capability[],
    domain_access: r.domain_access,
  }));
}

export async function createOrgRole(
  orgId: string,
  id: string,
  capabilities: string[],
  domain_access: string[]
): Promise<Role> {
  const res = await fetch(`${API_BASE}/admin/roles`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Org-Id': orgId },
    body: JSON.stringify({ id, capabilities, domain_access }),
  });
  if (!res.ok) throw new Error(`createOrgRole failed: ${res.status}`);
  return res.json();
}

export async function deleteOrgRole(orgId: string, roleId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/roles/${roleId}`, {
    method: 'DELETE',
    headers: { 'X-Org-Id': orgId },
  });
  if (!res.ok) throw new Error(`deleteOrgRole failed: ${res.status}`);
}

export async function fetchRoles(): Promise<Role[]> {
  const result = await client.query({
    query: RolesQuery,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { roles: Role[] };
  return data.roles.map((r) => ({
    ...r,
    domain_access: (r as any).domainAccess ?? r.domain_access,
  }));
}

export async function fetchSources(): Promise<Source[]> {
  const result = await client.query({
    query: SourcesQuery,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { sources: Source[] };
  return data.sources;
}

export async function fetchDomains(): Promise<Domain[]> {
  const result = await client.query({
    query: DomainsQuery,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { domains: Domain[] };
  return data.domains;
}

export async function createDomain(
  id: string,
  description: string,
  graphqlAlias?: string | null
): Promise<void> {
  await client.mutate({
    mutation: CreateDomain,
    variables: { id, description, graphqlAlias: graphqlAlias ?? null },
  });
}

export async function deleteDomain(id: string): Promise<void> {
  await client.mutate({
    mutation: DeleteDomain,
    variables: { id },
  });
}

export async function fetchTables(): Promise<RegisteredTable[]> {
  const result = await client.query({
    query: TablesQuery,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { tables: RegisteredTable[] };
  return data.tables;
}

export async function fetchRelationships(): Promise<Relationship[]> {
  const result = await client.query({
    query: RelationshipsQuery,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { relationships: Relationship[] };
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
  const result = await client.mutate<{ upsertRelationship: MutationResult }>({
    mutation: UpsertRelationship,
    variables: { input },
    refetchQueries: [{ query: RelationshipsQuery }],
  });
  return (result.data?.upsertRelationship ?? { success: false, message: '' }) as MutationResult;
}

export async function deleteRelationship(id: string): Promise<MutationResult> {
  const result = await client.mutate<{ deleteRelationship: MutationResult }>({
    mutation: DeleteRelationship,
    variables: { id },
    refetchQueries: [{ query: RelationshipsQuery }],
  });
  return (result.data?.deleteRelationship ?? { success: false, message: '' }) as MutationResult;
}

export async function fetchRlsRules(): Promise<RLSRule[]> {
  const result = await client.query({
    query: RLSRulesQuery,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { rlsRules: RLSRule[] };
  return data.rlsRules;
}

export async function upsertRlsRule(input: {
  tableId?: string | null;
  domainId?: string | null;
  roleId: string;
  filterExpr: string;
}): Promise<MutationResult> {
  const result = await client.mutate<{ upsertRlsRule: MutationResult }>({
    mutation: gqlTag`mutation($input: RLSRuleInput!) { upsertRlsRule(input: $input) { success message } }`,
    variables: { input },
    refetchQueries: [{ query: RLSRulesQuery }],
  });
  return (result.data?.upsertRlsRule ?? { success: false, message: '' }) as MutationResult;
}

export async function deleteRlsRule(
  roleId: string,
  tableId?: number | null,
  domainId?: string | null
): Promise<MutationResult> {
  const result = await client.mutate<{ deleteRlsRule: MutationResult }>({
    mutation: gqlTag`mutation($roleId: String!, $tableId: Int, $domainId: String) { deleteRlsRule(roleId: $roleId, tableId: $tableId, domainId: $domainId) { success message } }`,
    variables: { roleId, tableId: tableId ?? null, domainId: domainId ?? null },
    refetchQueries: [{ query: RLSRulesQuery }],
  });
  return (result.data?.deleteRlsRule ?? { success: false, message: '' }) as MutationResult;
}

export async function upsertRole(input: {
  id: string;
  capabilities: string[];
  domainAccess: string[];
}): Promise<MutationResult> {
  const result = await client.mutate<{ createRole: MutationResult }>({
    mutation: gqlTag`mutation($input: RoleInput!) { createRole(input: $input) { success message } }`,
    variables: { input },
  });
  return (result.data?.createRole ?? { success: false, message: '' }) as MutationResult;
}

export async function deleteRole(id: string): Promise<MutationResult> {
  const result = await client.mutate<{ deleteRole: MutationResult }>({
    mutation: gqlTag`mutation($id: String!) { deleteRole(id: $id) { success message } }`,
    variables: { id },
  });
  return (result.data?.deleteRole ?? { success: false, message: '' }) as MutationResult;
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
  const result = await client.mutate<{ createSource: MutationResult }>({
    mutation: CreateSource,
    variables: { input },
  });
  return (result.data?.createSource ?? { success: false, message: '' }) as MutationResult;
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
  columns: {
    name: string;
    visibleTo: string[];
    writableBy?: string[];
    unmaskedTo?: string[];
    maskType?: string;
    maskPattern?: string;
    maskReplace?: string;
    maskValue?: string;
    maskPrecision?: string;
    alias?: string;
    description?: string;
    nativeFilterType?: string | null;
    isPrimaryKey?: boolean;
    isForeignKey?: boolean;
    isAlternateKey?: boolean;
    scope?: string;
  }[];
  columnPresets?: {
    column: string;
    source: string;
    name?: string | null;
    value?: string | null;
    dataType?: string | null;
  }[];
}): Promise<MutationResult> {
  const result = await client.mutate<{ registerTable: MutationResult }>({
    mutation: RegisterTable,
    variables: { input },
    refetchQueries: [{ query: TablesQuery }],
  });
  return (result.data?.registerTable ?? { success: false, message: '' }) as MutationResult;
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
  viewSql?: string | null;
  columns: {
    name: string;
    visibleTo: string[];
    writableBy?: string[];
    unmaskedTo?: string[];
    maskType?: string;
    maskPattern?: string;
    maskReplace?: string;
    maskValue?: string;
    maskPrecision?: string;
    alias?: string;
    description?: string;
    nativeFilterType?: string | null;
    isPrimaryKey?: boolean;
    isForeignKey?: boolean;
    isAlternateKey?: boolean;
    scope?: string;
  }[];
  columnPresets?: {
    column: string;
    source: string;
    name?: string | null;
    value?: string | null;
    dataType?: string | null;
  }[];
  dataProduct?: boolean;
}): Promise<MutationResult> {
  const result = await client.mutate<{ updateTable: MutationResult }>({
    mutation: UpdateTable,
    variables: { input },
    refetchQueries: [{ query: TablesQuery }],
  });
  return (result.data?.updateTable ?? { success: false, message: '' }) as MutationResult;
}

export async function deployViewToDb(tableId: number): Promise<MutationResult> {
  const result = await client.mutate<{ deployViewToDb: MutationResult }>({
    mutation: DeployViewToDb,
    variables: { tableId },
    refetchQueries: [{ query: TablesQuery }],
  });
  return (result.data?.deployViewToDb ?? { success: false, message: '' }) as MutationResult;
}

export async function deleteTable(id: number): Promise<MutationResult> {
  const result = await client.mutate<{ deleteTable: MutationResult }>({
    mutation: DeleteTable,
    variables: { id },
    refetchQueries: [{ query: TablesQuery }],
  });
  return (result.data?.deleteTable ?? { success: false, message: '' }) as MutationResult;
}

export async function profileTable(
  tableId: number
): Promise<{ columns: string[]; rows: Record<string, unknown>[]; rowCount: number }> {
  const resp = await fetch(`${API_BASE}/admin/tables/${tableId}/profile`, { method: 'POST' });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}

export async function generateColumnDescription(
  tableId: number,
  columnName: string
): Promise<string> {
  const result = await client.query({
    query: GenerateColumnDescription,
    variables: { tableId: String(tableId), columnName },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { generateColumnDescription: string };
  return data.generateColumnDescription;
}

export async function generateTableDescription(tableId: number): Promise<string> {
  const result = await client.query({
    query: GenerateTableDescription,
    variables: { tableId: String(tableId) },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { generateTableDescription: string };
  return data.generateTableDescription;
}

export async function fetchAvailableSchemas(sourceId: string): Promise<string[]> {
  const result = await client.query({
    query: AvailableSchemas,
    variables: { sourceId },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { availableSchemas: string[] };
  return data.availableSchemas;
}

export interface TableMetadata {
  name: string;
  comment: string | null;
}

export async function fetchAvailableTables(
  sourceId: string,
  schemaName: string = 'public'
): Promise<TableMetadata[]> {
  const result = await client.query({
    query: AvailableTables,
    variables: { sourceId, schemaName },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { availableTables: TableMetadata[] };
  return data.availableTables;
}

export async function fetchAvailableColumns(
  sourceId: string,
  schemaName: string,
  tableName: string
): Promise<string[]> {
  const result = await client.query({
    query: AvailableColumns,
    variables: { sourceId, schemaName, tableName },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { availableColumns: string[] };
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
  tableName: string
): Promise<ColumnMetadata[]> {
  const result = await client.query({
    query: AvailableColumnsMetadata,
    variables: { sourceId, schemaName, tableName },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { availableColumnsMetadata: ColumnMetadata[] };
  return data.availableColumnsMetadata;
}

export async function fetchAvailableFunctions(
  sourceId: string,
  schemaName = 'openapi'
): Promise<TableMetadata[]> {
  const result = await client.query({
    query: AvailableFunctions,
    variables: { sourceId, schemaName },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { availableFunctions: TableMetadata[] };
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
  const result = await client.mutate<{ updateSource: MutationResult }>({
    mutation: UpdateSource,
    variables: { input },
    refetchQueries: [{ query: SourcesQuery }],
  });
  return (result.data?.updateSource ?? { success: false, message: '' }) as MutationResult;
}

export async function renameSource(oldId: string, newId: string): Promise<MutationResult> {
  const result = await client.mutate<{ renameSource: MutationResult }>({
    mutation: RenameSource,
    variables: { oldId, newId },
    refetchQueries: [{ query: SourcesQuery }],
  });
  return (result.data?.renameSource ?? { success: false, message: '' }) as MutationResult;
}

export async function deleteSource(id: string): Promise<MutationResult> {
  const result = await client.mutate<{ deleteSource: MutationResult }>({
    mutation: DeleteSource,
    variables: { id },
    refetchQueries: [{ query: SourcesQuery }],
  });
  return (result.data?.deleteSource ?? { success: false, message: '' }) as MutationResult;
}

export async function fetchSdl(roleId: string): Promise<string> {
  const resp = await fetch(`${API_BASE}/data/sdl`, {
    headers: { 'X-Role': roleId },
  });
  if (!resp.ok) throw new Error(`SDL fetch failed: ${resp.status}`);
  return resp.text();
}

// --- Discovery ---

const API_BASE_RAW = import.meta.env.VITE_API_BASE || '';

export async function discoverRelationships(
  scope: string,
  tableId?: number,
  domainId?: string
): Promise<{ candidates_found: number; stored_ids: number[] }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/relationships`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
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
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
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
    method: 'DELETE',
  });
  if (!resp.ok) throw new Error(`Clear rejections failed: ${resp.status}`);
  return resp.json();
}

export async function rejectCandidate(id: number, reason: string): Promise<void> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/reject`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
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
  }
): Promise<DiscoverSchemaResponse> {
  const resp = await fetch(`${API_BASE_RAW}/admin/schema-discovery/discover/${sourceId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
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
    method: 'PUT',
    headers: { 'Content-Type': 'application/x-yaml' },
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
    support_endpoint: string;
    support_redact_sql_literals: boolean;
    support_redact_attributes: string[];
  };
}

export async function fetchSettings(): Promise<PlatformSettings> {
  const resp = await fetch(`${API_BASE_RAW}/admin/settings`);
  if (!resp.ok) throw new Error(`Settings fetch failed: ${resp.status}`);
  return resp.json();
}

export async function updateSettings(
  settings: Partial<PlatformSettings>
): Promise<{ success: boolean; updated: string[] }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/settings`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
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
  flatSql?: boolean,
  flatCypher?: boolean,
  nodeOnlyCypher?: boolean
): Promise<CompileResult | { queries: CompileResult[] }> {
  const result = await client.query({
    query: CompileQuery,
    variables: {
      input: {
        query,
        role: roleId,
        variables: variables ?? null,
        flatSql: flatSql ?? false,
        flatCypher: flatCypher ?? false,
        nodeOnlyCypher: nodeOnlyCypher ?? false,
      },
    },
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { compileQuery: CompileResult[] };
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

export async function runSql(
  sqlText: string,
  role: string = 'admin',
  discoveryMode: boolean = false
): Promise<{ columns: string[]; rows: Record<string, unknown>[]; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
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

export async function nlToSql(
  question: string,
  role: string = 'admin'
): Promise<{ sql: string; attempts: number; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/nl-to-sql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question, role }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      return { sql: '', attempts: 0, error: text };
    }
    return await resp.json();
  } catch (e: any) {
    return { sql: '', attempts: 0, error: e.message };
  }
}

export async function executeQuery(
  roleId: string,
  query: string,
  variables?: Record<string, unknown>
): Promise<any> {
  const resp = await fetch(`${API_BASE}/data/graphql`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Provisa-Role': roleId,
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
  const result = await client.query({
    query: MVList,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { mvList: MVInfo[] };
  return data.mvList;
}

export async function fetchCacheStats(): Promise<CacheStats> {
  const result = await client.query({
    query: CacheStats,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { cacheStats: CacheStats };
  return data.cacheStats;
}

export async function fetchSystemHealth(): Promise<SystemHealth> {
  const result = await client.query({
    query: SystemHealth,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { systemHealth: SystemHealth };
  return data.systemHealth;
}

export async function refreshMV(mvId: string): Promise<MutationResult> {
  const result = await client.mutate<{ refreshMv: MutationResult }>({
    mutation: RefreshMv,
    variables: { mvId },
  });
  return (result.data?.refreshMv ?? { success: false, message: '' }) as MutationResult;
}

export async function toggleMV(mvId: string, enabled: boolean): Promise<MutationResult> {
  const result = await client.mutate<{ toggleMv: MutationResult }>({
    mutation: ToggleMv,
    variables: { mvId, enabled },
  });
  return (result.data?.toggleMv ?? { success: false, message: '' }) as MutationResult;
}

export async function purgeCache(): Promise<MutationResult> {
  const result = await client.mutate<{ purgeCache: MutationResult }>({
    mutation: gqlTag`mutation { purgeCache { success message } }`,
  });
  return (result.data?.purgeCache ?? { success: false, message: '' }) as MutationResult;
}

export async function updateSourceCache(
  sourceId: string,
  cacheEnabled: boolean,
  cacheTtl: number | null
): Promise<MutationResult> {
  const result = await client.mutate<{ updateSourceCache: MutationResult }>({
    mutation: gqlTag`mutation($sourceId: String!, $cacheEnabled: Boolean!, $cacheTtl: Int) { updateSourceCache(sourceId: $sourceId, cacheEnabled: $cacheEnabled, cacheTtl: $cacheTtl) { success message } }`,
    variables: { sourceId, cacheEnabled, cacheTtl },
  });
  return (result.data?.updateSourceCache ?? { success: false, message: '' }) as MutationResult;
}

export async function updateTableCache(
  tableId: number,
  cacheTtl: number | null
): Promise<MutationResult> {
  const result = await client.mutate<{ updateTableCache: MutationResult }>({
    mutation: gqlTag`mutation($tableId: Int!, $cacheTtl: Int) { updateTableCache(tableId: $tableId, cacheTtl: $cacheTtl) { success message } }`,
    variables: { tableId, cacheTtl },
  });
  return (result.data?.updateTableCache ?? { success: false, message: '' }) as MutationResult;
}

export async function updateSourceNaming(
  sourceId: string,
  namingConvention: string | null
): Promise<MutationResult> {
  const result = await client.mutate<{ updateSourceNaming: MutationResult }>({
    mutation: gqlTag`mutation($sourceId: String!, $namingConvention: String) { updateSourceNaming(sourceId: $sourceId, namingConvention: $namingConvention) { success message } }`,
    variables: { sourceId, namingConvention },
  });
  return (result.data?.updateSourceNaming ?? { success: false, message: '' }) as MutationResult;
}

export async function updateSourceAllowedDomains(
  sourceId: string,
  allowedDomains: string[]
): Promise<MutationResult> {
  const result = await client.mutate<{ updateSourceAllowedDomains: MutationResult }>({
    mutation: gqlTag`mutation($sourceId: String!, $allowedDomains: [String!]!) { updateSourceAllowedDomains(sourceId: $sourceId, allowedDomains: $allowedDomains) { success message } }`,
    variables: { sourceId, allowedDomains },
  });
  return (result.data?.updateSourceAllowedDomains ?? {
    success: false,
    message: '',
  }) as MutationResult;
}

export async function updateTableNaming(
  tableId: number,
  namingConvention: string | null
): Promise<MutationResult> {
  const result = await client.mutate<{ updateTableNaming: MutationResult }>({
    mutation: gqlTag`mutation($tableId: Int!, $namingConvention: String) { updateTableNaming(tableId: $tableId, namingConvention: $namingConvention) { success message } }`,
    variables: { tableId, namingConvention },
  });
  return (result.data?.updateTableNaming ?? { success: false, message: '' }) as MutationResult;
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
  const result = await client.query({
    query: ScheduledTasks,
    fetchPolicy: 'cache-first',
  });
  const data = result.data as { scheduledTasks: ScheduledTask[] };
  return data.scheduledTasks;
}

export async function toggleScheduledTask(
  taskId: string,
  enabled: boolean
): Promise<MutationResult> {
  const result = await client.mutate<{ toggleScheduledTask: MutationResult }>({
    mutation: ToggleScheduledTask,
    variables: { taskId, enabled },
  });
  return (result.data?.toggleScheduledTask ?? { success: false, message: '' }) as MutationResult;
}

export async function purgeCacheByTable(tableId: number): Promise<MutationResult> {
  const result = await client.mutate<{ purgeCacheByTable: MutationResult }>({
    mutation: PurgeCacheByTable,
    variables: { tableId },
  });
  return (result.data?.purgeCacheByTable ?? { success: false, message: '' }) as MutationResult;
}

export async function invalidateFileSource(tableId: number): Promise<MutationResult> {
  const result = await client.mutate<{ invalidateFileSource: MutationResult }>({
    mutation: InvalidateFileSource,
    variables: { tableId },
  });
  return (result.data?.invalidateFileSource ?? { success: false, message: '' }) as MutationResult;
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
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create user: ${text.slice(0, 200)}`);
  }
  return res.json();
}

export async function deleteLocalUser(userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}`, { method: 'DELETE' });
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

export async function addUserAssignment(
  userId: string,
  roleId: string,
  domainId: string
): Promise<UserAssignment> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ role_id: roleId, domain_id: domainId }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to add assignment: ${text.slice(0, 200)}`);
  }
  return res.json();
}

export async function removeUserAssignment(userId: string, assignmentId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments/${assignmentId}`, {
    method: 'DELETE',
  });
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

export async function createInvite(
  orgId: string,
  roleId?: string,
  expiresInDays = 7
): Promise<OrgInvite> {
  const res = await fetch(`${API_BASE}/admin/invites`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      org_id: orgId,
      role_id: roleId ?? null,
      expires_in_days: expiresInDays,
    }),
  });
  if (!res.ok) throw new Error(`createInvite failed: ${res.status}`);
  return res.json();
}

export async function revokeInvite(token: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/invites/${token}`, { method: 'DELETE' });
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

export async function reloadQueryEngineCatalog(
  catalog = 'otel'
): Promise<{ success: boolean; errors: string[] }> {
  const res = await fetch(
    `${API_BASE}/admin/query-engine/reload-catalog?catalog=${encodeURIComponent(catalog)}`,
    { method: 'POST' }
  );
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || `reload-catalog failed: ${res.status}`);
  }
  return res.json();
}

export async function restartQueryEngine(): Promise<{
  success: boolean;
  container: string;
  output: string;
}> {
  const res = await fetch(`${API_BASE}/admin/query-engine/restart`, { method: 'POST' });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || `restart failed: ${res.status}`);
  }
  return res.json();
}

export async function recomputeSchemaClusters(): Promise<{
  success: boolean;
  tables_clustered: number;
}> {
  const res = await fetch(`${API_BASE}/admin/schema-clusters/recompute`, { method: 'POST' });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || `recompute failed: ${res.status}`);
  }
  return res.json();
}
