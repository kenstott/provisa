import type { Role } from "../types/auth";
import type {
  Source,
  Domain,
  RegisteredTable,
  Relationship,
  RLSRule,
  PersistedQuery,
  MutationResult,
} from "../types/admin";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8001";

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

export async function fetchRoles(): Promise<Role[]> {
  const data = await gql<{ roles: Role[] }>(`{ roles { id capabilities domainAccess } }`);
  return data.roles.map((r) => ({
    ...r,
    domain_access: (r as any).domainAccess ?? r.domain_access,
  }));
}

export async function fetchSources(): Promise<Source[]> {
  const data = await gql<{ sources: Source[] }>(
    `{ sources { id type host port database username dialect } }`
  );
  return data.sources;
}

export async function fetchDomains(): Promise<Domain[]> {
  const data = await gql<{ domains: Domain[] }>(`{ domains { id description } }`);
  return data.domains;
}

export async function fetchTables(): Promise<RegisteredTable[]> {
  const data = await gql<{ tables: RegisteredTable[] }>(
    `{ tables { id sourceId domainId schemaName tableName governance alias description columns { id columnName visibleTo alias description } } }`
  );
  return data.tables;
}

export async function fetchRelationships(): Promise<Relationship[]> {
  const data = await gql<{ relationships: Relationship[] }>(
    `{ relationships { id sourceTableId targetTableId sourceTableName targetTableName sourceColumn targetColumn cardinality materialize refreshInterval } }`
  );
  return data.relationships;
}

export async function upsertRelationship(input: {
  id: string;
  sourceTableId: string;
  targetTableId: string;
  sourceColumn: string;
  targetColumn: string;
  cardinality: string;
  materialize: boolean;
  refreshInterval: number;
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
    `{ rlsRules { id tableId roleId filterExpr } }`
  );
  return data.rlsRules;
}

export async function createSource(input: {
  id: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
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
  columns: { name: string; visibleTo: string[] }[];
}): Promise<MutationResult> {
  const data = await gql<{ registerTable: MutationResult }>(
    `mutation($input: TableInput!) { registerTable(input: $input) { success message } }`,
    { input }
  );
  return data.registerTable;
}

export async function deleteTable(id: number): Promise<MutationResult> {
  const data = await gql<{ deleteTable: MutationResult }>(
    `mutation($id: Int!) { deleteTable(id: $id) { success message } }`,
    { id }
  );
  return data.deleteTable;
}

export async function fetchAvailableSchemas(sourceId: string): Promise<string[]> {
  const data = await gql<{ availableSchemas: string[] }>(
    `query($sourceId: String!) { availableSchemas(sourceId: $sourceId) }`,
    { sourceId }
  );
  return data.availableSchemas;
}

export async function fetchAvailableTables(sourceId: string, schemaName: string = "public"): Promise<string[]> {
  const data = await gql<{ availableTables: string[] }>(
    `query($sourceId: String!, $schemaName: String!) { availableTables(sourceId: $sourceId, schemaName: $schemaName) }`,
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

const API_BASE_RAW = import.meta.env.VITE_API_BASE || "http://localhost:8001";

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

export async function acceptCandidate(id: number): Promise<any> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/accept`, {
    method: "POST",
  });
  if (!resp.ok) throw new Error(`Accept failed: ${resp.status}`);
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

export interface ViewConfig {
  id: string;
  sql: string;
  description?: string;
  domain_id: string;
  governance: string;
  materialize: boolean;
  refresh_interval?: number;
  alias?: string;
  columns: { name: string; visible_to: string[]; description?: string }[];
}

export async function fetchViews(): Promise<ViewConfig[]> {
  const resp = await fetch(`${API_BASE_RAW}/admin/views`);
  if (!resp.ok) throw new Error(`Fetch views failed: ${resp.status}`);
  return resp.json();
}

export async function saveView(view: ViewConfig): Promise<{ success: boolean; message: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/views`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(view),
  });
  if (!resp.ok) throw new Error(`Save view failed: ${resp.status}`);
  return resp.json();
}

export async function deleteView(id: string): Promise<{ success: boolean; message: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/views/${id}`, { method: "DELETE" });
  if (!resp.ok) throw new Error(`Delete view failed: ${resp.status}`);
  return resp.json();
}

export async function sampleView(id: string): Promise<{ columns: string[]; rows: Record<string, unknown>[]; count: number }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/views/${id}/sample`, { method: "POST" });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}

// --- Query compilation and submission ---

export async function compileQuery(
  roleId: string,
  query: string,
  variables?: Record<string, unknown>,
): Promise<{
  sql: string;
  trino_sql: string | null;
  direct_sql: string | null;
  params: unknown[];
  route: string;
  route_reason: string;
  sources: string[];
  root_field: string;
}> {
  const resp = await fetch(`${API_BASE_RAW}/data/compile`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Provisa-Role": roleId,
    },
    body: JSON.stringify({ query, variables }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text);
  }
  return resp.json();
}

export async function submitQuery(
  roleId: string,
  query: string,
  variables?: Record<string, unknown>,
): Promise<{ query_id: number; operation_name: string; message: string }> {
  const resp = await fetch(`${API_BASE_RAW}/data/submit`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Provisa-Role": roleId,
    },
    body: JSON.stringify({ query, variables }),
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
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
      "X-Role": roleId,
    },
    body: JSON.stringify({ query, variables }),
  });
  return resp.json();
}
