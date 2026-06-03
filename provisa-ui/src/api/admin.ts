// Copyright (c) 2026 Kenneth Stott
// Canary: 3b91cb86-e709-4766-8d0d-cb129f169966
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Role, RoleAssignment, OrgMembership } from "../types/auth";

const API_BASE = import.meta.env.VITE_API_BASE || "";

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
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members/${userId}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error(`removeOrgMember failed: ${res.status}`);
}

export async function fetchOrgRoles(orgId: string): Promise<Role[]> {
  const res = await fetch(`${API_BASE}/admin/roles`, {
    headers: { "X-Org-Id": orgId },
  });
  if (!res.ok) throw new Error(`fetchOrgRoles failed: ${res.status}`);
  const rows: Array<{ id: string; capabilities: string[]; domain_access: string[] }> =
    await res.json();
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

export async function profileTable(
  tableId: number,
): Promise<{ columns: string[]; rows: Record<string, unknown>[]; rowCount: number }> {
  const resp = await fetch(`${API_BASE}/admin/tables/${tableId}/profile`, { method: "POST" });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(body.detail || resp.statusText);
  }
  return resp.json();
}

export interface TableMetadata {
  name: string;
  comment: string | null;
}

export interface ColumnMetadata {
  name: string;
  dataType: string;
  comment: string | null;
  nativeFilterType: string | null;
  isPrimaryKey: boolean;
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

export async function fetchCandidates(): Promise<unknown[]> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates`);
  if (!resp.ok) throw new Error(`Fetch candidates failed: ${resp.status}`);
  return resp.json();
}

export async function acceptCandidate(id: number, name?: string): Promise<unknown> {
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

export async function runSql(
  sqlText: string,
  role: string = "admin",
  discoveryMode: boolean = false,
): Promise<{ columns: string[]; rows: Record<string, unknown>[]; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
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
  } catch (e) {
    return { columns: [], rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

export async function nlToSql(
  question: string,
  role: string = "admin",
): Promise<{ sql: string; attempts: number; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/nl-to-sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, role }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      return { sql: "", attempts: 0, error: text };
    }
    return await resp.json();
  } catch (e) {
    return { sql: "", attempts: 0, error: e instanceof Error ? e.message : String(e) };
  }
}

export async function executeQuery(
  roleId: string,
  query: string,
  variables?: Record<string, unknown>,
): Promise<unknown> {
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

export async function addUserAssignment(
  userId: string,
  roleId: string,
  domainId: string,
): Promise<UserAssignment> {
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
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments/${assignmentId}`, {
    method: "DELETE",
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
  expiresInDays = 7,
): Promise<OrgInvite> {
  const res = await fetch(`${API_BASE}/admin/invites`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
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

export async function reloadQueryEngineCatalog(
  catalog = "otel",
): Promise<{ success: boolean; errors: string[] }> {
  const res = await fetch(
    `${API_BASE}/admin/query-engine/reload-catalog?catalog=${encodeURIComponent(catalog)}`,
    { method: "POST" },
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
  const res = await fetch(`${API_BASE}/admin/query-engine/restart`, { method: "POST" });
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
  const res = await fetch(`${API_BASE}/admin/schema-clusters/recompute`, { method: "POST" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || `recompute failed: ${res.status}`);
  }
  return res.json();
}
