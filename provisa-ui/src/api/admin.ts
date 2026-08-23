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
import { ORG_HEADER } from "../lib/authFetch";
import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/**
 * REQ-1286: a failed /auth/me carries WHY it failed. 401/403 is "this session is not valid" —
 * sign in again. Anything else (5xx, network) is the deployment failing, and reporting that as
 * "no account access" sends the user to ask an administrator for an invitation they already have.
 * status is 0 when the request never reached the server.
 */
export class AuthMeError extends Error {
  readonly status: number;
  constructor(status: number) {
    super(`auth/me failed: ${status}`);
    this.name = "AuthMeError";
    this.status = status;
  }
}

export async function fetchMe(): Promise<{
  user_id: string;
  email: string | null;
  display_name: string | null;
  given_name: string | null;
  family_name: string | null;
  dev_mode: boolean;
  billing: boolean; // REQ-1469
  active_org_id: string | null;
  org_memberships: OrgMembership[];
  assignments: RoleAssignment[];
}> {
  let res: Response;
  try {
    res = await fetch("/auth/me");
  } catch {
    throw new AuthMeError(0);
  }
  if (!res.ok) throw new AuthMeError(res.status);
  return res.json();
}

// REQ-1266: the user's own first/last name. display_name/email mirror the IdP and are read-only;
// given_name/family_name have no IdP source, so the user supplies them here.
export async function updateProfile(body: {
  given_name: string | null;
  family_name: string | null;
}): Promise<{ user_id: string; given_name: string | null; family_name: string | null }> {
  const res = await fetch("/auth/profile", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("updateProfile", res.status)));
  }
  return res.json();
}

export async function fetchProviderType(): Promise<string | null> {
  const res = await fetch("/auth/provider-type");
  if (!res.ok) return null;
  const data = await res.json();
  return data.provider ?? null;
}

// REQ-1290: claim the sole platform-admin slot for the signed-in caller. Called ONLY from the
// first-login page, immediately after the user picks a provider on the page that told them what
// claiming means. The server no longer claims it while validating a token, so a refresh with a
// still-valid credential cannot take platform admin behind the user's back.
export async function claimBootstrap(): Promise<boolean> {
  const res = await fetch("/auth/claim-bootstrap", { method: "POST" });
  if (!res.ok) throw new Error(requestFailed("claim platform admin", res.status));
  const data = await res.json();
  return data.claimed === true;
}

// REQ-1288: is the sole platform-admin slot still unclaimed? The login page asks before the user
// picks a provider, so that whoever signs in first is told they are about to become the platform
// admin rather than discovering it afterwards. Unauthenticated, like /auth/provider-type.
export async function fetchBootstrapStatus(): Promise<boolean> {
  const res = await fetch("/auth/bootstrap-status");
  if (!res.ok) throw new Error(requestFailed("read bootstrap status", res.status));
  const data = await res.json();
  return data.unclaimed === true;
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
    throw new Error(serverMessage(data, requestFailed("Registration", res.status)));
  }
  return res.json();
}

export interface Org {
  id: string;
  name: string;
  created_by: string | null;
  created_at: string;
  // REQ-1043/REQ-1067: true when the org runs on a dedicated federation engine.
  isolated_engine: boolean;
}

export async function fetchOrgs(): Promise<Org[]> {
  const res = await fetch(`${API_BASE}/admin/orgs`);
  if (!res.ok) throw new Error(requestFailed("fetchOrgs", res.status));
  return res.json();
}

export interface OrgProvisioning {
  id: string;
  name: string;
  // REQ-1476: on a commercial deployment an org is reserved as "awaiting_checkout" and only leaves
  // that state when its subscription exists.
  provisioning_state: "awaiting_checkout" | "provisioning" | "ready" | "failed";
  provisioning_error?: string | null;
}

export interface OrgJoinPolicy {
  // REQ-1268/REQ-1269: regex an invitee's email must match; auto-join grants membership to any
  // matching email with autoJoinRole, no invite required.
  emailRule?: string | null;
  autoJoin?: boolean;
  autoJoinRole?: string | null;
  // REQ-1567: the author's acceptance that a rule reaching past one exact domain may admit people
  // from outside their organization. The server measures the reach and refuses without this; it is
  // sent only after the author has been shown what the rule would admit.
  riskAcknowledged?: boolean;
}

/** An /admin/orgs error carrying the server's stable code, which the join-policy copy branches on. */
export class OrgError extends Error {
  readonly status: number;
  readonly code: string | null;
  constructor(status: number, code: string | null, message: string) {
    super(message);
    this.name = "OrgError";
    this.status = status;
    this.code = code;
  }
}

export async function createOrg(
  id: string,
  name: string,
  includeDemo = false,
  policy: OrgJoinPolicy = {},
  // REQ-1043/REQ-1067: run the org on a dedicated federation engine (premium lane; surfaced as a
  // plain checkbox until billing gates it).
  isolatedEngine = false,
): Promise<OrgProvisioning> {
  const res = await fetch(`${API_BASE}/admin/orgs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      id,
      name,
      include_demo: includeDemo,
      email_rule: policy.emailRule ?? null,
      auto_join: policy.autoJoin ?? false,
      auto_join_role: policy.autoJoinRole ?? null,
      auto_join_risk_acknowledged: policy.riskAcknowledged ?? false,
      isolated_engine: isolatedEngine,
    }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new OrgError(
      res.status,
      typeof data.code === "string" ? data.code : null,
      serverMessage(data, requestFailed("createOrg", res.status)),
    );
  }
  return res.json();
}

export interface OrgJoinSettings {
  id: string;
  email_rule: string | null;
  auto_join: boolean;
  auto_join_role: string | null;
}

export async function fetchOrgSettings(orgId: string): Promise<OrgJoinSettings> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/settings`);
  if (!res.ok) throw new Error(requestFailed("fetchOrgSettings", res.status));
  return res.json();
}

export async function updateOrgSettings(
  orgId: string,
  policy: OrgJoinPolicy,
): Promise<{
  id: string;
  email_rule: string | null;
  auto_join: boolean;
  auto_join_role: string | null;
}> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/settings`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      email_rule: policy.emailRule ?? null,
      auto_join: policy.autoJoin ?? false,
      auto_join_role: policy.autoJoinRole ?? null,
      auto_join_risk_acknowledged: policy.riskAcknowledged ?? false,
    }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new OrgError(
      res.status,
      typeof data.code === "string" ? data.code : null,
      serverMessage(data, requestFailed("updateOrgSettings", res.status)),
    );
  }
  return res.json();
}

export async function fetchOrgStatus(orgId: string): Promise<OrgProvisioning> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/status`);
  if (!res.ok) throw new Error(requestFailed("fetchOrgStatus", res.status));
  return res.json();
}

// REQ-1300: deletion is unrecoverable, so the server refuses it unless the caller repeats the org
// id back. The UI types that ceremony rather than sending a bare DELETE.
export async function deleteOrg(orgId: string, confirm: string): Promise<void> {
  const res = await fetch(
    `${API_BASE}/admin/orgs/${orgId}?confirm=${encodeURIComponent(confirm)}`,
    { method: "DELETE" },
  );
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("deleteOrg", res.status)));
  }
}

// REQ-1304: the org's live config YAML — the same document the config surface serves — so a
// departing org_admin can take their work with them before REQ-1300 destroys it.
export async function exportOrgConfig(orgId: string): Promise<string> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/config-export`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("exportOrgConfig", res.status)));
  }
  return res.text();
}

// REQ-1303/REQ-1308: an org_admin hands the keys to someone else, or takes them back. The server
// refuses the last removal (REQ-1302), so a failure here is a real answer, not a UI guess.
export async function grantOrgAdmin(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/admins/${userId}`, { method: "POST" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("grantOrgAdmin", res.status)));
  }
}

export async function revokeOrgAdmin(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/admins/${userId}`, { method: "DELETE" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("revokeOrgAdmin", res.status)));
  }
}

// REQ-1306: leaving is the member's own act, so it is not the org_admin's member-removal route.
export async function leaveOrg(orgId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/leave`, { method: "POST" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("leaveOrg", res.status)));
  }
}

// REQ-1478: record that the user has been told how they came to belong to this org, so the notice
// is shown once rather than on every sign-in.
export async function acknowledgeJoin(orgId: string): Promise<void> {
  const res = await fetch(`/auth/acknowledge-join`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("acknowledgeJoin", res.status)));
  }
}

// REQ-1307/REQ-1312: the account itself. Same typed ceremony as org deletion, against the user id.
export async function deleteAccount(confirm: string): Promise<void> {
  const res = await fetch(`${API_BASE}/auth/account?confirm=${encodeURIComponent(confirm)}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("deleteAccount", res.status)));
  }
}

// REQ-1263: personal access tokens. The caller's own tokens in their active org — there is no
// route here to read or revoke someone else's, so these take no user id.
export interface PersonalAccessToken {
  // The SHA-256 of the secret. It is the token's id for revocation; the secret itself is
  // returned once, by issuePersonalAccessToken, and is unrecoverable afterwards.
  token_hash: string;
  prefix: string;
  name: string;
  role_id: string | null;
  scopes: string[];
  created_at: string;
  expires_at: string | null;
  last_used_at: string | null;
  revoked_at: string | null;
}

export async function listPersonalAccessTokens(): Promise<PersonalAccessToken[]> {
  const res = await fetch(`${API_BASE}/auth/tokens`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("listPersonalAccessTokens", res.status)));
  }
  return res.json();
}

export async function issuePersonalAccessToken(body: {
  name: string;
  role_id?: string | null;
  scopes?: string[];
  expires_in_days?: number | null;
}): Promise<{ token: string; prefix: string; name: string; expires_at: string | null }> {
  const res = await fetch(`${API_BASE}/auth/tokens`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("issuePersonalAccessToken", res.status)));
  }
  return res.json();
}

export async function revokePersonalAccessToken(tokenHash: string): Promise<void> {
  const res = await fetch(`${API_BASE}/auth/tokens/${encodeURIComponent(tokenHash)}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("revokePersonalAccessToken", res.status)));
  }
}

export interface OrgMember {
  user_id: string;
  email: string | null;
  display_name: string | null;
  provider: string | null;
  // REQ-1302/REQ-1303: whether this person holds org_admin in the org. The server joins it from the
  // tenant plane; the team page needs it to decide between a promote and a demote control.
  is_org_admin: boolean;
}

export async function fetchOrgMembers(orgId: string): Promise<OrgMember[]> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members`);
  if (!res.ok) throw new Error(requestFailed("fetchOrgMembers", res.status));
  return res.json();
}

export async function addOrgMember(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId }),
  });
  if (!res.ok) throw new Error(requestFailed("addOrgMember", res.status));
}

export async function removeOrgMember(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members/${userId}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error(requestFailed("removeOrgMember", res.status));
}

export async function fetchOrgRoles(orgId: string): Promise<Role[]> {
  const res = await fetch(`${API_BASE}/admin/roles`, {
    headers: { [ORG_HEADER]: orgId },
  });
  if (!res.ok) throw new Error(requestFailed("fetchOrgRoles", res.status));
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
    headers: { "Content-Type": "application/json", [ORG_HEADER]: orgId },
    body: JSON.stringify({ id, capabilities, domain_access }),
  });
  if (!res.ok) throw new Error(requestFailed("createOrgRole", res.status));
  return res.json();
}

export async function deleteOrgRole(orgId: string, roleId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/roles/${roleId}`, {
    method: "DELETE",
    headers: { [ORG_HEADER]: orgId },
  });
  if (!res.ok) throw new Error(requestFailed("deleteOrgRole", res.status));
}

export async function profileTable(
  tableId: number,
  role: string,
): Promise<{ columns: string[]; rows: Record<string, unknown>[]; rowCount: number }> {
  const resp = await fetch(`${API_BASE}/admin/tables/${tableId}/profile`, {
    method: "POST",
    headers: { "X-Provisa-Role": role },
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed("profileTable", resp.status)));
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
  if (!resp.ok) throw new Error(requestFailed("SDL fetch", resp.status));
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
  if (!resp.ok) throw new Error(requestFailed("Discovery", resp.status));
  return resp.json();
}

export async function fetchCandidates(): Promise<unknown[]> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates`);
  if (!resp.ok) throw new Error(requestFailed("Fetch candidates", resp.status));
  return resp.json();
}

export async function acceptCandidate(id: number, name?: string): Promise<unknown> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/accept`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: name ?? null }),
  });
  if (!resp.ok) throw new Error(requestFailed("Accept", resp.status));
  return resp.json();
}

export async function fetchRejectedCount(): Promise<number> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/rejected/count`);
  if (!resp.ok) throw new Error(requestFailed("Fetch rejected count", resp.status));
  const data = await resp.json();
  return data.count;
}

export async function clearRejectedCandidates(): Promise<{ deleted: number }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/rejected`, {
    method: "DELETE",
  });
  if (!resp.ok) throw new Error(requestFailed("Clear rejections", resp.status));
  return resp.json();
}

export async function rejectCandidate(id: number, reason: string): Promise<void> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/reject`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason }),
  });
  if (!resp.ok) throw new Error(requestFailed("Reject", resp.status));
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
    throw new Error(serverMessage(body, requestFailed("discoverSourceSchema", resp.status)));
  }
  return resp.json();
}

// REQ-1093: introspect declared UNIQUE constraints for one (schema, table) to seed the
// register/edit "Uniques" panel. Empty list when the source declares none.
export async function fetchTableUniqueConstraints(
  sourceId: string,
  schema: string,
  table: string,
): Promise<{ name: string; columns: string[] }[]> {
  const qs = new URLSearchParams({ schema, table }).toString();
  const resp = await fetch(
    `${API_BASE_RAW}/admin/schema-discovery/unique-constraints/${sourceId}?${qs}`,
  );
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed("fetchTableUniqueConstraints", resp.status)));
  }
  const json = await resp.json();
  return json.unique_constraints ?? [];
}

// The canonical IR data-type vocabulary (REQ-846) offered when a steward assigns a
// column's type during schema discovery — engine-independent, translated to the store's
// physical type at landing.
export async function fetchIrTypes(): Promise<string[]> {
  const resp = await fetch(`${API_BASE_RAW}/admin/schema-discovery/ir-types`);
  if (!resp.ok) throw new Error(requestFailed("IR types fetch", resp.status));
  return resp.json();
}

// --- Config ---

export async function downloadConfig(): Promise<string> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config`);
  if (!resp.ok) throw new Error(requestFailed("Config download", resp.status));
  return resp.text();
}

// Both sides of the config diff — original (on-disk file) and current (live state) — normalized
// identically server-side so the diff shows only genuine changes, not section/key reordering.
export async function fetchConfigDiff(): Promise<{ original: string; current: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config/diff`);
  if (!resp.ok) throw new Error(requestFailed("Config diff", resp.status));
  return resp.json();
}

// A unified-diff patch (git-apply / patch compatible) from the startup baseline to the curated
// config — for committing UI config changes through CI/CD.
export async function downloadConfigPatch(revised: string): Promise<string> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config/patch`, {
    method: "POST",
    headers: { "Content-Type": "application/x-yaml" },
    body: revised,
  });
  if (!resp.ok) throw new Error(requestFailed("Config patch", resp.status));
  return resp.text();
}

export async function uploadConfig(yaml: string): Promise<{ success: boolean; message: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/x-yaml" },
    body: yaml,
  });
  if (!resp.ok) throw new Error(requestFailed("Config upload", resp.status));
  return resp.json();
}

// --- Platform Settings ---

/** REQ-1432: the trace switches, one per logical subsystem. Mirrors SubsystemTracesConfig. */
export interface SubsystemTraces {
  http_api: boolean;
  outbound_http: boolean;
  catalog_database: boolean;
  result_cache: boolean;
  document_sources: boolean;
  search_sources: boolean;
  grpc_services: boolean;
}

export const SUBSYSTEM_TRACE_KEYS: (keyof SubsystemTraces)[] = [
  "http_api",
  "outbound_http",
  "catalog_database",
  "result_cache",
  "document_sources",
  "search_sources",
  "grpc_services",
];

export interface PlatformSettings {
  features?: {
    live_config_export: boolean;
    // REQ-1349: true when the caller holds `platform_settings`. The deployment-wide blocks below
    // are OMITTED from the payload for a caller without it, so a surface that edits one renders
    // only when this is set rather than reading an absent block.
    platform_settings?: boolean;
  };
  // Deployment-wide (platform_settings). Optional because GET /admin/settings drops them for an
  // org administrator, whose settings are the org-scoped `redirect`, `cache` and `naming` mode.
  engine?: {
    jvm_heap_gb: number;
    query_max_memory: string;
    query_max_memory_per_node: string;
    query_max_total_memory: string;
    fault_tolerant_execution: boolean;
    fault_tolerant_task_memory: string;
    exchange_spool_dir: string;
  };
  redirect: {
    enabled: boolean;
    threshold: number;
    default_format: string;
    ttl: number;
  };
  sampling?: {
    default_sample_size: number;
  };
  cache: {
    default_ttl: number;
  };
  naming: {
    // Deployment-wide, so absent for an org administrator; the two below are the org's own.
    domain_prefix?: boolean;
    convention?: string;
    sql_convention?: string;
    use_domains: boolean | null;
    default_domain: string;
  };
  otel?: {
    endpoint: string;
    service_name: string;
    sample_rate: number;
    log_level: string;
    compact_cron: string;
    compact_batch_size: number;
    compact_file_chunk: number;
    ops_snapshot_retention_hours: number | null;
    span_export_delay_millis: number;
    otlp2parquet_max_age_secs: number;
    collector_batch_timeout_ms: number;
    s3_endpoint: string;
    support_endpoint: string;
    support_redact_sql_literals: boolean;
    support_redact_attributes: string[];
    // REQ-1432: which subsystems emit spans. Keys are the logical subsystem names the backend's
    // SubsystemTracesConfig declares.
    subsystem_traces: SubsystemTraces;
  };
  graphql_remote?: {
    max_object_depth: number;
    max_list_depth: number;
    max_list_items: number;
  };
  cdc?: {
    consumer_group_id: string;
  };
  materialize?: {
    store_url: string;
  };
}

/**
 * A settings read started ahead of the page that will consume it.
 *
 * TablesPage (and its viewsOnly twin, /views) holds its loading state until this call returns, so
 * on a loaded machine the whole page waits on one uncached round-trip that only begins once the
 * route has mounted. The guided tour knows one step early that such a page is next and calls
 * {@link prefetchSettings}; the page's own fetchSettings then adopts that promise.
 *
 * The entry is consumed exactly once, so a later read — a save on the Admin page, a manual
 * refresh — always re-queries the server and can never be served a stale snapshot.
 */
let prefetchedSettings: Promise<PlatformSettings> | null = null;

/**
 * Start the settings read now so a later {@link fetchSettings} resolves without waiting. A
 * rejection is held until the real caller adopts it, so the error still reaches the page that
 * asked for settings.
 */
export function prefetchSettings(): void {
  if (prefetchedSettings) return;
  const inflight = fetchSettings();
  prefetchedSettings = inflight;
  // An unadopted rejection would surface as an unhandled promise rejection; a no-op handler marks
  // it handled without discarding it — the stored promise still rejects for whoever adopts it.
  inflight.catch(() => undefined);
}

export async function fetchSettings(): Promise<PlatformSettings> {
  const warm = prefetchedSettings;
  if (warm) {
    prefetchedSettings = null;
    return warm;
  }
  const resp = await fetch(`${API_BASE_RAW}/admin/settings`);
  if (!resp.ok) throw new Error(requestFailed("Settings fetch", resp.status));
  return resp.json();
}

// --- Federation engine selection (REQ-916) ---

export interface EngineConfigField {
  config_key: string;
  label: string;
  type: "string" | "number" | "boolean" | "select";
  required: boolean;
  placeholder?: string;
  /** Choices for `type: "select"`. */
  options?: { value: string; label: string }[];
}

export interface EngineRegistryEntry {
  key: string;
  label: string;
  description: string;
  config_fields: EngineConfigField[];
  /** Types this engine reads LIVE via a live-attach connector (queried in place, always fresh). */
  reachable_source_types?: string[];
  /** Every type configurable on this engine: live-attach ∪ landed-replica reach (REQ-947). */
  live_source_types?: string[];
}

export interface FederationEngineState {
  /** The engine the service is actually running (resolved at boot), not the persisted selection. */
  current: string;
  /** The selection stored in the platform config — what a restart would use absent an env pin. */
  persisted: string;
  /** `$PROVISA_ENGINE` when the deployment pins the engine; the pin outranks `persisted`. */
  env_pinned_engine: string | null;
  /** Current value of every config key any engine declares (connection + execution tuning). */
  config: Record<string, string | number | boolean | null>;
  engines: EngineRegistryEntry[];
  restart_required_note: string;
}

export async function fetchFederationEngine(): Promise<FederationEngineState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/federation-engine`);
  if (!resp.ok) throw new Error(requestFailed("Federation engine fetch", resp.status));
  return resp.json();
}

export async function setFederationEngine(
  body: { engine: string } & Record<string, unknown>,
): Promise<{ success: boolean; updated: string[]; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/federation-engine`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Federation engine update", resp.status));
  return resp.json();
}

// --- Per-org engine lane (REQ-1412) ---
// Which coordinator the ACTING org's queries land on. Separate from the deployment-wide engine
// KIND above: this one is org_settings, so an org administrator owns it in either tenancy mode.

export type OrgEngineMode = "shared" | "isolated" | "external";

/** How an engine kind is reached: a DSN, a coordinator host/port pair (REQ-1418). */
export type OrgEngineAddressing = "url" | "endpoint";

export interface OrgEngineKind {
  key: string;
  label: string;
  description: string;
  addressing: OrgEngineAddressing;
}

export interface OrgEngineState {
  org_id: string;
  mode: OrgEngineMode;
  external_host: string | null;
  external_port: number | null;
  /** REQ-1418: the kind this org's own engine is; null means the deployment's kind. */
  engine_kind: string | null;
  /** Whether a DSN is on file. The value itself is never sent — it carries a warehouse token. */
  external_url_set: boolean;
  /** The kinds an org may operate itself, with the address each one needs. */
  external_kinds: OrgEngineKind[];
  /** False when the deployment cannot resolve a dedicated coordinator, so the option is unusable. */
  isolated_available: boolean;
  /** REQ-1412: false when the org's plan does not include a coordinator of its own. */
  isolated_entitled: boolean;
  /** The engine kind the deployment runs — the default for an org that picks none of its own. */
  engine_name: string;
  // REQ-1510/REQ-1512: on a hosted deployment the lane and the size come from the org's plan, so
  // this surface reports them and offers no control that changes them.
  /** The plan that decides the lane, or null where nothing does (self-hosted and enterprise). */
  plan: string | null;
  plan_derived: boolean;
  /** The machine the plan's lane gives this org, or null on the shared lane. */
  engine_size: OrgEngineSize | null;
  /** The dedicated coordinator's state, or null where the deployment creates none. */
  isolated_engine: IsolatedEngineStatus | null;
}

export interface OrgEngineSize {
  label: string;
  machine_type: string;
  vcpu: number;
  memory_gib: number;
  query_max_memory_gb: number;
}

export interface IsolatedEngineStatus {
  /** "ready", "starting", "stopped", "absent", or docker's own container state. */
  state: string;
}

export async function fetchOrgEngine(): Promise<OrgEngineState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/org-engine`);
  if (!resp.ok) throw new Error(requestFailed("Org engine fetch", resp.status));
  return resp.json();
}

export async function setOrgEngine(body: {
  mode: OrgEngineMode;
  engine_kind?: string | null;
  external_host?: string | null;
  external_port?: number | null;
  external_url?: string | null;
}): Promise<{ success: boolean; mode: OrgEngineMode }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/org-engine`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Org engine update", resp.status));
  return resp.json();
}

// --- MCP server status (REQ-1008) ---

export interface McpTool {
  name: string;
  description: string;
}

export interface McpServerStatus {
  enabled: boolean;
  port: number | null;
  transport: string | null;
  // Scheme-aware connect URL (https when the server serves TLS) and the TLS flag — REQ-1102/1106.
  url?: string | null;
  tls?: boolean;
  // Bundled mcp-proxy bridge command/args for the Claude Desktop config fallback — REQ-1104.
  bridge_command?: string | null;
  bridge_args?: string[];
  stdio_role: string | null;
  max_rows: number;
  tools: McpTool[];
  enable_env_var: string;
  role_env_var: string;
}

export async function fetchMcpServer(): Promise<McpServerStatus> {
  const resp = await fetch(`${API_BASE_RAW}/admin/mcp-server`);
  if (!resp.ok) throw new Error(requestFailed("MCP server status fetch", resp.status));
  return resp.json();
}

// --- MCP catalog search (the "explore" surface, REQ-1008) ---

export interface CatalogSearchColumn {
  name: string;
  type: string;
  description: string;
}

export interface CatalogSearchHit {
  schema: string;
  table: string;
  breadcrumb: string;
  matched_on: { level: string; column: string | null };
  score: number;
  branch: {
    schema: string;
    table: string;
    description: string;
    columns: CatalogSearchColumn[];
    foreign_keys: {
      column: string;
      references_schema: string;
      references_table: string;
      references_column: string;
    }[];
  };
}

export async function searchCatalog(
  query: string,
  role: string,
  k = 5,
): Promise<CatalogSearchHit[]> {
  const resp = await fetch(`${API_BASE_RAW}/admin/mcp/search-catalog`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-provisa-role": role },
    body: JSON.stringify({ query, k }),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Catalog search", resp.status)));
  }
  const json = await resp.json();
  return json.results ?? [];
}

// --- Cache (Redis) + materialize-store settings (REQ-917) ---

export interface CacheStorageState {
  cache: { enabled: boolean; redis_url: string; default_ttl: number | null };
  hot_tables: {
    auto_threshold: number;
    max_rows: number;
    max_bytes: number;
    refresh_interval: number | null;
  };
  warm_tables: {
    query_threshold: number;
    max_rows: number;
    refresh_interval: number | null;
    fs_cache_enabled: boolean;
    fs_cache_directories: string;
    fs_cache_max_sizes: string;
  };
  materialized_views: { default_ttl: number | null };
  materialize: { store_url: string; default_store_url: string };
  restart_required_note: string;
}

export async function fetchCacheStorage(): Promise<CacheStorageState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/cache-storage`);
  if (!resp.ok) throw new Error(requestFailed("Cache/storage fetch", resp.status));
  return resp.json();
}

export async function setCacheStorage(
  body: Partial<{
    cache: Partial<CacheStorageState["cache"]>;
    hot_tables: Partial<CacheStorageState["hot_tables"]>;
    warm_tables: Partial<CacheStorageState["warm_tables"]>;
    materialized_views: Partial<CacheStorageState["materialized_views"]>;
    materialize: { store_url: string };
  }>,
): Promise<{ success: boolean; updated: string[]; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/cache-storage`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Cache/storage update", resp.status));
  return resp.json();
}

// --- Encryption key management (REQ-918) ---

export interface EncryptionProviderField {
  config_key: string;
  label: string;
  type: "string";
  required: boolean;
  secret?: boolean;
  placeholder?: string;
}

export interface EncryptionProvider {
  key: string;
  label: string;
  description: string;
  /** False for providers whose runtime hasn't landed yet — shown but not selectable. */
  available: boolean;
  config_fields: EncryptionProviderField[];
}

export interface EncryptionState {
  provider: string;
  key_id: string | null;
  key_present: boolean | null;
  providers: EncryptionProvider[];
  /** Per-provider persisted config (keyed by provider key). */
  config: Record<string, Record<string, unknown>>;
  restart_required_note: string;
}

export async function fetchEncryption(): Promise<EncryptionState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/encryption`);
  if (!resp.ok) throw new Error(requestFailed("Encryption fetch", resp.status));
  return resp.json();
}

export async function setEncryption(body: {
  provider: string;
  key_id?: string | null;
  config?: Record<string, unknown>;
}): Promise<{ success: boolean; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/encryption`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Encryption update", resp.status));
  return resp.json();
}

export async function generateEncryptionKey(body: {
  key_id?: string | null;
}): Promise<{ stored: boolean; key_id: string; key_b64: string | null; env_var: string | null }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/encryption/generate-key`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Key generation", resp.status));
  return resp.json();
}

// --- Auth provider config (REQ-919) ---

export interface AuthProviderField {
  config_key: string;
  label: string;
  type: "string";
  required: boolean;
  secret?: boolean;
  placeholder?: string;
}

export interface AuthProviderMeta {
  key: string;
  label: string;
  description: string;
  config_fields: AuthProviderField[];
}

export interface AuthConfigState {
  provider: string;
  providers: AuthProviderMeta[];
  config: Record<string, Record<string, unknown>>;
  common: {
    default_role: string;
    assignments_source: string;
    trust_upstream: boolean;
    allow_simple_auth: boolean;
  };
  restart_required_note: string;
}

export async function fetchAuthConfig(): Promise<AuthConfigState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/auth`);
  if (!resp.ok) throw new Error(requestFailed("Auth config fetch", resp.status));
  return resp.json();
}

export async function setAuthConfig(body: {
  provider: string;
  config?: Record<string, unknown>;
  common?: Partial<AuthConfigState["common"]>;
}): Promise<{ success: boolean; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/auth`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Auth config update", resp.status));
  return resp.json();
}

export async function updateSettings(
  settings: Partial<PlatformSettings>,
): Promise<{ success: boolean; updated: string[]; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/settings`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(settings),
  });
  if (!resp.ok) throw new Error(requestFailed("Settings update", resp.status));
  const data = await resp.json();
  // A rejected value (an unknown naming convention, an engine setting that fails validation) comes
  // back 200 with success:false and a message instead of `updated`, so reading `updated.length`
  // here threw a TypeError and the reason the server gave never reached the operator.
  if (!data.success) throw new Error(serverMessage(data, "Settings update rejected"));
  return data;
}

export async function setDomainPolicy(body: {
  use_domains: boolean | null;
  default_domain: string;
}): Promise<{ success: boolean; use_domains: boolean | null }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/domain-policy`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Domain policy update", resp.status)));
  }
  return resp.json();
}

// --- Views ---

// --- Query compilation and submission ---

export interface CompileResult {
  sql: string;
  semantic_sql: string;
  engine_sql: string | null;
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
  statsEnabled: boolean = false,
): Promise<{
  columns: string[];
  rows: Record<string, unknown>[];
  error?: string;
  provisa_stats?: unknown;
}> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Accept: "application/json",
    };
    if (statsEnabled) headers["X-Provisa-Stats"] = "true";
    const resp = await fetch(`${API_BASE_RAW}/data/sql`, {
      method: "POST",
      headers,
      body: JSON.stringify({ sql: sqlText, role }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      return { columns: [], rows: [], error: text };
    }
    const json = await resp.json();
    const rows: Record<string, unknown>[] = json?.data?.sql ?? [];
    // REQ-1436: the projection comes from the response, not from the first row. Read off a row, an
    // empty result has no columns, so a filter matching nothing left the grid with no header row —
    // and therefore no filter input to clear it with. /data/sql returns `columns` on every JSON
    // response, empty result included.
    const columns: string[] = json.columns;
    return { columns, rows, provisa_stats: json?.provisa_stats };
  } catch (e) {
    return { columns: [], rows: [], error: e instanceof Error ? e.message : String(e) };
  }
}

// REQ-1519: the plan the ONE pipeline would have executed, described by the target's own EXPLAIN.
export interface ExplainNodeDto {
  op: string;
  detail: Record<string, string>;
  rows: number | null;
  cost: number | null;
  actual_ms: number | null;
  children: ExplainNodeDto[];
}

export interface ExplainResponse {
  route: string;
  route_reason: string;
  dialect: string;
  analyzed: boolean;
  sources: string[];
  optimizations: string[];
  sql: string;
  plan: ExplainNodeDto[];
  mermaid: string;
}

export async function explainSql(
  sqlText: string,
  role: string = "admin",
  analyze: boolean = false,
): Promise<ExplainResponse> {
  const resp = await fetch(`${API_BASE_RAW}/data/sql/explain`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ sql: sqlText, role, analyze }),
  });
  if (!resp.ok) throw new Error(await resp.text());
  return resp.json();
}

export async function nlToSql(
  question: string,
  role: string = "admin",
  strict: boolean = true,
): Promise<{ sql: string; cypher?: string; attempts: number; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/nl-to-sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, role, strict }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      return { sql: "", attempts: 0, error: text };
    }
    const result = await resp.json();
    if (result.sql) {
      const { format } = await import("sql-formatter");
      result.sql = format(result.sql, { language: "trino", tabWidth: 2, keywordCase: "upper" });
    }
    return result;
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
  usedMemoryBytes: number | null;
  maxMemoryBytes: number | null;
  evictedKeys: number | null;
  expiredKeys: number | null;
  connectedClients: number | null;
  opsPerSec: number | null;
}

export interface CacheTableStat {
  tableId: number;
  cachedEntries: number;
}

export interface HotTableStat {
  tableName: string;
  catalog: string;
  schemaName: string;
  rowCount: number;
  // What is being kept for this table: a registered promotion candidate with nothing mirrored yet
  // ("hot_candidate"), a mirrored hot copy ("hot"), or an Iceberg warm copy ("warm").
  kind: "hot_candidate" | "hot" | "warm";
}

export interface MaterializeStoreInfo {
  engineName: string;
  storeRef: string | null; // null when no materialization store is configured yet
  mvCount: number;
  instanceLocalStore: boolean; // resolved store is a local file (per-instance copy) vs shared
}

export interface ProtocolHealth {
  name: string;
  status: "running" | "down" | "disabled";
  port: number | null;
}

export interface SystemHealth {
  engineConnected: boolean;
  engineWorkerCount: number;
  engineActiveWorkers: number;
  metadataPoolSize: number;
  metadataPoolFree: number;
  metadataDialect: string;
  cacheMode: "disabled" | "embedded" | "server";
  cacheConnected: boolean;
  protocols: ProtocolHealth[];
  mvRefreshLoopRunning: boolean;
}

// --- Admin: Scheduled Tasks ---

export interface ScheduledTask {
  id: string;
  name: string;
  cronExpression: string;
  webhookUrl: string | null;
  kind: string;
  sql: string | null;
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
  if (!res.ok) throw new Error(requestFailed("fetch users", res.status));
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
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("create user", res.status)));
  }
  return res.json();
}

export async function deleteLocalUser(userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(requestFailed("delete user", res.status));
}

export interface UserAssignment {
  id: number;
  role_id: string;
  domain_id: string;
  created_at: string;
}

export async function fetchUserAssignments(userId: string): Promise<UserAssignment[]> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments`);
  if (!res.ok) throw new Error(requestFailed("fetch assignments", res.status));
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
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("add assignment", res.status)));
  }
  return res.json();
}

export async function removeUserAssignment(userId: string, assignmentId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments/${assignmentId}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error(requestFailed("remove assignment", res.status));
}

export interface OrgInvite {
  token: string;
  org_id: string;
  org_name: string;
  role_id: string | null;
  // REQ-1287: the address the invitation was sent to, absent on a shareable link.
  email: string | null;
  created_by: string;
  expires_at: string;
  used_at: string | null;
  used_by: string | null;
  // REQ-1310: only on the creation response — what happened to the message. "not_addressed",
  // "saas_only", "sent", or "failed: <reason>".
  delivery?: string;
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
  if (!res.ok) throw new Error(requestFailed("fetchInvites", res.status));
  return res.json();
}

export async function createInvite(
  orgId: string,
  roleId?: string,
  expiresInDays = 7,
  // REQ-1287: addressing the invite to an email is what lets the invitee be told they have a
  // pending invitation on first sign-in. Omit for a shareable link.
  email?: string,
): Promise<OrgInvite> {
  const res = await fetch(`${API_BASE}/admin/invites`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      org_id: orgId,
      role_id: roleId ?? null,
      expires_in_days: expiresInDays,
      email: email ?? null,
    }),
  });
  if (!res.ok) throw new Error(requestFailed("createInvite", res.status));
  return res.json();
}

export async function revokeInvite(token: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/invites/${token}`, { method: "DELETE" });
  if (!res.ok) throw new Error(requestFailed("revokeInvite", res.status));
}

export async function fetchInviteInfo(token: string): Promise<InviteInfo> {
  const res = await fetch(`/auth/invite/${token}`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("fetchInviteInfo", res.status)));
  }
  return res.json();
}

// Redeem an invite for the CURRENT bearer-authenticated user (Firebase/OIDC). The global
// authFetch wrapper attaches the just-stored provisa_token as the bearer, so no header is set here.
export interface PendingInvite {
  token: string;
  org_id: string;
  org_name: string;
  role_id: string | null;
  expires_at: string;
}

/**
 * REQ-1287: invitations addressed to the signed-in user's email that are still open. Onboarding
 * uses this to answer "do you have an invitation?" without the user needing to already hold the
 * token — an invited person who arrives via the front door otherwise looks like a stranger.
 */
export async function fetchMyInvites(): Promise<PendingInvite[]> {
  const res = await fetch("/auth/my-invites");
  if (!res.ok) throw new Error(requestFailed("my-invites", res.status));
  const data = await res.json();
  return data.invites;
}

export async function redeemInvite(
  token: string,
): Promise<{ user_id: string; org_id: string; role_id: string }> {
  const res = await fetch(`/auth/redeem-invite`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("Redeem invite", res.status)));
  }
  return res.json();
}

/** REQ-1568: an org whose auto-join rule matches the signed-in address, offered as a choice. */
export interface AutoJoinOffer {
  org_id: string;
  org_name: string;
  role_id: string;
}

/**
 * REQ-1568: the orgs claiming this address that were NOT joined at sign-in.
 *
 * Empty in the ordinary case — a single claim is joined before the page loads. A list here means
 * several orgs matched and the server declined to pick one, so the question comes to the person.
 */
export async function fetchAutoJoinOffers(): Promise<AutoJoinOffer[]> {
  const res = await fetch("/auth/auto-join-offers");
  if (!res.ok) throw new Error(requestFailed("auto-join-offers", res.status));
  const data = await res.json();
  return data.offers;
}

/** Join the chosen org. The server records the opt-out for every claim passed over. */
export async function acceptAutoJoin(orgId: string): Promise<{ org_id: string; role_id: string }> {
  const res = await fetch("/auth/auto-join", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("auto-join", res.status)));
  }
  return res.json();
}

/** Turn down every claim, so the page can go on offering org creation. */
export async function declineAutoJoin(): Promise<string[]> {
  const res = await fetch("/auth/auto-join/decline", { method: "POST" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("auto-join decline", res.status)));
  }
  return (await res.json()).declined;
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
    throw new Error(serverMessage(data, requestFailed("reload-catalog", res.status)));
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
    throw new Error(serverMessage(data, requestFailed("restart", res.status)));
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
    throw new Error(serverMessage(data, requestFailed("recompute", res.status)));
  }
  return res.json();
}

export async function submitNlQuery(
  q: string,
  role: string,
  strict: boolean = false,
): Promise<{ job_id: string }> {
  const res = await fetch(`${API_BASE}/query/nl`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ q, role, strict }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(serverMessage(data, data.error || requestFailed("NL submit", res.status)));
  }
  return res.json();
}

export interface NlBranchEvent {
  target: "sql" | "graphql" | "cypher";
  query: string | null;
  result: unknown | null;
  error: string | null;
}

export function streamNlResult(
  jobId: string,
  onBranch: (event: NlBranchEvent) => void,
  onDone: (state: string) => void,
  onError: (msg: string) => void,
): () => void {
  // REQ-1349: read the SSE stream with fetch, not EventSource. EventSource cannot carry headers
  // and is not routed through the window.fetch interceptor (REQ-1267/REQ-1317) that attaches the
  // bearer token and X-Org-Provisa, so on an authenticated deployment every NL stream answered 401
  // and the results never arrived.
  const controller = new AbortController();

  const dispatch = (event: string, data: string) => {
    if (event === "branch") {
      onBranch(JSON.parse(data) as NlBranchEvent);
    } else if (event === "done") {
      const payload = JSON.parse(data) as { state: string };
      onDone(payload.state);
    } else if (event === "timeout") {
      onError("NL query timed out");
    }
  };

  void (async () => {
    let res: Response;
    try {
      res = await fetch(`${API_BASE}/query/nl/${jobId}/stream`, { signal: controller.signal });
    } catch (err) {
      if (!controller.signal.aborted) onError(String(err));
      return;
    }
    if (!res.ok || !res.body) {
      onError(`NL stream failed: ${res.status}`);
      return;
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        // SSE frames are separated by a blank line; a partial trailing frame stays in the buffer.
        const frames = buffer.split("\n\n");
        buffer = frames.pop() ?? "";
        for (const frame of frames) {
          let event = "message";
          const dataLines: string[] = [];
          for (const line of frame.split("\n")) {
            if (line.startsWith("event:")) event = line.slice(6).trim();
            else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
          }
          if (dataLines.length > 0) dispatch(event, dataLines.join("\n"));
        }
      }
    } catch (err) {
      // A read or parse failure ends the stream — surface it rather than leaving the caller
      // waiting on a "done" that will never arrive.
      if (!controller.signal.aborted) onError(String(err));
    }
  })();

  return () => controller.abort();
}

// --- Apache Ossie semantic interchange (REQ-1316) ---

/** The live Ossie export endpoint path (shown copyable in the Model area UI). */
export const OSSIE_ENDPOINT_PATH = "/admin/ossie";

export interface OssieColumnProposal {
  name: string;
  datatype: string | null;
  description: string | null;
  is_primary_key: boolean;
}

export interface OssieTableProposal {
  name: string;
  table_name: string;
  schema_name: string;
  source_id: string;
  description: string | null;
  columns: OssieColumnProposal[];
  primary_key: string[];
  unique_keys: string[][];
  modeling_role?: string | null;
  modeling_history?: string | null;
}

export interface OssieRelationshipProposal {
  name: string;
  from: string;
  to: string;
  from_columns: string[];
  to_columns: string[];
}

export interface OssieMetricProposal {
  name: string;
  expression: string;
  datatype: string | null;
  description: string | null;
  ai_context: string | null;
}

/** Registration PROPOSALS parsed from a posted Ossie document — nothing is
 * registered server-side; the review screen applies checked items through the
 * existing registration mutations (imports never bypass registration review). */
export interface OssieImportProposals {
  model_name: string;
  tables: OssieTableProposal[];
  relationships: OssieRelationshipProposal[];
  metrics: OssieMetricProposal[];
}

/** GET the canonical live Ossie YAML document. */
export async function fetchOssieYaml(): Promise<string> {
  const resp = await fetch(`${API_BASE}${OSSIE_ENDPOINT_PATH}`);
  if (!resp.ok) throw new Error(requestFailed("Ossie export", resp.status));
  return resp.text();
}

/** POST a raw Ossie YAML/JSON document; returns registration proposals. */
export async function importOssie(body: string): Promise<OssieImportProposals> {
  const resp = await fetch(`${API_BASE}${OSSIE_ENDPOINT_PATH}/import`, {
    method: "POST",
    headers: { "Content-Type": "application/yaml" },
    body,
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Ossie import", resp.status)));
  }
  return resp.json();
}
