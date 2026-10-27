// Copyright (c) 2026 Kenneth Stott
// Canary: 3b91cb86-e709-4766-8d0d-cb129f169966
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Role, RoleAssignment, OrgMembership, DerivedRole } from "../types/auth";
import type { ViewerRequest } from "../components/nativeParams";
import type { Paging, PagingKind } from "../types/admin";
import { ORG_HEADER } from "../lib/authFetch";
import { normalizeRole, type RawRole } from "../lib/roles";
import type { ApprovedJoin } from "../components/canvas/joinGraph";
import { serverMessage, requestFailed, type ServerMessageShape } from "../i18n/serverMessage";
import type { MaskEntry } from "../lib/masks";
import type { ResultRedirect } from "../types/resultRedirect";
import { columnKeys, rowByKeys } from "../lib/columnKeys";

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
  control_plane_id: string; // REQ-2008: browser state is saved per control plane and org
  org_memberships: OrgMembership[];
  assignments: RoleAssignment[];
  // REQ-1528: rights held by OWNING the environment the request was served by.
  env_capabilities: string[];
  // REQ-2057: the derived roles the caller holds, named like any role.
  derived_roles: DerivedRole[];
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

// REQ-1931: called from the sandbox exit modal so its "deleted when you log out" claim is true.
// Server refuses (409) unless the sandbox org is the caller's only membership.
export async function deleteSandboxAccount(): Promise<void> {
  const res = await fetch("/auth/sandbox-account", { method: "DELETE" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("deleteSandboxAccount", res.status)));
  }
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
  if (!res.ok) throw httpError("claim platform admin", res.status);
  const data = await res.json();
  return data.claimed === true;
}

// REQ-1288: is the sole platform-admin slot still unclaimed? The login page asks before the user
// picks a provider, so that whoever signs in first is told they are about to become the platform
// admin rather than discovering it afterwards. Unauthenticated, like /auth/provider-type.
export async function fetchBootstrapStatus(): Promise<boolean> {
  const res = await fetch("/auth/bootstrap-status");
  if (!res.ok) throw httpError("read bootstrap status", res.status);
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
  // REQ-1066: the org's subscription tier, which tier-gated features read; null when none is set.
  tier: OrgTier | null;
}

export type OrgTier = "free" | "standard" | "premium";
export const ORG_TIERS: OrgTier[] = ["free", "standard", "premium"];

/** Set an org's subscription tier (null clears it): a platform administrator's act (REQ-1066). */
export async function setOrgTier(orgId: string, tier: OrgTier | null): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/tier`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ tier }),
  });
  if (!res.ok) throw httpError("set org tier", res.status);
}

export async function fetchOrgs(): Promise<Org[]> {
  const res = await fetch(`${API_BASE}/admin/orgs`);
  if (!res.ok) throw httpError("fetchOrgs", res.status);
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
  if (!res.ok) throw httpError("fetchOrgSettings", res.status);
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
  if (!res.ok) throw httpError("fetchOrgStatus", res.status);
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
  // REQ-1503: the one environment the token addresses -- the one it was issued into.
  env_name: string;
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
  if (!res.ok) throw httpError("fetchOrgMembers", res.status);
  return res.json();
}

export async function addOrgMember(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId }),
  });
  if (!res.ok) throw httpError("addOrgMember", res.status);
}

// REQ-2076: what is suspended in an org for naming a role that was not held, and the events that
// suspended it (the org's admin trail). An event names its recent statements by audit row id; their
// text is read with fetchAuditedStatement, under the view_governance right.
export interface SuspendedToken {
  token_hash: string;
  prefix: string;
  name: string;
  user_id: string;
  suspended_at: string;
  suspended_reason: string | null;
}

export interface SuspendedPrincipal {
  user_id: string;
  token_access_suspended_at: string;
  token_access_suspended_reason: string | null;
}

export interface RoleProbeEvent {
  id: number;
  subject_id: string;
  created_at: string;
  detail: {
    severity: string;
    principal: string | null;
    credential: string | null;
    credential_id: string | null;
    client: string | null;
    transport: string;
    endpoint: string;
    requested_role: string;
    held_roles: string[];
    at: string;
    suspended: "token" | "principal" | null;
    recent_statement_ids: number[];
  };
}

export interface OrgSuspensions {
  tokens: SuspendedToken[];
  principals: SuspendedPrincipal[];
  events: RoleProbeEvent[];
}

export async function fetchOrgSuspensions(orgId: string): Promise<OrgSuspensions> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/suspensions`);
  if (!res.ok) throw httpError("fetchOrgSuspensions", res.status);
  return res.json();
}

export async function liftOrgSuspension(
  orgId: string,
  target: { token_hash: string } | { user_id: string },
): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/suspensions/lift`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(target),
  });
  if (!res.ok) throw httpError("liftOrgSuspension", res.status);
}

export async function fetchAuditedStatement(auditId: number): Promise<string> {
  const res = await fetch(`${API_BASE}/admin/audit/queries/${auditId}/text`);
  if (!res.ok) throw httpError("fetchAuditedStatement", res.status);
  return (await res.json()).query_text;
}

export async function removeOrgMember(orgId: string, userId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/members/${userId}`, {
    method: "DELETE",
  });
  if (!res.ok) throw httpError("removeOrgMember", res.status);
}

export async function fetchOrgRoles(orgId: string): Promise<Role[]> {
  const res = await fetch(`${API_BASE}/admin/roles`, {
    headers: { [ORG_HEADER]: orgId },
  });
  if (!res.ok) throw httpError("fetchOrgRoles", res.status);
  const rows: RawRole[] = await res.json();
  return rows.map(normalizeRole);
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
  if (!res.ok) throw httpError("createOrgRole", res.status);
  return res.json();
}

export async function deleteOrgRole(orgId: string, roleId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/roles/${roleId}`, {
    method: "DELETE",
    headers: { [ORG_HEADER]: orgId },
  });
  if (!res.ok) throw httpError("deleteOrgRole", res.status);
}

/** A preview the server did not answer with rows, with the code it gave: the viewer tells a
 * copy that is still being built (`data.replica_building`) from a refusal. */
export class PreviewRefused extends Error {
  code: string | null;
  constructor(message: string, code: string | null) {
    super(message);
    this.code = code;
  }
}

/** One page of a registered table as the table viewer shows it, read as the caller — the read a
 * DRAFT takes (REQ-1921). The body is what the viewer chose; the server writes the statement.
 * One row more than the page comes back when a next page exists. */
export async function previewTable(
  tableId: number,
  role: string,
  request: ViewerRequest,
): Promise<{
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  draft: boolean;
}> {
  const resp = await fetch(`${API_BASE}/admin/tables/${tableId}/preview`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Provisa-Role": role },
    body: JSON.stringify(request),
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new PreviewRefused(
      serverMessage(body, requestFailed("previewTable", resp.status)),
      typeof body?.code === "string" ? body.code : null,
    );
  }
  return resp.json();
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
  // REQ-1954: the heading the source lists the table under (a ServiceNow table's theme); a
  // table with one is shown by its name and its label (``comment``).
  group?: string | null;
  // REQ-1924: its source says nothing of what its rows hold; the steward declares its columns.
  columnsDeclared?: boolean;
  // REQ-318: what would read the table page by page once registered, the paging its source
  // suggests, and (a connection table) the operator's row bound.
  pagingKind?: PagingKind | null;
  pagination?: Paging | null;
  pagingCeilingRows?: number | null;
}

export interface ColumnMetadata {
  name: string;
  dataType: string;
  comment: string | null;
  nativeFilterType: string | null;
  /** Whether the source requires a value for this parameter; null where it is not recorded. */
  nativeFilterRequired?: boolean | null;
  isPrimaryKey: boolean;
  /** REQ-1982: where a document source's nested field is read from; null for a top field. */
  path?: string | null;
}

export async function fetchSdl(roleId: string): Promise<string> {
  const resp = await fetch(`${API_BASE}/data/sdl`, {
    // X-Provisa-Role is the role header the server validates; X-Role names no role.
    headers: { "X-Provisa-Role": roleId },
  });
  if (!resp.ok) throw httpError("SDL fetch", resp.status);
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
  if (!resp.ok) throw httpError("Discovery", resp.status);
  return resp.json();
}

export async function fetchCandidates(): Promise<unknown[]> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates`);
  if (!resp.ok) throw httpError("Fetch candidates", resp.status);
  return resp.json();
}

export async function acceptCandidate(id: number, name?: string): Promise<unknown> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/accept`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: name ?? null }),
  });
  if (!resp.ok) throw httpError("Accept", resp.status);
  return resp.json();
}

export async function fetchRejectedCount(): Promise<number> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/rejected/count`);
  if (!resp.ok) throw httpError("Fetch rejected count", resp.status);
  const data = await resp.json();
  return data.count;
}

export async function clearRejectedCandidates(): Promise<{ deleted: number }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/rejected`, {
    method: "DELETE",
  });
  if (!resp.ok) throw httpError("Clear rejections", resp.status);
  return resp.json();
}

export async function rejectCandidate(id: number, reason: string): Promise<void> {
  const resp = await fetch(`${API_BASE_RAW}/admin/discover/candidates/${id}/reject`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason }),
  });
  if (!resp.ok) throw httpError("Reject", resp.status);
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
    // REQ-1767: kafka discovery hints.
    topic?: string;
    value_format?: string;
    schema_registry_url?: string;
    // REQ-150: kafka SchemaSource.SAMPLE mode — used only when schema_registry_url is unset.
    bootstrap_servers?: string;
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
  if (!resp.ok) throw httpError("IR types fetch", resp.status);
  return resp.json();
}

// --- Config ---

export async function downloadConfig(): Promise<string> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config`);
  if (!resp.ok) throw httpError("Config download", resp.status);
  return resp.text();
}

// REQ-1919: the sections of the model a configuration file can carry, in the order a file writes them.
export const MODEL_SECTIONS = [
  "stores",
  "regions",
  "sources",
  "domains",
  "roles",
  "data_products",
  "glossary_terms",
  "tables",
  "relationships",
  "metrics",
  "tags",
  "tag_assignments",
  "rls_rules",
  "functions",
  "webhooks",
  "scheduled_triggers",
] as const;

// REQ-1919: the model as it stands, written as a configuration file — the whole model, or only the
// chosen sections. Applied to an empty model store it builds the same model.
export async function exportModelConfig(sections: string[]): Promise<string> {
  const query = sections.length > 0 ? `?sections=${encodeURIComponent(sections.join(","))}` : "";
  const resp = await fetch(`${API_BASE_RAW}/admin/config/live${query}`);
  if (!resp.ok) throw httpError("Model export", resp.status);
  return resp.text();
}

// Both sides of the config diff — original (on-disk file) and current (live state) — normalized
// identically server-side so the diff shows only genuine changes, not section/key reordering.
export async function fetchConfigDiff(): Promise<{ original: string; current: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config/diff`);
  if (!resp.ok) throw httpError("Config diff", resp.status);
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
  if (!resp.ok) throw httpError("Config patch", resp.status);
  return resp.text();
}

export async function uploadConfig(yaml: string): Promise<{ success: boolean; message: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/x-yaml" },
    body: yaml,
  });
  if (!resp.ok) throw httpError("Config upload", resp.status);
  return resp.json();
}

/**
 * A failed HTTP request as an Error carrying its `status`, so a surface can tell "platform
 * administrator required" (403) from a failure without parsing the message.
 */
export function httpError(op: string, status: number | string): Error {
  return Object.assign(new Error(requestFailed(op, status)), { status: Number(status) });
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
    // True only for a platform administrator: the deployment-wide platform surfaces gate on this,
    // not on `platform_settings`, which a single-tenant org administrator also holds.
    deployment_settings?: boolean;
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
    max_rows: number;
  };
  cdc?: {
    consumer_group_id: string;
  };
  materialize?: {
    store_url: string;
  };
  // REQ-1905: `request_timeouts` holds one value per transport, null where the transport uses the
  // default `request_timeout`.
  limits?: {
    default_row_limit: number;
    request_timeout: number;
    request_timeouts: Record<string, number | null>;
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
  if (!resp.ok) {
    // `status` lets a surface tell "platform administrator required" (403) from a failure.
    throw Object.assign(new Error(requestFailed("Settings fetch", resp.status)), {
      status: resp.status,
    });
  }
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
  /** REQ-1575: write-only — the GET carries no value, only `secret_set`'s bit. */
  secret?: boolean;
}

export interface EngineRegistryEntry {
  key: string;
  label: string;
  /** The hosted services that are this engine (e.g. managed PostgreSQL), shown in the picker. */
  managed?: string;
  description: string;
  config_fields: EngineConfigField[];
  /** Types this engine reads LIVE via a live-attach connector (queried in place, always fresh). */
  reachable_source_types?: string[];
  /** Every type configurable on this engine: live-attach ∪ landed-replica reach (REQ-947). */
  live_source_types?: string[];
  /** What this install could not load for the engine (its driver library), or null: an
   *  unavailable engine reaches nothing and cannot be selected. */
  unavailable?: string | null;
}

export interface FederationEngineState {
  /** The engine the service is actually running (resolved at boot), not the persisted selection. */
  current: string;
  /** The selection stored in the platform config — what a restart would use absent an env pin. */
  persisted: string;
  /** `$PROVISA_ENGINE` when the deployment pins the engine; the pin outranks `persisted`. */
  env_pinned_engine: string | null;
  /** Current value of every config key any engine declares (connection + execution tuning).
   *  REQ-1575: minus every key declared `secret`, and with DSN passwords stripped. */
  config: Record<string, string | number | boolean | null>;
  /** REQ-1575: per secret config key, whether a value is stored. Never the value. */
  secret_set: Record<string, boolean>;
  engines: EngineRegistryEntry[];
  restart_required_note: string;
}

/** What the Sources type list is gated and annotated by (REQ-947): the engine serving the org,
 *  and what each engine reads live or from a replica. Readable by whoever registers sources --
 *  none of the engine settings, which are the platform's. */
export interface SourceReach {
  engine: string;
  engines: Pick<
    EngineRegistryEntry,
    "key" | "label" | "live_source_types" | "reachable_source_types" | "unavailable"
  >[];
}

export async function fetchSourceReach(): Promise<SourceReach> {
  const resp = await fetch(`${API_BASE_RAW}/admin/source-kinds`);
  if (!resp.ok) throw httpError("Source reach fetch", resp.status);
  return resp.json();
}

export async function fetchFederationEngine(): Promise<FederationEngineState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/federation-engine`);
  if (!resp.ok) throw httpError("Federation engine fetch", resp.status);
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
  if (!resp.ok) throw httpError("Federation engine update", resp.status);
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
  if (!resp.ok) throw httpError("Org engine fetch", resp.status);
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
  if (!resp.ok) throw httpError("Org engine update", resp.status);
  return resp.json();
}

// --- The org's own encryption key (REQ-1574) ---

/** What may be said about an org's key. No field here carries key material, by design. */
export interface OrgEncryptionState {
  org_id: string;
  configured: boolean;
  key_id?: string;
  /** First 16 hex of SHA-256 over the key: enough to recognise it, never enough to be it. */
  fingerprint?: string;
  supplied?: boolean;
  created_at?: string | null;
  created_by?: string | null;
  retired_count?: number;
}

export async function fetchOrgEncryption(): Promise<OrgEncryptionState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/org-encryption`);
  if (!resp.ok) throw httpError("Org encryption fetch", resp.status);
  return resp.json();
}

/**
 * Set or rotate the key. `key_b64` null means "generate one", in which case the key exists only in
 * the ring — the response carries a fingerprint and never the key, on this call as on every other.
 */
export async function setOrgEncryption(body: { key_b64: string | null }): Promise<{
  key_id: string;
  fingerprint: string;
  supplied: boolean;
  created_at: string | null;
  created_by: string | null;
  retired_count: number;
}> {
  const resp = await fetch(`${API_BASE_RAW}/admin/org-encryption`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Org encryption update", resp.status)));
  }
  return resp.json();
}

// --- Central redaction rules (REQ-2087) ---

export interface RedactionRule {
  name: string;
  pattern: string | null;
  reference: string | null;
  source?: "builtin" | "deployment" | "org";
}

export interface RedactionState {
  builtins: RedactionRule[];
  deployment: RedactionRule[];
  org: RedactionRule[];
  /** Names only: the org's secrets redacted by the automatic rule. Never a value. */
  automatic: string[];
  resolved_at_use: number;
  unresolved: string[];
  can_edit_deployment: boolean;
}

export async function fetchRedaction(): Promise<RedactionState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/redaction`);
  if (!resp.ok) throw httpError("Redaction rules fetch", resp.status);
  return resp.json();
}

/** Replace the org's own rules (`scope` "org") or the deployment's (`scope` "deployment"). */
export async function saveRedactionRules(
  scope: "org" | "deployment",
  rules: RedactionRule[],
): Promise<RedactionState> {
  const path = scope === "org" ? "/admin/redaction" : "/admin/redaction/deployment";
  const resp = await fetch(`${API_BASE_RAW}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      rules: rules.map(({ name, pattern, reference }) => ({ name, pattern, reference })),
    }),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Redaction rules update", resp.status)));
  }
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
  if (!resp.ok) throw httpError("MCP server status fetch", resp.status);
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
  // REQ-826: when a busy table is replicated (Default threshold, window, size ceiling).
  replication: { hot_threshold: number; hot_interval: number; hot_max_rows: number };
  materialized_views: { default_ttl: number | null };
  materialize: { store_url: string; default_store_url: string };
  restart_required_note: string;
}

export async function fetchCacheStorage(): Promise<CacheStorageState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/cache-storage`);
  if (!resp.ok) throw httpError("Cache/storage fetch", resp.status);
  return resp.json();
}

export async function setCacheStorage(
  body: Partial<{
    cache: Partial<CacheStorageState["cache"]>;
    hot_tables: Partial<CacheStorageState["hot_tables"]>;
    replication: Partial<CacheStorageState["replication"]>;
    materialized_views: Partial<CacheStorageState["materialized_views"]>;
    materialize: { store_url: string };
  }>,
): Promise<{ success: boolean; updated: string[]; restart_required: boolean }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/cache-storage`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw httpError("Cache/storage update", resp.status);
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
  /** The fingerprint of the master key in use: the one this deployment's secrets are recorded
   *  under, else the one this host holds; null when there is none. A deployment with one
   *  generates no other (REQ-918). */
  key_fingerprint: string | null;
  providers: EncryptionProvider[];
  /** Per-provider persisted config (keyed by provider key), minus every secret field (REQ-1575). */
  config: Record<string, Record<string, unknown>>;
  /** REQ-1575: per provider, per secret field, whether a value is stored. Never the value. */
  secret_set: Record<string, Record<string, boolean>>;
  restart_required_note: string;
}

export async function fetchEncryption(): Promise<EncryptionState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/encryption`);
  if (!resp.ok) throw httpError("Encryption fetch", resp.status);
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
  if (!resp.ok) throw httpError("Encryption update", resp.status);
  return resp.json();
}

// REQ-1574: the key is generated INTO the OS keychain and is never returned. With no keychain to
// hold it the server refuses (503) rather than putting a master key on the wire. REQ-918: a
// deployment whose secrets are already written under a key refuses too (409) — it provisions a
// key, it does not replace one.
export async function generateEncryptionKey(body: {
  key_id?: string | null;
}): Promise<{ stored: boolean; key_id: string }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/encryption/generate-key`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw httpError("Key generation", resp.status);
  return resp.json();
}

// --- Auth provider config (REQ-919) ---

export interface AuthProviderField {
  config_key: string;
  label: string;
  /** REQ-1265: "boolean" is a true/false setting, shown as a checkbox and saved as a boolean. */
  type: "string" | "boolean";
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
  /** Per-provider config, minus client secrets and signing secrets (REQ-1575). */
  config: Record<string, Record<string, unknown>>;
  /** REQ-1575: per provider, per secret field, whether a value is stored. Never the value. */
  secret_set: Record<string, Record<string, boolean>>;
  common: {
    default_role: string;
    assignments_source: string;
    trust_upstream: boolean;
    allow_simple_auth: boolean;
    /** REQ-1265: whether /auth/register creates local accounts (basic provider). */
    allow_registration: boolean;
  };
  restart_required_note: string;
}

export async function fetchAuthConfig(): Promise<AuthConfigState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/auth`);
  if (!resp.ok) throw httpError("Auth config fetch", resp.status);
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
  if (!resp.ok) throw httpError("Auth config update", resp.status);
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
  if (!resp.ok) {
    // A 400 names the offending field in its error `params`; the error carries them so the surface
    // can show the message on that field.
    const body = await resp.json().catch(() => null);
    throw Object.assign(
      new Error(serverMessage(body, requestFailed("Settings update", resp.status))),
      { code: body?.code, params: body?.params },
    );
  }
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

/** One thing an answer says about itself (REQ-1350), as the server sends it. */
export interface StatementWarning {
  code: string;
  params: Record<string, unknown>;
  message: string;
}

/** The server's redirect handle (``redirect_url``, ``row_count``, …) as the UI holds it. Every
 *  buffered data surface answers a redirected result with one (REQ-1194, REQ-1224). */
export function toResultRedirect(handle: {
  redirect_url: string;
  row_count: number;
  expires_in?: number | null;
  content_type?: string | null;
}): ResultRedirect {
  return {
    url: handle.redirect_url,
    rowCount: handle.row_count,
    expiresIn: handle.expires_in ?? null,
    contentType: handle.content_type ?? null,
  };
}

/** What a refused statement says: in the reader's language by its code and params (serverErrors),
 * else in the server's own words; never the body it came in. */
async function refusal(resp: Response, op: string): Promise<string> {
  const text = await resp.text();
  let body: unknown = null;
  try {
    body = JSON.parse(text);
  } catch {
    // Not JSON (a proxy's page, a bare message): its text is all there is to say.
  }
  return body !== null && typeof body === "object"
    ? serverMessage(body as ServerMessageShape, requestFailed(op, resp.status))
    : text || requestFailed(op, resp.status);
}

function sqlHeaders(role: string, statsEnabled: boolean): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
    // REQ-273: the server trusts only this header to select the acting role — the body's
    // `role` is a fallback for callers with no auth context at all. Without it here, the
    // middleware's default-assigned role silently overrode the SQL Explorer's picker.
    "X-Provisa-Role": role,
  };
  if (statsEnabled) headers["X-Provisa-Stats"] = "true";
  return headers;
}

export async function runSql(
  // A statement, or a statement with the values of its $1…$n placeholders. The values are
  // sent beside the text and bound by the server; they are never written into it.
  statement: string | { sql: string; params: (string | number)[] },
  role: string = "org_admin",
  statsEnabled: boolean = false,
): Promise<{
  columns: string[];
  /** REQ-1937: each column's SQL type, parallel to `columns`; null when the engine reported none. */
  column_types?: string[] | null;
  rows: Record<string, unknown>[];
  /** REQ-1350: what the answer says about itself (a row limit that cut it, …). */
  warnings?: StatementWarning[];
  error?: string;
  provisa_stats?: unknown;
  /**
   * REQ-1194 / REQ-1224: the result was landed in the results store and is there, not in `rows`
   * (which is then empty): where it is and how many rows it holds.
   */
  redirect?: ResultRedirect;
}> {
  try {
    const headers = sqlHeaders(role, statsEnabled);
    const resp = await fetch(`${API_BASE_RAW}/data/sql`, {
      // The acting role travels ONLY in X-Provisa-Role. Under "Role: All" that header is the
      // comma-separated set; a body `role` repeating it is refused as a mismatch (data.role_mismatch,
      // api/acting_role.py), because the server's acting role is one member of the set.
      method: "POST",
      headers,
      body: JSON.stringify(
        typeof statement === "string"
          ? { sql: statement }
          : statement.params.length > 0
            ? statement
            : { sql: statement.sql },
      ),
    });
    if (!resp.ok) {
      return { columns: [], column_types: null, rows: [], error: await refusal(resp, "runSql") };
    }
    const json = await resp.json();
    // A result landed in the results store comes back as a handle to it, not rows.
    if (json?.redirect) {
      const handle = json.redirect;
      return {
        columns: [],
        column_types: null,
        rows: [],
        warnings: json?.extensions?.warnings ?? [],
        redirect: toResultRedirect(handle),
      };
    }
    const rows: Record<string, unknown>[] = json?.data?.sql ?? [];
    // REQ-1436: the projection comes from the response, not from the first row. Read off a row, an
    // empty result has no columns, so a filter matching nothing left the grid with no header row —
    // and therefore no filter input to clear it with. /data/sql returns `columns` on every JSON
    // response, empty result included; an answer without them is not a result, and is said as the
    // server's own words, never handed on as one with no columns.
    if (!Array.isArray(json?.columns)) {
      return {
        columns: [],
        column_types: null,
        rows: [],
        error: serverMessage(json, requestFailed("runSql", resp.status)),
      };
    }
    const columns: string[] = json.columns;
    return {
      columns,
      column_types: json.column_types ?? null,
      rows,
      warnings: json?.extensions?.warnings ?? [],
      provisa_stats: json?.provisa_stats,
    };
  } catch (e) {
    return {
      columns: [],
      column_types: null,
      rows: [],
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

/** One statement's answer in a batch answered one result per statement. */
export interface StatementAnswer {
  statement: string;
  /** Each column's key, unique by position (lib/columnKeys); columnLabel gives its name. */
  columns: string[];
  column_types: string[] | null;
  rows: Record<string, unknown>[];
  warnings: StatementWarning[];
  /** Why the statement failed, as it alone would have been refused; the others still ran. */
  error?: string;
  /** REQ-1194 / REQ-1224: landed in the results store, not in `rows`. */
  redirect?: ResultRedirect;
}

/** The joins the acting role's statements may write: what the SQL canvas draws. */
export async function fetchApprovedJoins(): Promise<ApprovedJoin[]> {
  const resp = await fetch(`${API_BASE_RAW}/data/sql/approved-joins`, {
    headers: { Accept: "application/json" },
  });
  if (!resp.ok) throw new Error(await refusal(resp, "fetchApprovedJoins"));
  return ((await resp.json()) as { joins: ApprovedJoin[] }).joins;
}

/** The statements of a batch, as /data/sql splits it (a `;` in a string, a comment or a
 * dollar-quoted block does not split). Nothing is run. */
export async function splitSql(sql: string): Promise<string[]> {
  const resp = await fetch(`${API_BASE_RAW}/data/sql/statements`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ sql }),
  });
  if (!resp.ok) throw new Error(await refusal(resp, "splitSql"));
  return ((await resp.json()) as { statements: string[] }).statements;
}

interface EachEntry {
  statement: string;
  warnings: StatementWarning[];
  /** Each row's values in column order. */
  data?: unknown[][];
  columns?: string[];
  column_types?: string[] | null;
  redirect?: Parameters<typeof toResultRedirect>[0];
  error?: { status: number; body: unknown };
}

/** Run a batch answered one result per statement, in order: each governed and run on its own,
 * in one request. `error` is the request's own failure (refused before any statement ran). */
export async function runSqlEach(
  sql: string,
  role: string,
  statsEnabled: boolean,
  // REQ-1937: the values of the statement's $1…$n placeholders, bound by the server; none sent
  // when there are none.
  params: (string | number)[] = [],
): Promise<{ answers: StatementAnswer[]; provisa_stats?: unknown; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/sql`, {
      method: "POST",
      headers: sqlHeaders(role, statsEnabled),
      body: JSON.stringify(
        params.length > 0 ? { sql, results: "each", params } : { sql, results: "each" },
      ),
    });
    if (!resp.ok) return { answers: [], error: await refusal(resp, "runSql") };
    const json = (await resp.json()) as { results: EachEntry[]; provisa_stats?: unknown };
    const answers = await Promise.all(
      json.results.map(async (e): Promise<StatementAnswer> => {
        const empty = { statement: e.statement, columns: [], column_types: null, rows: [] };
        if (e.error)
          return {
            ...empty,
            warnings: e.warnings,
            // Said as the same statement's refusal alone is said.
            error: await refusal(
              new Response(JSON.stringify(e.error.body), { status: e.error.status }),
              "runSql",
            ),
          };
        if (e.redirect)
          return { ...empty, warnings: e.warnings, redirect: toResultRedirect(e.redirect) };
        if (!Array.isArray(e.columns) || !Array.isArray(e.data))
          throw new Error(`runSql: an answer for "${e.statement}" holds neither rows nor error`);
        // Keyed by position, so two columns of one name each keep their own value.
        const keys = columnKeys(e.columns);
        return {
          statement: e.statement,
          columns: keys,
          column_types: e.column_types ?? null,
          rows: e.data.map((row) => rowByKeys(keys, row)),
          warnings: e.warnings,
        };
      }),
    );
    return { answers, provisa_stats: json.provisa_stats };
  } catch (e) {
    return { answers: [], error: e instanceof Error ? e.message : String(e) };
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
  // REQ-1322: the metric expansion in semantic terms; null when the statement referenced no
  // metric. `sql` is the physical lowering — it names source catalogs the pipeline refuses on
  // the way back in, so this is the only expansion form that can be pasted into an editor.
  semantic_sql: string | null;
  plan: ExplainNodeDto[];
  mermaid: string;
}

export async function explainSql(
  sqlText: string,
  role: string = "org_admin",
  analyze: boolean = false,
): Promise<ExplainResponse> {
  const resp = await fetch(`${API_BASE_RAW}/data/sql/explain`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      "X-Provisa-Role": role,
    },
    body: JSON.stringify({ sql: sqlText, analyze }),
  });
  if (!resp.ok) throw new Error(await refusal(resp, "explainSql"));
  return resp.json();
}

export async function nlToSql(
  question: string,
  role: string = "org_admin",
  strict: boolean = true,
): Promise<{ sql: string; cypher?: string; attempts: number; error?: string }> {
  try {
    const resp = await fetch(`${API_BASE_RAW}/data/nl-to-sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Provisa-Role": role },
      body: JSON.stringify({ question, strict }),
    });
    if (!resp.ok) {
      return { sql: "", attempts: 0, error: await refusal(resp, "nlToSql") };
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
  /** The table whose version log this view is; null for any other view. */
  versionLogOf: string | null;
  /** Seconds a row its refresh reads may be committed late. */
  allowedLateness: number;
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

/** One replica and its build, as the admin API reports it (REQ-1915). */
export interface ReplicaBuild {
  sourceId: string;
  schemaName: string;
  tableName: string;
  /** REQ-1922: the table's home region; null when none / no regions declared. */
  region: string | null;
  /** idle (built, nothing pending) | requested | building | failed | retired */
  state: string;
  requestedReason: string | null;
  /** engine_statement | stream_batches | store_pipeline | delta | apply_units, of the running or
   * last build */
  method: string | null;
  /** bulk_stream | row_copy */
  loadKind: string | null;
  startedAt: string | null;
  rowsCopied: number | null;
  /** A running build's rate so far. */
  rowsPerSecond: number | null;
  /** A running build waiting out a throttle: who asked it to wait, for how many seconds, and
   * until when (ISO 8601). Null when it is not waiting. */
  wait?: { by: string; seconds: number; until: string } | null;
  /** The rows of the last build that copied the whole table: an estimate of its size for a
   * running build's progress. Null before the first such build. */
  completedRows: number | null;
  /** A running build that lands a part at a time: how many of the source's mailboxes it has
   * read to their end. `first` says the replica had no copy before this build. */
  progress?: { done: number; total: number; unit: string; first: boolean } | null;
  /** The replica is read while its first build is still filling it. */
  partial?: boolean;
  /** null: there is no replica yet. */
  completedAt: string | null;
  /** When its probe last found the source unchanged since that build; null: not since. */
  verifiedAt: string | null;
  nextRefreshAt: string | null;
  /** The last failure, in English; with (REQ-1350) its code and params when the cause is a named one. */
  lastError: string | null;
  lastErrorCode: string | null;
  lastErrorParams: Record<string, unknown> | null;
  /** Builds that have failed in a row since the last one that completed. */
  failedAttempts: number;
  /** When a failed build is tried next (its wait grows with each failure in a row); null unless failed. */
  nextAttemptAt: string | null;
  /** Why a requested build has not started: English text and its code. */
  waitingOn: string | null;
  waitingOnCode: string | null;
  /** REQ-1861: while a change-feed table's listener is down, since when and the server's reason. */
  feedDownSince: string | null;
  feedError: string | null;
  /** REQ-874: the last build of a delta table. `deltaSkipped` is the code for why it was a whole
   * rebuild instead of a delta (a delta.SKIP_* value), or null when a delta was applied;
   * `deltaCursor` is the stored watermark the next delta resumes from. Both null for a non-delta table. */
  deltaSkipped: string | null;
  deltaCursor: unknown | null;
  /** What the last completed build had to say of the copy it made: a code and its particulars
   * (`replication.unreadable_messages`: count, ids, more). Null when it had nothing to say. */
  buildNotes: { code: string; params: Record<string, unknown> | null }[];
  /** REQ-2015: each index the standing landing asked for, and how it stands. */
  indexes: ReplicaIndex[];
}

/** One index a replica's standing landing asked for. A failed one carries its English text and
 * the code the UI words (`replica.index_duplicate_key`, `replica.index_failed`). */
export interface ReplicaIndex {
  name: string;
  columns: string[];
  primary: boolean;
  state: "built" | "pending" | "failed";
  error: string | null;
  errorCode: string | null;
}

export interface ReplicaBuilds {
  builds: ReplicaBuild[];
  /** The last failure to bring replicas in line with the model; null when the last pass worked. */
  convergenceError: string | null;
  convergenceErrorAt: string | null;
}

export interface HotTableStat {
  tableName: string;
  catalog: string;
  schemaName: string;
  rowCount: number;
  // What is being kept for this table: a registered promotion candidate with nothing mirrored yet
  // ("hot_candidate"), a mirrored hot copy ("hot"), a table past its Hot threshold and served
  // from its replica ("replica", REQ-826), or one past its threshold whose replica is still
  // being built and which is read live meanwhile ("replica_building").
  kind: "hot_candidate" | "hot" | "replica" | "replica_building";
  /** REQ-1922: the table's home region; null when none / no regions declared. */
  region: string | null;
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

/** A trigger: when it fires (a cron schedule, or a registered table's change or event), what it
 * runs (a webhook, a SQL statement or a command) and as which role, and how its last run ended. */
export interface ScheduledTask {
  id: string;
  name: string;
  /** "schedule" (cronExpression), or "change" / "event" (onTable, for onEvent). */
  firesOn: "schedule" | "change" | "event";
  cronExpression: string | null;
  onTable: string | null;
  /** change | error | quarantine. */
  onEvent: string | null;
  webhookUrl: string | null;
  kind: "webhook" | "sql" | "command";
  sql: string | null;
  command: string | null;
  role: string | null;
  enabled: boolean;
  lastRunAt: string | null;
  /** succeeded | refused | failed. */
  lastOutcome: string | null;
  lastError: string | null;
  /** What fired the last run ("change:<table>"); null for a schedule. */
  lastFiredBy: string | null;
  /** A schedule's next firing; null for a trigger on a table. */
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
  if (!res.ok) throw httpError("fetch users", res.status);
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

/** What removing a user did: an org administrator removes the person from their org; the holder
 *  of the cross-org right deletes the account everywhere. The server decides, and says which. */
export type UserRemovalScope = "org" | "account";

export async function deleteLocalUser(userId: string): Promise<UserRemovalScope> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}`, { method: "DELETE" });
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(serverMessage(body, requestFailed("delete user", res.status)));
  }
  return ((await res.json()) as { scope: UserRemovalScope }).scope;
}

export interface UserAssignment {
  id: number;
  role_id: string;
  domain_id: string;
  created_at: string;
  /** REQ-2077: a time-limited grant is held until this instant; null is held until removed. */
  expires_at: string | null;
  /** REQ-2077: the last day a request depended on the grant; null when never used. */
  last_used_at: string | null;
}

export async function fetchUserAssignments(userId: string): Promise<UserAssignment[]> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments`);
  if (!res.ok) throw httpError("fetch assignments", res.status);
  return res.json();
}

export async function addUserAssignment(
  userId: string,
  roleId: string,
  domainId: string,
  expiresAt: string | null = null,
): Promise<UserAssignment> {
  const res = await fetch(`${API_BASE}/admin/users/${userId}/assignments`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ role_id: roleId, domain_id: domainId, expires_at: expiresAt }),
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
  if (!res.ok) throw httpError("remove assignment", res.status);
}

/** A grant, mask or row rule that names a derived role or one of its value roles (REQ-2079). */
export interface DerivedRoleUse {
  kind: string;
  name: string;
  via: string[];
}

/** An attribute the org declares as a derived role (REQ-2057, REQ-2078): its description, the
 *  values it has been seen with (each a value role, `<attribute>__<value>`) and what names it. */
export interface DeclaredDerivedRole {
  attribute: string;
  description: string;
  values: string[];
  uses: DerivedRoleUse[];
}

/** The org's derived roles, and whether the list is the org's own or the deployment
 *  configuration's. */
export interface DerivedRoles {
  roles: DeclaredDerivedRole[];
  source: "org" | "deployment";
}

export async function fetchDerivedRoles(): Promise<DerivedRoles> {
  const res = await fetch(`${API_BASE}/admin/derived-roles`);
  if (!res.ok) throw httpError("fetch derived roles", res.status);
  return res.json();
}

async function refusedOrJson(res: Response, what: string): Promise<unknown> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(what, res.status)));
  }
  return res.json();
}

/** Declare a derived role, or change its description. */
export async function saveDerivedRole(attribute: string, description: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/derived-roles/${encodeURIComponent(attribute)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ description }),
  });
  await refusedOrJson(res, "save derived role");
}

/** Stop deriving roles from an attribute. */
export async function deleteDerivedRole(attribute: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/derived-roles/${encodeURIComponent(attribute)}`, {
    method: "DELETE",
  });
  await refusedOrJson(res, "delete derived role");
}

/** One attribute of a member: the org's record, or what their identity provider supplied at their
 *  last sign-in. Where both name it, the org's value is in force and the provider's is overridden. */
export interface MemberAttribute {
  name: string;
  value: string;
  source: "org" | "idp";
  overridden: boolean;
}

function attributesPath(userId: string, name?: string): string {
  const base = `${API_BASE}/admin/users/${encodeURIComponent(userId)}/attributes`;
  return name === undefined ? base : `${base}/${encodeURIComponent(name)}`;
}

export async function fetchMemberAttributes(userId: string): Promise<MemberAttribute[]> {
  const res = await fetch(attributesPath(userId));
  if (!res.ok) throw httpError("fetch attributes", res.status);
  return res.json();
}

export async function setMemberAttribute(
  userId: string,
  name: string,
  value: string,
): Promise<MemberAttribute> {
  const res = await fetch(attributesPath(userId, name), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ value }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("set attribute", res.status)));
  }
  return res.json();
}

export async function deleteMemberAttribute(userId: string, name: string): Promise<void> {
  const res = await fetch(attributesPath(userId, name), { method: "DELETE" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("delete attribute", res.status)));
  }
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
  // REQ-1594: how many redemptions the link has admitted and how many it admits in total. null is
  // unlimited — an open link, which "used_at is set" no longer describes.
  uses: number;
  max_uses: number | null;
  // REQ-1595/REQ-1600: what each redeemer is given to work in. "none" seats them in the org;
  // "shared" seats everyone in `env_name`; "per_visitor" mints one per redeemer, reaped after
  // `env_ttl_seconds` of disuse.
  env_policy: string;
  env_ttl_seconds: number | null;
  env_name: string | null;
}

// REQ-1595: the env policies an invitation can carry, as the API names them.
export const ENV_POLICY_NONE = "none";
export const ENV_POLICY_PER_VISITOR = "per_visitor";
export const ENV_POLICY_SHARED = "shared";

export interface CreateInviteOptions {
  roleId?: string;
  expiresInDays?: number;
  // REQ-1287: addressing the invite to an email is what lets the invitee be told they have a
  // pending invitation on first sign-in. Omit for a shareable link.
  email?: string;
  // REQ-1594: null is unlimited, which is the only answer an open link can give. Omitted is the
  // single-use invitation every caller minted before open invites existed.
  maxUses?: number | null;
  // REQ-1595/REQ-1600.
  envPolicy?: string;
  envTtlSeconds?: number | null;
  envName?: string | null;
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
  if (!res.ok) throw httpError("fetchInvites", res.status);
  return res.json();
}

export async function createInvite(
  orgId: string,
  opts: CreateInviteOptions = {},
): Promise<OrgInvite> {
  const res = await fetch(`${API_BASE}/admin/invites`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      org_id: orgId,
      role_id: opts.roleId ?? null,
      expires_in_days: opts.expiresInDays ?? 7,
      email: opts.email ?? null,
      // REQ-1594: `undefined` is absent from the JSON, so an omitted ceiling is the server's
      // default of one rather than the unlimited that `null` means.
      max_uses: opts.maxUses === undefined ? 1 : opts.maxUses,
      env_policy: opts.envPolicy ?? ENV_POLICY_NONE,
      env_ttl_seconds: opts.envTtlSeconds ?? null,
      env_name: opts.envName ?? null,
    }),
  });
  if (!res.ok) throw httpError("createInvite", res.status);
  return res.json();
}

export async function revokeInvite(token: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/invites/${token}`, { method: "DELETE" });
  if (!res.ok) throw httpError("revokeInvite", res.status);
}

export async function fetchInviteInfo(token: string): Promise<InviteInfo> {
  // Use public endpoint that's not behind auth middleware — invites must be
  // fetchable before login, when there's no bearer token.
  const res = await fetch(`/public/invite-info/${token}`);
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
  if (!res.ok) throw httpError("my-invites", res.status);
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
  if (!res.ok) throw httpError("auto-join-offers", res.status);
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
    headers: { "Content-Type": "application/json", "X-Provisa-Role": role },
    body: JSON.stringify({ q, strict }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(serverMessage(data, data.error || requestFailed("NL submit", res.status)));
  }
  return res.json();
}

/** Whether `role` may run non-strict NL: only a role that may bypass the relationship guard, by the
 * server's one bypass rule (REQ-603). The NL page holds its Strict switch on for any other. */
export async function fetchNlMode(role: string): Promise<{ non_strict: boolean }> {
  const res = await fetch(`${API_BASE}/query/nl/mode`, { headers: { "X-Provisa-Role": role } });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(serverMessage(data, data.error || requestFailed("NL mode", res.status)));
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
  /** The field's own name, when it differs from the column it reads. */
  alias: string | null;
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
  /** Fields that are computations, not columns of the source: named, never registered. */
  not_importable: { name: string; expression: string }[];
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
  if (!resp.ok) throw httpError("Ossie export", resp.status);
  return resp.text();
}

/** REQ-1967: the governed catalog as a dbt sources file, as `role` is served it. */
export const DBT_SOURCES_ENDPOINT_PATH = "/admin/dbt/sources";

export async function fetchDbtSourcesYaml(role: string): Promise<string> {
  const resp = await fetch(
    `${API_BASE}${DBT_SOURCES_ENDPOINT_PATH}?role=${encodeURIComponent(role)}`,
  );
  if (!resp.ok) {
    const body = await resp.json().catch(() => null);
    throw new Error(serverMessage(body, requestFailed("dbt sources export", resp.status)));
  }
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

// --- Settings catalog (REQ-1913) ---

export type CatalogType = "int" | "float" | "bool" | "enum" | "str" | "list" | "map" | "secret";

export interface CatalogSetting {
  key: string;
  type: CatalogType;
  unit?: string;
  min?: number;
  max?: number;
  choices?: string[];
  /** What the process runs with. For a restart setting, `stored` is what the next start uses. */
  value?: unknown;
  running_value?: unknown;
  source: "stored" | "env" | "config" | "default";
  default: unknown;
  stored?: unknown;
  env?: unknown;
  config_key: string | null;
  restart_required: boolean;
  pending_restart: boolean;
  secret: boolean;
  /** A credential field that keeps what was typed (a value, or a reference to a secret) and
   * shows it again in a masked input. */
  credential?: boolean;
  /** Secrets only: whether a value is stored. A secret never carries a value. */
  set?: boolean;
  editable: boolean;
  guard: null | "confirm";
  /** An i18n key naming why the setting is not editable. */
  readonly_reason?: string;
  /** The Helm chart value that sets it, where the chart renders the engine's config. */
  chart_value?: string;
  /** A map with a fixed set of entries: rendered as one row per key. */
  map_keys?: string[];
  /** A list whose entries are rows of these columns, space-separated: shown as a table. */
  columns?: string[];
  /** A map's per-entry source. */
  sources?: Record<string, "stored" | "env" | "config" | "default">;
  updated_by: string | null;
  updated_at: string | null;
  /** Set when the current value is unusable (`value` is then null): where it came from and why. */
  error?: { field: string; source: string; reason: string };
}

export interface SettingsCatalog {
  snapshot_ttl_seconds: number;
  pending_restart: string[];
  cards: { id: string; settings: CatalogSetting[] }[];
}

/** platform_admin only: a 403 carries `status` so the page can show that state, not an empty one. */
export async function fetchSettingsCatalog(): Promise<SettingsCatalog> {
  const resp = await fetch(`${API_BASE_RAW}/admin/settings/catalog`);
  if (!resp.ok) {
    throw Object.assign(new Error(requestFailed("Settings catalog fetch", resp.status)), {
      status: resp.status,
    });
  }
  return resp.json();
}

/** `null` clears a stored value. A 400 names the field and a `reason` in its error `params`. */
export async function updateSettingsCatalog(
  values: Record<string, unknown>,
  confirm: string[],
): Promise<{ updated: string[]; pending_restart: string[] }> {
  const resp = await fetch(`${API_BASE_RAW}/admin/settings/catalog`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ values, confirm }),
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => null);
    throw Object.assign(
      new Error(serverMessage(body, requestFailed("Settings catalog update", resp.status))),
      { status: resp.status, code: body?.code, params: body?.params },
    );
  }
  return resp.json();
}

/** REQ-464: one candidate a natural-language table search ranked, with the confidence and the
 *  reasoning the ranker gave (empty when no model ranked it). */
export interface TableSearchCandidate {
  schema_name: string;
  table_name: string;
  comment: string | null;
  confidence: number;
  reasoning: string;
  cache_warm: boolean;
}

/** REQ-464: what a table search answers with, and what it searched. Table names are complete
 *  from the first answer. Column names are loaded the first time a schema is searched:
 *  "loading" means a table that matches only by a column may be missing — ask again;
 *  "unavailable" means they could not be loaded. */
export interface TableSearchAnswer {
  table_names: "complete";
  column_names: "complete" | "loading" | "unavailable";
  candidates: TableSearchCandidate[];
}

/** REQ-464: search a source's schema for the tables a description fits. The steward chooses;
 *  nothing is registered by a search. */
export async function searchSourceTables(
  sourceId: string,
  query: string,
  schemaName: string,
): Promise<TableSearchAnswer> {
  const params = new URLSearchParams({ q: query, schema_name: schemaName });
  const res = await fetch(
    `${API_BASE}/admin/sources/${encodeURIComponent(sourceId)}/tables/search?${params}`,
  );
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(body, requestFailed("table search", res.status)));
  }
  return res.json();
}

/** REQ-1992: whether the org's file folder is encrypted, and the vault reference naming its key. */
export interface OrgFilesState {
  org_id: string;
  encrypted: boolean;
  key_reference: string | null;
}

export async function fetchOrgFiles(): Promise<OrgFilesState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/org-files`);
  if (!resp.ok) throw httpError("Org files fetch", resp.status);
  return resp.json();
}

export async function setOrgFiles(body: {
  encrypted: boolean;
  key_reference: string | null;
}): Promise<OrgFilesState> {
  const resp = await fetch(`${API_BASE_RAW}/admin/org-files`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw httpError("Org files save", resp.status);
  return resp.json();
}

/** A new 256-bit key, answered once: the server keeps no copy. */
export async function generateOrgFilesKey(): Promise<string> {
  const resp = await fetch(`${API_BASE_RAW}/admin/org-files/generate-key`, { method: "POST" });
  if (!resp.ok) throw httpError("Key generation", resp.status);
  return ((await resp.json()) as { key: string }).key;
}

/** The server's verdict on a regex entry's pattern or replacement: a construct outside the
 *  universal regex dialect, where it starts. ``ok`` true is a warning, not a refusal. */
export interface MaskPatternVerdict {
  entry: number;
  field: "pattern" | "replace";
  ok: boolean;
  construct: string;
  position: number;
  message: string;
}

export interface MasksReport {
  unreachable: { entry: number; covered_by: number | null }[];
  unlisted_unmasked: boolean;
  applies: number | null;
  /** Each construct of a regex entry outside the universal dialect; empty when all pass. */
  patterns: MaskPatternVerdict[];
}

/** A regex mask the server offers, in the universal dialect, with an example of what it does.
 *  One with ``params`` has a ``pattern_template`` the editor fills with a number for each. */
export interface MaskPreset {
  id: string;
  label: string;
  pattern?: string;
  pattern_template?: string;
  params?: string[];
  replace: string;
  example: { input: string; output: string };
}

/** The regex masks offered beside a regex entry's Pattern and Replacement. */
export async function maskPresets(): Promise<MaskPreset[]> {
  const res = await fetch(`${API_BASE}/admin/masks/presets`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("mask presets", res.status)));
  }
  return res.json();
}

/** The server's check of a mask list as edited: unreachable entries, readers left unmasked, and
 *  with ``roles`` the entry a reader acting as them reads the column through. */
export async function checkMasks(masks: MaskEntry[], roles?: string[]): Promise<MasksReport> {
  const res = await fetch(`${API_BASE}/admin/masks/check`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ masks, roles: roles ?? null }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("check masks", res.status)));
  }
  return res.json();
}
