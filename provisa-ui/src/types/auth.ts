// Copyright (c) 2026 Kenneth Stott
// Canary: a9ba6c1e-3396-4f35-8c71-db0540a4fcc3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Capabilities matching provisa/security/rights.py */
export type Capability =
  | "source_registration"
  | "table_registration"
  | "create_relationship"
  | "access_config"
  | "query_development"
  | "approve_view"
  | "full_results"
  | "admin"
  | "usage"
  | "read_restricted"
  | "approve_relationship"
  | "create_view"
  | "column_grant"
  | "user_management"
  | "masking_config"
  | "superadmin"
  // REQ-1337: RIGHTS ONLY — no role id appears in this union. Every gate, server and UI alike,
  // names a right; the seed decides which role carries it (platform_settings and cross_org go to
  // platform_admin always, and to org_admin only where single-tenant mode grants them).
  | "platform_settings"
  | "cross_org"
  // REQ-1349: the org-scoped pair. `org_settings` gates surfaces whose subject is the acting org
  // (its AI/NL provider, domains, scheduled tasks, approvals); `observability` gates read-only
  // performance and health. org_admin carries both in either tenancy mode.
  | "org_settings"
  | "observability"
  // REQ-1573: the two environment rights. `environment_management` is creating and deleting one and
  // reaching the environments admin surface; `environment_switch` is being served by one other than
  // prod. org_admin and developer carry both; analyst and modeler carry neither.
  | "environment_management"
  | "environment_switch";

export interface RoleRateLimit {
  requestsPerSecond: number | null;
  maxQueryDepth: number | null;
  maxQueryNodes: number | null;
  maxQueryTimeMs: number | null;
}

export interface Role {
  id: string;
  capabilities: Capability[];
  domain_access: string[];
  rateLimit?: RoleRateLimit | null; // REQ-1174: per-role rate + query-complexity limits
}

/** A single role:domain pair from a user's identity claims or DB assignments. */
export interface RoleAssignment {
  role_id: string;
  domain_id: string;
}

// REQ-1478: how the membership came about ("created" | "invite" | "auto_join" | "admin"), and
// whether the member has been shown that explanation. null joined_via predates the column.
export interface OrgMembership {
  org_id: string;
  org_name: string;
  joined_via?: string | null;
  acknowledged?: boolean;
}

export interface AuthState {
  /** First selected role — used for API headers that require a single role. */
  role: Role | null;
  /** All currently selected roles (kept for backwards compat). */
  selectedRoles: Role[];
  /** Unioned capabilities across all selected roles. */
  capabilities: Capability[];
  /** Unioned domain_access across all selected roles. */
  domainAccess: string[];
  selectedRole: Role | "all";
  selectedDomain: string | null;
  /** All role:domain pairs for the authenticated user (empty in dev mode until roles load). */
  assignments: RoleAssignment[];
  loading: boolean;
  error: string | null;
  activeOrgId: string | null;
  orgMemberships: OrgMembership[];
  userId: string | null;
  email: string | null;
  displayName: string | null;
  givenName: string | null;
  familyName: string | null;
}
