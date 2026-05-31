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
  | 'source_registration'
  | 'table_registration'
  | 'create_relationship'
  | 'access_config'
  | 'query_development'
  | 'approve_view'
  | 'full_results'
  | 'admin'
  | 'usage'
  | 'read_restricted'
  | 'approve_relationship'
  | 'create_view'
  | 'column_grant'
  | 'user_management'
  | 'masking_config'
  | 'superadmin';

export interface Role {
  id: string;
  capabilities: Capability[];
  domain_access: string[];
}

/** A single role:domain pair from a user's identity claims or DB assignments. */
export interface RoleAssignment {
  role_id: string;
  domain_id: string;
}

export interface OrgMembership {
  org_id: string;
  org_name: string;
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
  selectedRole: Role | 'all';
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
}
