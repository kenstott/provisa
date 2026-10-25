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
  | "usage"
  | "read_restricted"
  | "approve_relationship"
  | "create_view"
  | "column_grant"
  | "user_management"
  | "masking_config"
  // REQ-1134: see the meta domain's GOVERNANCE columns (visible_to, unmasked_to, writable_by, the
  // mask definitions, view SQL) — the security posture rather than the structure.
  | "view_governance"
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
  // REQ-2155: manage the environments the caller owns (create one, becoming its owner; delete one
  // it owns) -- a developer's; `environment_management` manages any.
  | "environment_owned_management"
  | "environment_switch"
  // REQ-1942: changing an environment's data choices; org_admin alone.
  | "environment_data"
  // REQ-1943: revealing or hiding a sensitive column; org_admin by default.
  | "sensitive_data"
  // REQ-1590: the glossary's two rights. `glossary_read` opens the surface — reading a term is not
  // administering the org, so an analyst holds it; `glossary_rw` is curation on top of it, and the
  // page renders read-only without it. A curator is granted both.
  | "glossary_read"
  | "glossary_rw"
  // REQ-1921: set, change or remove where data lives, for the region values the grant lists.
  // Exists only when the platform declares regions.
  | "data_residency"
  // REQ-1592: the org's glossary owner — curates any term whatever its domains and whoever
  // authored it, and is alone in being able to scope a term to the whole org ("*"). Seeded to
  // org_admin only; it widens what a curator may do rather than opening the surface, so a holder
  // carries `glossary_read` and `glossary_rw` as well.
  | "org_glossary_rw"
  // REQ-1634: data products' two rights. `data_product_read` opens the surface — org_admin,
  // analyst, developer, and modeler all hold it. `data_product_rw` is create/delete, on the
  // same footing as `table_registration`: org_admin only.
  | "data_product_read"
  | "data_product_rw"
  // REQ-2155: edit a data product the caller's role owns (its owner_role, an ancestor included).
  | "data_product_owned_rw"
  // REQ-2155: data quality's own rights -- reading check definitions and results, and managing
  // checks, expectations and promises -- so managing checks needs no table registration.
  | "dq_read"
  | "dq_rw";

export interface RoleRateLimit {
  requestsPerSecond: number | null;
  maxQueryComplexity: number | null;
  maxQueryTimeMs: number | null;
}

/** REQ-1682: one of a role's constant session values. */
export interface SessionVar {
  name: string;
  value: string;
}

export interface Role {
  id: string;
  /** REQ-2080: what the role is for, as an access review reads it. Shown even where the role's
   *  definition is withheld: it explains, it grants nothing. */
  description: string;
  capabilities: Capability[];
  /**
   * REQ-1602: rights the role does NOT hold, whose surfaces it is shown anyway -- disabled and
   * badged as belonging to the production system. A role defined by subtraction (the sandbox,
   * REQ-1597) is a demonstration of the product, and a feature that is simply absent demonstrates
   * nothing. Disjoint from `capabilities`.
   */
  demonstrated: Capability[];
  domain_access: string[];
  rateLimit?: RoleRateLimit | null; // REQ-1174: per-role rate + query-complexity limits
  /** REQ-2155: the roles this one inherits from, in order; its capabilities are their union. */
  parents?: string[];
  /** REQ-2155: a standard role is Provisa's and locked -- extended with child roles, never edited. */
  standard?: boolean;
  /** REQ-1921: with data_residency, the region values its grant covers (regions, "no_region"). */
  residencyValues?: string[];
  /** REQ-1682: the role's own constant session values, what its row rules read when the request
   *  binds nothing of that name. */
  sessionVars?: SessionVar[];
  /** REQ-2077: a grant of the role unused this many days is removed; null/absent: never. */
  inactivityDays?: number | null;
  /** The server withheld this role's definition from the caller; the lists above are empty placeholders. */
  detailsHidden?: boolean;
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
  /** Set only for a per_visitor ephemeral membership (provisa/api/invite_env.py) -- the visitor
   *  never chose to belong to the org, an invite minted them a throwaway sandbox, so there is
   *  nothing about "joining" worth announcing. */
  env_name?: string | null;
}

/** A derived role the user holds (REQ-2057): a declared attribute (parent null) or the sub-role
 *  of the value their attributes give it. Rightless; a request acts as it only by naming it. */
export interface DerivedRole {
  id: string;
  parent: string | null;
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
  /** REQ-1602: rights shown-but-withheld across all selected roles, minus any actually held. */
  demonstrated: Capability[];
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
