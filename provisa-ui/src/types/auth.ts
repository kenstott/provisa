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
  | "relationship_registration"
  | "security_config"
  | "query_development"
  | "query_approval"
  | "full_results"
  | "admin";

export interface Role {
  id: string;
  capabilities: Capability[];
  domain_access: string[];
}

export interface AuthState {
  /** First selected role — used for API headers that require a single role. */
  role: Role | null;
  /** All currently selected roles. */
  selectedRoles: Role[];
  /** Unioned capabilities across all selected roles. */
  capabilities: Capability[];
  /** Unioned domain_access across all selected roles. */
  domainAccess: string[];
  loading: boolean;
  error: string | null;
}
