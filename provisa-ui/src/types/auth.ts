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
