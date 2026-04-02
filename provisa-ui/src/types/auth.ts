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
  role: Role | null;
  loading: boolean;
  error: string | null;
}
