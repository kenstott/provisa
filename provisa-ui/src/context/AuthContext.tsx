import { createContext, useContext, useState, useEffect, ReactNode } from "react";
import type { Capability, Role, AuthState } from "../types/auth";
import { fetchRoles } from "../api/admin";

/** Default admin role when no auth/roles are configured. */
const DEFAULT_ADMIN_ROLE: Role = {
  id: "admin",
  capabilities: [
    "source_registration", "table_registration", "relationship_registration",
    "security_config", "query_development", "query_approval", "full_results", "admin",
  ] as Capability[],
  domain_access: ["*"],
};

interface AuthContextValue extends AuthState {
  setRole: (role: Role | null) => void;
  roles: Role[];
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [role, setRole] = useState<Role | null>(null);
  const [roles, setRoles] = useState<Role[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchRoles()
      .then((r) => {
        if (r.length > 0) {
          setRoles(r);
          setRole(r[0]);
        } else {
          // No roles configured — assume admin with full access
          setRoles([DEFAULT_ADMIN_ROLE]);
          setRole(DEFAULT_ADMIN_ROLE);
        }
      })
      .catch(() => {
        // Backend unavailable or no security — assume admin
        setRoles([DEFAULT_ADMIN_ROLE]);
        setRole(DEFAULT_ADMIN_ROLE);
      })
      .finally(() => setLoading(false));
  }, []);

  return (
    <AuthContext.Provider value={{ role, setRole, roles, loading, error }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
