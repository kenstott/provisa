// Copyright (c) 2026 Kenneth Stott
// Canary: d193460f-9c5e-4775-99e0-f8d6840808bb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createContext, useContext, useState, useEffect, useMemo } from "react";
import type { ReactNode } from "react";
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

function unionCapabilities(roles: Role[]): Capability[] {
  const set = new Set<Capability>();
  for (const r of roles) {
    for (const c of r.capabilities) set.add(c);
  }
  return [...set];
}

function unionDomainAccess(roles: Role[]): string[] {
  const set = new Set<string>();
  for (const r of roles) {
    for (const d of r.domain_access) set.add(d);
  }
  return [...set];
}

interface AuthContextValue extends AuthState {
  /** Toggle a role on/off. At least one role must remain selected. */
  toggleRole: (role: Role) => void;
  /** All available roles. */
  availableRoles: Role[];
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [selectedRoles, setSelectedRoles] = useState<Role[]>([]);
  const [availableRoles, setAvailableRoles] = useState<Role[]>([]);
  const [loading, setLoading] = useState(true);
  const error: string | null = null;

  useEffect(() => {
    fetchRoles()
      .then((r) => {
        if (r.length > 0) {
          setAvailableRoles(r);
          setSelectedRoles([r[0]]);
        } else {
          setAvailableRoles([DEFAULT_ADMIN_ROLE]);
          setSelectedRoles([DEFAULT_ADMIN_ROLE]);
        }
      })
      .catch(() => {
        setAvailableRoles([DEFAULT_ADMIN_ROLE]);
        setSelectedRoles([DEFAULT_ADMIN_ROLE]);
      })
      .finally(() => setLoading(false));
  }, []);

  const capabilities = useMemo(() => unionCapabilities(selectedRoles), [selectedRoles]);
  const domainAccess = useMemo(() => unionDomainAccess(selectedRoles), [selectedRoles]);
  const role = selectedRoles.length > 0 ? selectedRoles[0] : null;

  const toggleRole = (r: Role) => {
    setSelectedRoles((prev) => {
      const isSelected = prev.some((p) => p.id === r.id);
      if (isSelected) {
        // Don't allow deselecting the last role
        if (prev.length <= 1) return prev;
        return prev.filter((p) => p.id !== r.id);
      }
      return [...prev, r];
    });
  };

  return (
    <AuthContext.Provider value={{
      role, selectedRoles, capabilities, domainAccess,
      toggleRole, availableRoles, loading, error,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
