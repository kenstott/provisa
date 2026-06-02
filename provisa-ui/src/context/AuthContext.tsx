// Copyright (c) 2026 Kenneth Stott
// Canary: d193460f-9c5e-4775-99e0-f8d6840808bb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-refresh/only-export-components -- context Provider + hook colocated by design */
import { createContext, useContext, useState, useEffect, useMemo } from "react";
import type { ReactNode } from "react";
import type { Capability, Role, RoleAssignment, AuthState, OrgMembership } from "../types/auth";
import { fetchRoles, fetchMe, fetchDomains } from "../api/admin";

const DEFAULT_ADMIN_ROLE: Role = {
  id: "admin",
  capabilities: [
    "source_registration",
    "table_registration",
    "create_relationship",
    "access_config",
    "query_development",
    "approve_view",
    "full_results",
    "admin",
    "usage",
    "read_restricted",
    "approve_relationship",
    "create_view",
    "column_grant",
    "user_management",
    "masking_config",
    "superadmin",
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

interface AuthContextValue extends AuthState {
  selectRole: (role: Role | "all") => void;
  availableRoles: Role[];
  availableDomains: string[];
  selectDomain: (domain: string | null) => void;
  selectOrg: (orgId: string | null) => void;
  devMode: boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [availableRoles, setAvailableRoles] = useState<Role[]>([]);
  const [assignments, setAssignments] = useState<RoleAssignment[]>([]);
  const [selectedRole, setSelectedRole] = useState<Role | "all">("all");
  const [selectedDomain, setSelectedDomain] = useState<string | null>(null);
  const [allDomainsList, setAllDomainsList] = useState<string[]>([]);
  const [devMode, setDevMode] = useState(false);
  const [loading, setLoading] = useState(true);
  const error: string | null = null;
  const [selectedOrg, setSelectedOrg] = useState<string | null>(() =>
    localStorage.getItem("provisa_org"),
  );
  const [orgMemberships, setOrgMemberships] = useState<OrgMembership[]>([]);
  const [userId, setUserId] = useState<string | null>(null);
  const [email, setEmail] = useState<string | null>(null);
  const [displayName, setDisplayName] = useState<string | null>(null);

  useEffect(() => {
    async function init() {
      let allRoles: Role[] = [];
      let isDev: boolean;
      let userAssignments: RoleAssignment[] = [];

      try {
        const me = await fetchMe();
        isDev = me.dev_mode;
        userAssignments = me.assignments;
        setOrgMemberships(me.org_memberships);
        setUserId(me.user_id);
        setEmail(me.email ?? null);
        setDisplayName(me.display_name ?? null);
        if (me.active_org_id && !localStorage.getItem("provisa_org")) {
          setSelectedOrg(me.active_org_id);
        } else if (me.org_memberships.length === 1 && !localStorage.getItem("provisa_org")) {
          setSelectedOrg(me.org_memberships[0].org_id);
        }
      } catch {
        isDev = true;
      }

      try {
        const roles = await fetchRoles();
        if (roles.length > 0) {
          if (isDev) {
            allRoles = roles;
          } else {
            const assignedRoleIds = new Set(userAssignments.map((a) => a.role_id));
            allRoles = roles.filter((r) => assignedRoleIds.has(r.id));
          }
        }
      } catch {
        // fall through
      }

      if (allRoles.length === 0) {
        allRoles = [DEFAULT_ADMIN_ROLE];
        if (isDev) {
          userAssignments = [{ role_id: "admin", domain_id: "*" }];
        }
      }

      const hasWildcard = userAssignments.some((a) => a.domain_id === "*");
      if (hasWildcard) {
        try {
          const domains = await fetchDomains();
          setAllDomainsList(domains.map((d) => d.id));
        } catch {
          setAllDomainsList([]);
        }
      }

      setDevMode(isDev);
      setAvailableRoles(allRoles);
      setAssignments(userAssignments);
    }

    init().finally(() => setLoading(false));
  }, []);

  const availableDomains = useMemo(() => {
    const roleFilter = selectedRole === "all" ? null : (selectedRole as Role).id;
    const relevant = roleFilter ? assignments.filter((a) => a.role_id === roleFilter) : assignments;
    const domainIds = relevant.map((a) => a.domain_id);
    if (domainIds.includes("*")) return allDomainsList;
    return [...new Set(domainIds)];
  }, [selectedRole, assignments, allDomainsList]);

  const activeRoles = useMemo(
    () => (selectedRole === "all" ? availableRoles : [selectedRole as Role]),
    [selectedRole, availableRoles],
  );
  const capabilities = useMemo(() => unionCapabilities(activeRoles), [activeRoles]);
  const domainAccess = availableDomains;
  const role = activeRoles.length > 0 ? activeRoles[0] : null;
  const selectedRoles = activeRoles;

  function selectRole(r: Role | "all") {
    setSelectedRole(r);
    setSelectedDomain(null);
  }

  function selectDomain(domain: string | null) {
    setSelectedDomain(domain);
  }

  function selectOrg(orgId: string | null) {
    setSelectedOrg(orgId);
    if (orgId) localStorage.setItem("provisa_org", orgId);
    else localStorage.removeItem("provisa_org");
  }

  const activeOrgId =
    selectedOrg ?? (orgMemberships.length === 1 ? orgMemberships[0].org_id : null);

  return (
    <AuthContext.Provider
      value={{
        role,
        selectedRoles,
        capabilities,
        domainAccess,
        selectedRole,
        selectedDomain,
        selectRole,
        selectDomain,
        availableRoles,
        availableDomains,
        assignments,
        devMode,
        loading,
        error,
        activeOrgId,
        orgMemberships,
        selectOrg,
        userId,
        email,
        displayName,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
