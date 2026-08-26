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
import { createContext, useContext, useState, useEffect, useMemo, useCallback } from "react";
import type { ReactNode } from "react";
import type { Capability, Role, RoleAssignment, AuthState, OrgMembership } from "../types/auth";
import { AuthMeError, fetchMe } from "../api/admin";
import { useRoles, useDomains } from "../hooks/useAdminQueries";
import { CHECKED_DOMAINS_KEY, KNOWN_DOMAINS_KEY } from "../lib/domainFilterKeys";

// REQ-1297: dev/no-auth mirrors what the server grants an unsecured caller, and that is now
// org_admin — the DATA-plane administrator — not platform_admin. The no-auth configs' single
// default assignment is org_admin (config/provisa.yaml, config/provisa-install.yaml), and the
// middleware honours X-Provisa-Role only for roles the caller is actually assigned, so claiming
// platform_admin here would 403 every request. platform_admin carries only the control-plane bypass:
// it grants no capability the data surfaces gate on and names no column grant.
//
// REQ-1352: this list is org_admin's schema.sql seed, verbatim. It is a second copy of a definition
// that belongs to the seed alone, so it drifts silently — it had gained `read_restricted`, which
// the seed does not grant, and never gained the REQ-1349 pair
// `org_settings`/`observability`, so a dev-mode org administrator saw no Admin navigation group at
// all. `authContextDefaultAdminRole.test.ts` pins it to the seed so the next divergence fails a test
// instead of removing a tab.
export const DEFAULT_ADMIN_ROLE: Role = {
  id: "org_admin",
  capabilities: [
    "source_registration",
    "table_registration",
    "create_relationship",
    "create_view",
    "approve_view",
    "approve_relationship",
    "access_config",
    "user_management",
    "masking_config",
    "column_grant",
    "view_governance",
    "query_development",
    "full_results",
    "write",
    "usage",
    "org_settings",
    "observability",
    "environment_management",
    "environment_switch",
    "glossary_read", // REQ-1590
    "glossary_rw", // REQ-1590
  ] as Capability[],
  domain_access: ["*"],
};

// REQ-1337: a CONTROL-PLANE role is identified by the right it carries, never by its name or by
// enumerating what it lacks. `cross_org` — acting in an org one is not a member of — is exactly the
// control-plane authority, and REQ-1327 keeps a role holding it off the data plane entirely. This
// mirrors is_control_plane_role() in provisa/security/rights.py.
function isControlPlaneOnly(role: Role): boolean {
  return role.capabilities.includes("cross_org" as Capability);
}

// The acting role sent in X-Provisa-Role while "All roles" is selected is the first of this list, so
// the order decides which per-role schema the data surfaces get. It came straight from the `roles`
// query, i.e. Postgres heap order, which put platform_admin ahead of org_admin for the bootstrap
// super-admin — the GraphQL tab then built its schema for a role with zero data capabilities and the
// demo tables whose columns are granted to analyst/org_admin vanished ("Cannot query field
// 'ps__inquiriesGroupBy'"). Control-plane-only roles sort last; the rest sort by id so the choice is
// stable rather than storage-dependent.
function orderByDataPlaneFirst(roles: Role[]): Role[] {
  return [...roles].sort((a, b) => {
    const ca = Number(isControlPlaneOnly(a));
    const cb = Number(isControlPlaneOnly(b));
    return ca !== cb ? ca - cb : a.id.localeCompare(b.id);
  });
}

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
  // Re-run the identity bootstrap (fetchMe + roles/domains). REQ-1266: after self-service org
  // creation the new membership must be reflected without a full page reload.
  refresh: () => Promise<void>;
  // REQ-1286: HTTP status of the last /auth/me failure (0 = the request never reached the server),
  // null when the identity resolved. The onboarding gate routes on this: 401/403 means the session
  // is not valid, anything else means the deployment is broken — two different screens.
  identityErrorStatus: number | null;
  devMode: boolean;
  // REQ-1469: whether this deployment mounts /billing at all. Reported by /auth/me from the
  // commercial plugin seam; a self-hosted deployment has no billing surface to offer.
  billing: boolean;
  // Runtime auth-enforcement flag from /setup/status (REQ-1267). Drives the login gate
  // and logout affordance instead of a build-time constant.
  authEnabled: boolean;
  // Tenancy mode from /setup/status: false hides org-lifecycle affordances (e.g. Delete
  // Organization) that make no sense on a single-tenant deploy.
  multitenancy: boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({
  children,
  authEnabled,
  authSettled,
  authVersion = 0,
  multitenancy = false,
}: {
  children: ReactNode;
  authEnabled: boolean;
  // True once /setup/status has answered, i.e. once `authEnabled` carries the deployment's real
  // value rather than the `false` it is initialised to. The bootstrap waits for it: run before the
  // flag settles, the whole identity fetch is thrown away and repeated the moment it flips, so
  // every page load paid for two serialised rounds of /auth/me + roles + domains and the capability
  // gates stayed on "Checking your credentials…" until the SECOND round landed.
  authSettled: boolean;
  // REQ-1291: bumped by App every time a token is stored or dropped. The provider outlives the
  // login page, so without this the identity fetched at mount (none) was never revisited: a
  // just-signed-in user stayed userId=null and OnboardGate read the fresh token as a rejected
  // credential, cleared it, and put them back on sign-in — an unbreakable loop.
  authVersion?: number;
  multitenancy?: boolean;
}) {
  const [availableRoles, setAvailableRoles] = useState<Role[]>([]);
  const [assignments, setAssignments] = useState<RoleAssignment[]>([]);
  const [selectedRole, setSelectedRole] = useState<Role | "all">("all");
  const [selectedDomain, setSelectedDomain] = useState<string | null>(null);
  const [allDomainsList, setAllDomainsList] = useState<string[]>([]);
  const [devMode, setDevMode] = useState(false);
  const [billing, setBilling] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedOrg, setSelectedOrg] = useState<string | null>(() =>
    localStorage.getItem("provisa_org"),
  );
  const [orgMemberships, setOrgMemberships] = useState<OrgMembership[]>([]);
  const [userId, setUserId] = useState<string | null>(null);
  const [identityErrorStatus, setIdentityErrorStatus] = useState<number | null>(null);
  const [email, setEmail] = useState<string | null>(null);
  const [displayName, setDisplayName] = useState<string | null>(null);
  const [givenName, setGivenName] = useState<string | null>(null);
  const [familyName, setFamilyName] = useState<string | null>(null);
  const { refetch: refetchRoles } = useRoles();
  const { refetch: refetchDomains } = useDomains();

  // REQ-1291: raise `loading` in the SAME render that authVersion changes, not in the effect that
  // re-runs the bootstrap. OnboardGate is a descendant, and React runs child effects before parent
  // effects — so an effect here is too late: the gate would already have seen the freshly stored
  // token alongside the pre-login (null) identity, read it as a rejected credential, and deleted
  // it. An in-flight identity fetch is `loading`, never a resolved absence of identity.
  const [bootstrappedVersion, setBootstrappedVersion] = useState(authVersion);
  if (bootstrappedVersion !== authVersion) {
    setBootstrappedVersion(authVersion);
    setLoading(true);
  }

  const bootstrap = useCallback(async () => {
    {
      let allRoles: Role[] = [];
      let isDev: boolean;
      let userAssignments: RoleAssignment[] = [];

      try {
        const me = await fetchMe();
        isDev = me.dev_mode;
        setBilling(me.billing);
        userAssignments = me.assignments;
        setOrgMemberships(me.org_memberships);
        setUserId(me.user_id);
        setIdentityErrorStatus(null);
        setEmail(me.email ?? null);
        setDisplayName(me.display_name ?? null);
        setGivenName(me.given_name ?? null);
        setFamilyName(me.family_name ?? null);
        // REQ-1326: the server is the authority on which orgs this identity may act in. A stored
        // `provisa_org` is only honoured while it names an org `/auth/me` still reports membership
        // of; otherwise it is stale (org deleted, or left behind by a previous sign-in) and rides
        // on every request as X-Org-Provisa, scoping the whole app to an org that is not theirs.
        const storedOrg = localStorage.getItem("provisa_org");
        const isMember = me.org_memberships.some((m) => m.org_id === storedOrg);
        if (storedOrg && !isMember) {
          localStorage.removeItem("provisa_org");
        }
        if (!storedOrg || !isMember) {
          if (me.active_org_id) {
            setSelectedOrg(me.active_org_id);
          } else if (me.org_memberships.length === 1) {
            setSelectedOrg(me.org_memberships[0].org_id);
          } else {
            setSelectedOrg(null);
          }
        }
      } catch (e) {
        // /auth/me failed. On an unsecured deploy that means dev mode (full admin). When auth
        // IS enforced, a failure here is an unauthenticated caller — never grant dev admin;
        // RequireAuth redirects to /login before any admin content renders (REQ-1267).
        isDev = !authEnabled;
        // REQ-1286: keep WHY it failed. A 5xx is the deployment failing, not an access decision,
        // and must not be reported to the user as "this account has no access".
        setIdentityErrorStatus(e instanceof AuthMeError ? e.status : 0);
        // Clear any prior identity so a session that dies mid-app (token expired / account
        // removed) resolves to userId=null — the signal OnboardGate uses to treat the stored
        // token as a dead session rather than a live, member-less onboarding state.
        setUserId(null);
        setBilling(false);
        setOrgMemberships([]);
        setEmail(null);
        setDisplayName(null);
        setGivenName(null);
        setFamilyName(null);
      }

      let rolesError: string | null = null;
      try {
        const { data } = await refetchRoles();
        // refetch returns RAW query data; the GraphQL field is camelCase `domainAccess`,
        // but the rest of the app reads snake_case `domain_access` (e.g. DomainFilterContext).
        // Apply the same mapping useRoles()'s return value does.
        const roles = (data?.roles ?? []).map((r) => ({
          ...r,
          domain_access: (r as { domainAccess?: string[] }).domainAccess ?? r.domain_access,
        }));
        if (roles.length > 0) {
          if (isDev) {
            allRoles = orderByDataPlaneFirst(roles);
          } else {
            const assignedRoleIds = new Set(userAssignments.map((a) => a.role_id));
            allRoles = orderByDataPlaneFirst(roles.filter((r) => assignedRoleIds.has(r.id)));
          }
        }
      } catch (e) {
        rolesError = e instanceof Error ? e.message : String(e);
      }

      // REQ-1295: the full-capability `admin` role is real ONLY where auth is not enforced — there
      // the server grants every capability to every caller and the client mirrors that. Under
      // enforced auth it is a fiction: the server honors X-Provisa-Role only for roles the user is
      // assigned (provisa/auth/middleware.py), so handing an org_admin a client-side `admin` makes
      // every subsequent request 403 "Role 'admin' is not assigned to this user" — which is exactly
      // what a self-service org creator saw. An empty role list is reported, never papered over.
      if (allRoles.length === 0) {
        if (isDev) {
          allRoles = [DEFAULT_ADMIN_ROLE];
          userAssignments = [{ role_id: "org_admin", domain_id: "*" }];
        } else if (rolesError) {
          setError(`Could not load roles: ${rolesError}`);
        } else if (userAssignments.length > 0) {
          setError("Your assigned roles are not defined in this org.");
        }
      }
      if (allRoles.length > 0) setError(null);

      const hasWildcard = userAssignments.some((a) => a.domain_id === "*");
      if (hasWildcard) {
        try {
          const { data } = await refetchDomains();
          setAllDomainsList((data?.domains ?? []).map((d) => d.id));
        } catch {
          setAllDomainsList([]);
        }
      }

      setDevMode(isDev);
      setAvailableRoles(allRoles);
      setAssignments(userAssignments);

      // REQ-273/REQ-1295: reconcile the acting role against the roles just resolved, in BOTH
      // directions. A role the caller no longer holds — after an org switch, which keeps
      // `provisa_role` and the selectedRole state from the org being left, or after an assignment
      // is revoked — must not stay acting: it is stamped into the `role=` parameter of
      // /data/rest/docs, whose Swagger page repeats it as X-Provisa-Role, and the server refuses a
      // role the user is not assigned with 403 "Role 'x' is not assigned to this user". The rest of
      // the app never sends that header, so the stale role surfaces only there — the OpenAPI page
      // failing to load its spec while every other surface works.
      const savedRoleId = localStorage.getItem("provisa_role");
      const saved = savedRoleId ? allRoles.find((r) => r.id === savedRoleId) : undefined;
      if (saved) {
        setSelectedRole(saved);
      } else {
        if (savedRoleId) localStorage.removeItem("provisa_role");
        setSelectedRole((current) =>
          current === "all" || allRoles.some((r) => r.id === current.id) ? current : "all",
        );
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- setters are stable; gate on authEnabled
  }, [authEnabled, authVersion]);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      await bootstrap();
    } finally {
      setLoading(false);
    }
  }, [bootstrap]);

  useEffect(() => {
    // Bootstrap once the runtime auth-enforcement flag has resolved, so the dev-mode fallback keys
    // off the settled value; and again on every authVersion bump (REQ-1291). An unsettled flag is
    // not a value to bootstrap against — see `authSettled`.
    if (!authSettled) return;
    let cancelled = false;
    async function run() {
      await bootstrap();
      if (!cancelled) setLoading(false);
    }
    void run();
    return () => {
      cancelled = true;
    };
  }, [bootstrap, authSettled]);

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
    if (r === "all") localStorage.removeItem("provisa_role");
    else localStorage.setItem("provisa_role", (r as Role).id);
  }

  function selectDomain(domain: string | null) {
    setSelectedDomain(domain);
  }

  function selectOrg(orgId: string | null) {
    setSelectedOrg(orgId);
    // REQ-1297: the persisted domain filter belongs to the org being left. Its domain ids are that
    // org's, and a domain it had unchecked would stay unchecked in the org being entered — the
    // reported "the new org has no meta or ops domain" with both present server-side. Drop it so the
    // incoming org's domains all start checked.
    localStorage.removeItem(CHECKED_DOMAINS_KEY);
    localStorage.removeItem(KNOWN_DOMAINS_KEY);
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
        billing,
        loading,
        error,
        activeOrgId,
        orgMemberships,
        selectOrg,
        refresh,
        identityErrorStatus,
        userId,
        email,
        displayName,
        givenName,
        familyName,
        authEnabled,
        multitenancy,
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
