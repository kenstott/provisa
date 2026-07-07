// Copyright (c) 2026 Kenneth Stott
// Canary: bd13514a-c705-475b-bf21-997c34eaaab5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { NavLink, useLocation, useNavigate } from "react-router-dom";
import { User, Compass } from "lucide-react";
import { CapabilityGate } from "./CapabilityGate";
import { useTour } from "../tour/useTour";
import { RoleSelector } from "./RoleSelector";
import { OrgSwitcher } from "./OrgSwitcher";
import { UserProfileModal } from "./UserProfileModal";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";
import type { Capability } from "../types/auth";

const AUTH_ENABLED = import.meta.env.VITE_AUTH_ENABLED === "true";

interface DropdownItem {
  to: string;
  label: string;
  capability: Capability;
  comingSoon?: boolean;
  separatorBefore?: boolean;
}

interface NavGroup {
  id: string;
  label: string;
  items: DropdownItem[];
}

const NAV_GROUPS: NavGroup[] = [
  {
    id: "model",
    label: "Model",
    items: [
      { to: "/views", label: "Views", capability: "table_registration" },
      { to: "/commands", label: "Commands", capability: "admin" },
    ],
  },
  {
    id: "security",
    label: "Security",
    items: [
      { to: "/security/roles", label: "Roles", capability: "access_config" },
      { to: "/security/rls", label: "RLS Rules", capability: "access_config" },
    ],
  },
  {
    id: "explore",
    label: "Explore",
    items: [
      { to: "/schema", label: "Schema", capability: "query_development" },
      { to: "/nl", label: "NL", capability: "query_development", separatorBefore: true },
      { to: "/query", label: "GraphQL", capability: "query_development" },
      { to: "/graph", label: "Cypher", capability: "query_development" },
      { to: "/sql", label: "SQL", capability: "query_development" },
      { to: "/grpc", label: "gRPC", capability: "query_development" },
      { to: "/jsonapi", label: "JSON:API", capability: "query_development" },
      { to: "/openapi", label: "OpenAPI", capability: "query_development" },
    ],
  },
  {
    id: "admin",
    label: "Admin",
    items: [
      { to: "/admin/orgs", label: "Orgs", capability: "admin" },
      { to: "/admin/overview", label: "Overview", capability: "admin" },
      { to: "/admin/domains", label: "Domains", capability: "admin" },
      { to: "/admin/materialized-views", label: "Materialized Views", capability: "admin" },
      { to: "/admin/cache", label: "Cache", capability: "admin" },
      { to: "/admin/scheduled-tasks", label: "Scheduled Tasks", capability: "admin" },
      { to: "/admin/federation-engine", label: "Federation Engine", capability: "admin" },
      { to: "/admin/cache-storage", label: "Cache & Storage", capability: "admin" },
      { to: "/admin/encryption", label: "Encryption", capability: "admin" },
      { to: "/admin/auth", label: "Authentication", capability: "admin" },
      { to: "/admin/system-health", label: "System Health", capability: "admin" },
      { to: "/admin/observability", label: "Observability", capability: "admin" },
      { to: "/admin/local-users", label: "Local Users", capability: "admin" },
      { to: "/admin/requests", label: "Requests", capability: "admin" },
    ],
  },
];

function activeGroupId(pathname: string): string | null {
  for (const group of NAV_GROUPS) {
    if (
      group.items.some(
        (i) => !i.comingSoon && (pathname === i.to || pathname.startsWith(i.to + "/")),
      )
    ) {
      return group.id;
    }
  }
  return null;
}

export function NavBar() {
  const location = useLocation();
  const navigate = useNavigate();
  const { domains, checkedDomains, toggleDomain, domainsEnabled } = useDomainFilter();
  const { displayName, email, devMode } = useAuth();
  const { startTour } = useTour();
  const [pinnedGroup, setPinnedGroup] = useState<string | null>(null);
  const [domainOpen, setDomainOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const navRef = useRef<HTMLElement>(null);
  const subnavRef = useRef<HTMLElement>(null);
  const domainRef = useRef<HTMLDivElement>(null);
  const userMenuRef = useRef<HTMLDivElement>(null);

  const routeGroup = activeGroupId(location.pathname);

  // When route changes into a group, clear any manual pin so the route drives display
  useEffect(() => {
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       reset internal pin state in sync with an external system (router pathname) */
    setPinnedGroup(null);
  }, [location.pathname]);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (
        pinnedGroup &&
        !navRef.current?.contains(e.target as Node) &&
        !subnavRef.current?.contains(e.target as Node)
      ) {
        setPinnedGroup(null);
      }
      if (domainOpen && !domainRef.current?.contains(e.target as Node)) {
        setDomainOpen(false);
      }
      if (userMenuOpen && !userMenuRef.current?.contains(e.target as Node)) {
        setUserMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [pinnedGroup, domainOpen, userMenuOpen]);

  function handleLogout() {
    localStorage.removeItem("provisa_token");
    localStorage.removeItem("provisa_org");
    setUserMenuOpen(false);
    navigate("/login");
  }

  const displayedGroupId = pinnedGroup ?? routeGroup;
  const displayedGroup = NAV_GROUPS.find((g) => g.id === displayedGroupId) ?? null;

  const onTablesPage =
    location.pathname === "/tables" ||
    location.pathname === "/relationships" ||
    location.pathname.startsWith("/security") ||
    location.pathname === "/schema" ||
    location.pathname === "/query" ||
    location.pathname === "/graph" ||
    location.pathname === "/sql" ||
    location.pathname === "/nl" ||
    location.pathname === "/grpc" ||
    location.pathname === "/jsonapi" ||
    location.pathname === "/openapi";

  function toggleGroup(id: string) {
    // If already in this group's route, just toggle the pin
    if (routeGroup === id) {
      setPinnedGroup((prev) => (prev === id ? null : id));
      return;
    }
    // Navigate to first non-comingSoon item in the group
    const group = NAV_GROUPS.find((g) => g.id === id);
    const first = group?.items.find((i) => !i.comingSoon);
    if (first) navigate(first.to);
    setPinnedGroup(null);
  }

  return (
    <>
      <nav className="navbar" ref={navRef}>
        <div className="navbar-brand">
          <NavLink to="/" aria-label="Provisa home">
            <svg
              className="navbar-brand-mark"
              viewBox="0 0 100 100"
              width="24"
              height="24"
              role="img"
              aria-hidden="true"
            >
              <g fill="currentColor">
                <rect x="30" y="18" width="15" height="64" rx="7" />
                <circle cx="52" cy="35" r="22" />
              </g>
              <circle cx="52" cy="34" r="10.5" fill="var(--surface)" />
              <circle cx="52" cy="34" r="4.5" fill="#10B981" />
            </svg>
            <span>Provisa</span>
          </NavLink>
        </div>
        <div className="navbar-links">
          <CapabilityGate capability="source_registration">
            <NavLink to="/sources" data-tour="nav-sources">Sources</NavLink>
          </CapabilityGate>
          <CapabilityGate capability="table_registration">
            <NavLink to="/tables" data-tour="nav-tables">Tables</NavLink>
          </CapabilityGate>
          <NavLink to="/relationships" data-tour="nav-relationships">Relationships</NavLink>
          {NAV_GROUPS.map((group) => {
            const isActive = routeGroup === group.id || pinnedGroup === group.id;
            return (
              <button
                key={group.id}
                data-tour={`nav-${group.id}`}
                className={`nav-group-label${isActive ? " nav-group-active" : ""}`}
                onClick={() => toggleGroup(group.id)}
              >
                {group.label}
              </button>
            );
          })}
        </div>
        <div className="navbar-role">
          <OrgSwitcher />
          {domainsEnabled && onTablesPage && domains.length > 0 && (
            <div className="navbar-domain-wrapper" ref={domainRef}>
              <button className="navbar-domain-btn" onClick={() => setDomainOpen((o) => !o)}>
                Domains ({checkedDomains.size}/{domains.length}) ▾
              </button>
              {domainOpen && (
                <div className="navbar-domain-panel">
                  {domains.map((d) => (
                    <label key={d} className="navbar-domain-item">
                      <input
                        type="checkbox"
                        checked={checkedDomains.has(d)}
                        onChange={() => toggleDomain(d)}
                      />
                      {d}
                    </label>
                  ))}
                </div>
              )}
            </div>
          )}
          <RoleSelector />
          <button
            className="navbar-tour-btn"
            title="Take a guided tour"
            aria-label="Take a guided tour"
            onClick={() => startTour()}
          >
            <Compass size={16} />
          </button>
          <div className="navbar-user-wrapper" ref={userMenuRef}>
            <button
              className="navbar-user-btn"
              onClick={() => setUserMenuOpen((o) => !o)}
              title={displayName ?? email ?? "User menu"}
            >
              <User size={16} />
            </button>
            {userMenuOpen && (
              <div className="navbar-user-panel">
                {(displayName || email) && (
                  <div className="navbar-user-identity">
                    {displayName && <span className="navbar-user-name">{displayName}</span>}
                    {email && <span className="navbar-user-email">{email}</span>}
                    {devMode && <span className="navbar-user-dev">DEV</span>}
                  </div>
                )}
                <button
                  className="navbar-user-item"
                  onClick={() => {
                    setProfileOpen(true);
                    setUserMenuOpen(false);
                  }}
                >
                  Profile
                </button>
                <CapabilityGate capability="admin">
                  <button
                    className="navbar-user-item"
                    onClick={() => {
                      navigate("/admin/overview");
                      setUserMenuOpen(false);
                    }}
                  >
                    Settings
                  </button>
                </CapabilityGate>
                {AUTH_ENABLED && (
                  <button className="navbar-user-item navbar-user-logout" onClick={handleLogout}>
                    Logout
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      </nav>
      {displayedGroup && (
        <nav className="subnav" ref={subnavRef}>
          {displayedGroup.items.map((item) => (
            <span key={item.to} className="subnav-item-wrapper">
              {item.separatorBefore && <span className="subnav-sep">|</span>}
              {item.comingSoon ? (
                <span className="subnav-coming-soon">{item.label} — coming soon</span>
              ) : (
                <CapabilityGate capability={item.capability}>
                  <NavLink to={item.to}>{item.label}</NavLink>
                </CapabilityGate>
              )}
            </span>
          ))}
        </nav>
      )}
      {profileOpen && <UserProfileModal onClose={() => setProfileOpen(false)} />}
    </>
  );
}
