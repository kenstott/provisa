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
import { User, Compass, ChevronDown } from "lucide-react";
import { ActionIcon, Badge, Checkbox, Menu, Stack, Text, Tooltip } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { CapabilityGate } from "./CapabilityGate";
import { useTour } from "../tour/useTour";
import { RoleSelector } from "./RoleSelector";
import { OrgSwitcher } from "./OrgSwitcher";
import { ColorSchemeToggle } from "../theme/ColorSchemeToggle";
import { UserProfileModal } from "./UserProfileModal";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useAuth } from "../context/AuthContext";
import type { Capability } from "../types/auth";

const AUTH_ENABLED = import.meta.env.VITE_AUTH_ENABLED === "true";

interface DropdownItem {
  to: string;
  labelKey: string;
  capability: Capability;
  comingSoon?: boolean;
  separatorBefore?: boolean;
}

interface NavGroup {
  id: string;
  labelKey: string;
  items: DropdownItem[];
}

const NAV_GROUPS: NavGroup[] = [
  {
    id: "model",
    labelKey: "navBar.groupModel",
    items: [
      { to: "/views", labelKey: "navBar.itemViews", capability: "table_registration" },
      { to: "/commands", labelKey: "navBar.itemCommands", capability: "admin" },
      { to: "/lineage", labelKey: "navBar.itemLineage", capability: "admin" }, // REQ-1160/1161
    ],
  },
  {
    id: "security",
    labelKey: "navBar.groupSecurity",
    items: [
      { to: "/security/roles", labelKey: "navBar.itemRoles", capability: "access_config" },
      { to: "/security/rls", labelKey: "navBar.itemRlsRules", capability: "access_config" },
    ],
  },
  {
    id: "explore",
    labelKey: "navBar.groupExplore",
    items: [
      { to: "/schema", labelKey: "navBar.itemSchema", capability: "query_development" },
      {
        to: "/nl",
        labelKey: "navBar.itemNl",
        capability: "query_development",
        separatorBefore: true,
      },
      { to: "/query", labelKey: "navBar.itemGraphql", capability: "query_development" },
      { to: "/graph", labelKey: "navBar.itemCypher", capability: "query_development" },
      { to: "/sql", labelKey: "navBar.itemSql", capability: "query_development" },
      { to: "/grpc", labelKey: "navBar.itemGrpc", capability: "query_development" },
      { to: "/jsonapi", labelKey: "navBar.itemJsonApi", capability: "query_development" },
      { to: "/openapi", labelKey: "navBar.itemOpenApi", capability: "query_development" },
      { to: "/explore", labelKey: "navBar.itemExplore", capability: "query_development" },
    ],
  },
  {
    id: "admin",
    labelKey: "navBar.groupAdmin",
    items: [
      { to: "/admin/orgs", labelKey: "navBar.itemOrgs", capability: "admin" },
      { to: "/admin/overview", labelKey: "navBar.itemOverview", capability: "admin" },
      { to: "/admin/domains", labelKey: "navBar.itemDomains", capability: "admin" },
      { to: "/admin/cache", labelKey: "navBar.itemCache", capability: "admin" },
      { to: "/admin/scheduled-tasks", labelKey: "navBar.itemScheduler", capability: "admin" },
      {
        to: "/admin/federation-engine",
        labelKey: "navBar.itemFederation",
        capability: "admin",
      },
      { to: "/admin/security", labelKey: "navBar.itemSecurity", capability: "admin" },
      { to: "/admin/ai-models", labelKey: "navBar.itemAiModels", capability: "admin" },
      { to: "/admin/system-health", labelKey: "navBar.itemHealth", capability: "admin" },
      { to: "/admin/observability", labelKey: "navBar.itemObservability", capability: "admin" },
      { to: "/admin/mcp-server", labelKey: "navBar.itemMcpServer", capability: "admin" },
      { to: "/admin/requests", labelKey: "navBar.itemRequests", capability: "admin" },
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

// Remembers the last submenu item visited within each group so returning to a
// group restores that item instead of always landing on the first one.
const LAST_SUBNAV_KEY = "provisa_nav_last_item";

function readLastSubnav(): Record<string, string> {
  const raw = localStorage.getItem(LAST_SUBNAV_KEY);
  if (!raw) return {};
  const parsed: unknown = JSON.parse(raw);
  return parsed && typeof parsed === "object" ? (parsed as Record<string, string>) : {};
}

function writeLastSubnav(groupId: string, to: string) {
  localStorage.setItem(LAST_SUBNAV_KEY, JSON.stringify({ ...readLastSubnav(), [groupId]: to }));
}

// The submenu item within a group to navigate to on entry: the remembered one
// (if still valid and available) or the first non-comingSoon item.
function entryItem(group: NavGroup): DropdownItem | undefined {
  const remembered = readLastSubnav()[group.id];
  const match = group.items.find((i) => !i.comingSoon && i.to === remembered);
  return match ?? group.items.find((i) => !i.comingSoon);
}

export function NavBar() {
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();
  const { domains, checkedDomains, toggleDomain, domainsEnabled } = useDomainFilter();
  const { displayName, email, devMode } = useAuth();
  const { startTour, canResume } = useTour();
  const [pinnedGroup, setPinnedGroup] = useState<string | null>(null);
  const [profileOpen, setProfileOpen] = useState(false);
  const navRef = useRef<HTMLElement>(null);
  const subnavRef = useRef<HTMLElement>(null);

  const routeGroup = activeGroupId(location.pathname);

  // When route changes into a group, clear any manual pin so the route drives display
  useEffect(() => {
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       reset internal pin state in sync with an external system (router pathname) */
    setPinnedGroup(null);
  }, [location.pathname]);

  // Remember the submenu item the route landed on, per group, for later restore
  useEffect(() => {
    const group = NAV_GROUPS.find((g) => g.id === routeGroup);
    const item = group?.items.find(
      (i) =>
        !i.comingSoon &&
        (location.pathname === i.to || location.pathname.startsWith(i.to + "/")),
    );
    if (group && item) writeLastSubnav(group.id, item.to);
  }, [location.pathname, routeGroup]);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (
        pinnedGroup &&
        !navRef.current?.contains(e.target as Node) &&
        !subnavRef.current?.contains(e.target as Node)
      ) {
        setPinnedGroup(null);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [pinnedGroup]);

  function handleLogout() {
    localStorage.removeItem("provisa_token");
    localStorage.removeItem("provisa_org");
    navigate("/login");
  }

  const displayedGroupId = pinnedGroup ?? routeGroup;
  const displayedGroup = NAV_GROUPS.find((g) => g.id === displayedGroupId) ?? null;

  const onTablesPage =
    location.pathname === "/tables" ||
    location.pathname === "/views" ||
    location.pathname === "/commands" ||
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
    // Navigate to the last-visited item in the group (or the first, if none)
    const group = NAV_GROUPS.find((g) => g.id === id);
    const target = group ? entryItem(group) : undefined;
    if (target) navigate(target.to);
    setPinnedGroup(null);
  }

  return (
    <>
      <nav className="navbar" ref={navRef}>
        <div className="navbar-brand">
          <NavLink to="/" aria-label={t("navBar.home")}>
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
              <circle cx="52" cy="35" r="10.5" fill="var(--surface)" />
              <circle cx="52" cy="35" r="4.5" fill="#10B981" />
            </svg>
            <span>{t("navBar.brand")}</span>
          </NavLink>
        </div>
        <div className="navbar-links">
          <CapabilityGate capability="source_registration">
            <NavLink to="/sources" data-tour="nav-sources">{t("navBar.sources")}</NavLink>
          </CapabilityGate>
          <CapabilityGate capability="table_registration">
            <NavLink to="/tables" data-tour="nav-tables">{t("navBar.tables")}</NavLink>
          </CapabilityGate>
          <NavLink to="/relationships" data-tour="nav-relationships">
            {t("navBar.relationships")}
          </NavLink>
          {NAV_GROUPS.map((group) => {
            const isActive = routeGroup === group.id || pinnedGroup === group.id;
            return (
              <button
                key={group.id}
                type="button"
                data-tour={`nav-${group.id}`}
                data-testid={`nav-group-${group.id}`}
                className={`nav-group-label${isActive ? " nav-group-active" : ""}`}
                aria-expanded={isActive}
                aria-current={isActive ? "true" : undefined}
                onClick={() => toggleGroup(group.id)}
              >
                {t(group.labelKey)}
              </button>
            );
          })}
          {/* Docs — ungated, available to everyone */}
          <NavLink to="/docs" data-tour="nav-docs">{t("navBar.docs")}</NavLink>
        </div>
        <div className="navbar-role">
          <OrgSwitcher />
          {domainsEnabled && onTablesPage && domains.length > 0 && (
            <div className="navbar-domain-wrapper">
              <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }}>
                <Menu.Target>
                  <button
                    type="button"
                    className="navbar-domain-btn"
                    data-testid="navbar-domain-trigger"
                  >
                    {t("navBar.domainsToggle", {
                      checked: checkedDomains.size,
                      total: domains.length,
                    })}
                    <ChevronDown size={14} aria-hidden />
                  </button>
                </Menu.Target>
                <Menu.Dropdown>
                  <Menu.Label>{t("navBar.domainsLabel")}</Menu.Label>
                  <Stack gap={4} px="sm" pb="xs">
                    {domains.map((d) => (
                      <Checkbox
                        key={d}
                        label={d}
                        data-testid={`navbar-domain-item-${d}`}
                        checked={checkedDomains.has(d)}
                        onChange={() => toggleDomain(d)}
                      />
                    ))}
                  </Stack>
                </Menu.Dropdown>
              </Menu>
            </div>
          )}
          <RoleSelector />
          <ColorSchemeToggle />
          <Tooltip label={canResume ? t("navBar.tourResume") : t("navBar.tourStart")}>
            <ActionIcon
              variant="default"
              size="lg"
              aria-label={canResume ? t("navBar.tourResume") : t("navBar.tourStart")}
              className="navbar-tour-btn"
              onClick={() => startTour()}
            >
              <Compass size={16} aria-hidden />
            </ActionIcon>
          </Tooltip>
          <div className="navbar-user-wrapper">
            <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }}>
              <Menu.Target>
                <ActionIcon
                  variant="default"
                  size="lg"
                  className="navbar-user-btn"
                  aria-label={displayName ?? email ?? t("navBar.userMenu")}
                  data-testid="navbar-user-trigger"
                >
                  <User size={16} aria-hidden />
                </ActionIcon>
              </Menu.Target>
              <Menu.Dropdown>
                {(displayName || email) && (
                  <Menu.Label>
                    {displayName && <Text size="sm" fw={600}>{displayName}</Text>}
                    {email && (
                      <Text size="xs" c="dimmed">
                        {email}
                      </Text>
                    )}
                    {devMode && (
                      <Badge mt="xs" size="xs" color="orange" variant="filled" autoContrast>
                        {t("navBar.dev")}
                      </Badge>
                    )}
                  </Menu.Label>
                )}
                <Menu.Item onClick={() => setProfileOpen(true)}>{t("navBar.profile")}</Menu.Item>
                <CapabilityGate capability="admin">
                  <Menu.Item onClick={() => navigate("/admin/overview")}>
                    {t("navBar.settings")}
                  </Menu.Item>
                </CapabilityGate>
                {AUTH_ENABLED && (
                  <Menu.Item color="red" onClick={handleLogout}>
                    {t("navBar.logout")}
                  </Menu.Item>
                )}
              </Menu.Dropdown>
            </Menu>
          </div>
        </div>
      </nav>
      {displayedGroup && (
        <nav className="subnav" ref={subnavRef}>
          {displayedGroup.items.map((item) => (
            <span key={item.to} className="subnav-item-wrapper">
              {item.separatorBefore && <span className="subnav-sep">|</span>}
              {item.comingSoon ? (
                <span className="subnav-coming-soon">
                  {t("navBar.comingSoon", { label: t(item.labelKey) })}
                </span>
              ) : (
                <CapabilityGate capability={item.capability}>
                  <NavLink to={item.to}>{t(item.labelKey)}</NavLink>
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
