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
import { useSubnavExtraSlot } from "../context/subnavExtraSlot";
import { useAuth } from "../context/AuthContext";
import { clearSessionState } from "../lib/session";
import { NAV_GROUPS, entryItem, writeLastSubnav } from "./navGroups";
import { hasCapability } from "../lib/capabilities";

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
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();
  const { domains, checkedDomains, toggleDomain, domainsEnabled } = useDomainFilter();
  const { displayName, email, devMode, authEnabled, capabilities, billing } = useAuth();
  const { startTour, canResume, status: tourStatus } = useTour();
  const { setNode: setSubnavExtraNode } = useSubnavExtraSlot();
  const [pinnedGroup, setPinnedGroup] = useState<string | null>(null);
  const [profileOpen, setProfileOpen] = useState(false);
  const navRef = useRef<HTMLElement>(null);
  const subnavRef = useRef<HTMLElement>(null);

  const routeGroup = activeGroupId(location.pathname);
  const adminGroup = NAV_GROUPS.find((g) => g.id === "admin");
  const adminEntry = adminGroup ? entryItem(adminGroup, capabilities) : undefined;

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
        !i.comingSoon && (location.pathname === i.to || location.pathname.startsWith(i.to + "/")),
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

  async function handleLogout() {
    // Clear the Firebase session too, or signInWithPopup silently reuses the persisted
    // Google account on the next login and never offers the account chooser.
    const { signOutFirebase } = await import("../lib/firebase");
    await signOutFirebase();
    // REQ-1326: sign-out clears exactly what sign-in clears — token, org, role and the persisted
    // Apollo snapshot. Clearing a subset left provisa_role and the cached org-scoped admin data
    // behind for the next identity.
    clearSessionState();
    // Full document load, not navigate(): App reads the token only on an authVersion bump (login
    // path), so an in-app navigate would keep the shell mounted and render /login inside the
    // navbar. A hard load re-reads the token-less localStorage into the public LandingPage branch
    // and drops the Apollo/auth state built for the signed-in session.
    window.location.assign("/");
  }

  const displayedGroupId = pinnedGroup ?? routeGroup;
  const displayedGroup = NAV_GROUPS.find((g) => g.id === displayedGroupId) ?? null;

  const onTablesPage =
    location.pathname === "/tables" ||
    location.pathname === "/views" ||
    location.pathname === "/commands" ||
    location.pathname === "/lineage" ||
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
    // Navigate to the last-visited PERMITTED item in the group (or the first permitted one)
    const group = NAV_GROUPS.find((g) => g.id === id);
    const target = group ? entryItem(group, capabilities) : undefined;
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
            <NavLink to="/sources" data-tour="nav-sources">
              {t("navBar.sources")}
            </NavLink>
          </CapabilityGate>
          <CapabilityGate capability="table_registration">
            <NavLink to="/tables" data-tour="nav-tables">
              {t("navBar.tables")}
            </NavLink>
          </CapabilityGate>
          <NavLink to="/relationships" data-tour="nav-relationships">
            {t("navBar.relationships")}
          </NavLink>
          <CapabilityGate capability="org_settings">
            <NavLink to="/admin/glossary" data-tour="nav-glossary">
              {t("navBar.itemGlossary")}
            </NavLink>
          </CapabilityGate>
          {/* REQ-1351: a group whose every item is denied has nowhere to go, so it is not offered —
              rendering it left a tab that swallowed the click and never navigated. */}
          {NAV_GROUPS.filter((group) => entryItem(group, capabilities)).map((group) => {
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
          <NavLink to="/docs" data-tour="nav-docs">
            {t("navBar.docs")}
          </NavLink>
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
              // The launch prefetch can run for seconds on a loaded machine; the button itself has
              // to show that the click landed, or it gets clicked again while the tour is starting.
              loading={tourStatus?.kind === "preparing"}
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
                    {displayName && (
                      <Text size="sm" fw={600}>
                        {displayName}
                      </Text>
                    )}
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
                {/* REQ-1349: shown when the caller has ANY admin surface, and it opens the first
                    one they hold — gating on the `admin` wildcard hid it from every org_admin, and
                    the hardcoded /admin/overview needs `observability` the wildcard does not imply. */}
                {adminEntry && (
                  <Menu.Item onClick={() => navigate(adminEntry.to)}>
                    {t("navBar.settings")}
                  </Menu.Item>
                )}
                {/* REQ-1469: the plan, the running bill and the next charge are the org's
                    commercial relationship, not an operational setting, so they sit with the
                    account rather than under Admin. Shown only where the deployment mounts
                    /billing (`billing` on /auth/me) — the right exists in every deployment, the
                    routes do not — and only to the org right that owns the subscription. */}
                {billing && hasCapability(capabilities, "org_settings") && (
                  <Menu.Item
                    onClick={() => navigate("/admin/billing")}
                    data-testid="navbar-billing"
                  >
                    {t("navBar.itemBilling")}
                  </Menu.Item>
                )}
                {authEnabled && (
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
          <div className="subnav-extra" ref={setSubnavExtraNode} />
        </nav>
      )}
      {profileOpen && <UserProfileModal onClose={() => setProfileOpen(false)} />}
    </>
  );
}
