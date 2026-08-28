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
import {
  ActionIcon,
  Badge,
  Button,
  Checkbox,
  Menu,
  Modal,
  Stack,
  Text,
  Tooltip,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import { BrandMark } from "./BrandMark";
import { CapabilityGate } from "./CapabilityGate";
import { useTour } from "../tour/useTour";
import { RoleSelector } from "./RoleSelector";
import { OrgSwitcher } from "./OrgSwitcher";
import { EnvSwitcher } from "./EnvSwitcher";
import { ColorSchemeToggle } from "../theme/ColorSchemeToggle";
import { UserProfileModal } from "./UserProfileModal";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useSubnavExtraSlot } from "../context/subnavExtraSlot";
import { useAuth } from "../context/AuthContext";
import { signOut } from "../lib/session";
import { NAV_GROUPS, activeGroupId, entryItem, labelKeyFor, writeLastSubnav } from "./navGroups";
import { hasCapability } from "../lib/capabilities";

export function NavBar() {
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();
  const { domains, checkedDomains, toggleDomain, domainsEnabled } = useDomainFilter();
  const { displayName, email, devMode, authEnabled, capabilities, billing, activeOrg } = useAuth();
  const { startTour, canResume, status: tourStatus, available: tourAvailable } = useTour();
  const { setNode: setSubnavExtraNode } = useSubnavExtraSlot();
  const [pinnedGroup, setPinnedGroup] = useState<string | null>(null);
  const [profileOpen, setProfileOpen] = useState(false);
  const [showUpgradeModal, setShowUpgradeModal] = useState(false);
  const navRef = useRef<HTMLElement>(null);
  const subnavRef = useRef<HTMLElement>(null);

  const routeGroup = activeGroupId(location.pathname);
  const adminGroup = NAV_GROUPS.find((g) => g.id === "admin");
  const adminEntry = adminGroup ? entryItem(adminGroup, capabilities, billing) : undefined;

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
    if (activeOrg === "sandbox") {
      setShowUpgradeModal(true);
    } else {
      await signOut();
    }
  }

  async function proceedWithLogout() {
    setShowUpgradeModal(false);
    await signOut();
  }

  const displayedGroupId = pinnedGroup ?? routeGroup;
  const displayedGroup = NAV_GROUPS.find((g) => g.id === displayedGroupId) ?? null;

  // Every route whose content the domain selection narrows. A page that READS the filter must
  // offer it here, or it obeys a narrowing the reader cannot see or undo from where they are.
  const onDomainFilteredPage =
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
    location.pathname === "/openapi" ||
    // REQ-1591: the glossary list is narrowed by the same selection, and its create modal makes a
    // term's domains from it.
    location.pathname === "/admin/glossary";

  function toggleGroup(id: string) {
    // If already in this group's route, just toggle the pin
    if (routeGroup === id) {
      setPinnedGroup((prev) => (prev === id ? null : id));
      return;
    }
    // Navigate to the last-visited PERMITTED item in the group (or the first permitted one)
    const group = NAV_GROUPS.find((g) => g.id === id);
    const target = group ? entryItem(group, capabilities, billing) : undefined;
    if (target) navigate(target.to);
    setPinnedGroup(null);
  }

  return (
    <>
      <nav className="navbar" ref={navRef}>
        <div className="navbar-brand">
          <NavLink to="/" aria-label={t("navBar.home")}>
            <BrandMark className="navbar-brand-mark" />
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
          {/* Same capability the /relationships route gates on — an ungated link led an analyst
              straight to "You do not have permission to view this page." */}
          <CapabilityGate capability="create_relationship">
            <NavLink to="/relationships" data-tour="nav-relationships">
              {t("navBar.relationships")}
            </NavLink>
          </CapabilityGate>
          {/* REQ-1590: gated on the glossary's own read right, not org_settings — every seeded
              role reads the vocabulary the model is described in. */}
          <CapabilityGate capability="glossary_read">
            <NavLink to="/admin/glossary" data-tour="nav-glossary">
              {t("navBar.itemGlossary")}
            </NavLink>
          </CapabilityGate>
          {/* REQ-1351: a group whose every item is denied has nowhere to go, so it is not offered —
              rendering it left a tab that swallowed the click and never navigated. */}
          {NAV_GROUPS.filter((group) => entryItem(group, capabilities, billing)).map((group) => {
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
          {domainsEnabled && onDomainFilteredPage && domains.length > 0 && (
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
          <EnvSwitcher />
          <RoleSelector />
          <ColorSchemeToggle />
          {/* No launcher where the viewer's rights open none of the tour's pages — a button
              whose tour would be empty is worse than no button. */}
          {tourAvailable && (
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
          )}
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
      {/* REQ-1559: Admin's entries are drawn as a left-hand rail (AdminRail) instead, because two
          dozen of them only crowd a horizontal bar. One navigation, in one place, never both. */}
      {displayedGroup && displayedGroup.id !== "admin" && (
        <nav className="subnav" ref={subnavRef}>
          {displayedGroup.items
            .filter((item) => !(item.commercial && !billing) && !(item.installedOnly && billing))
            .map((item) => (
              <span key={item.to} className="subnav-item-wrapper">
                {item.separatorBefore && <span className="subnav-sep">|</span>}
                {item.comingSoon ? (
                  <span className="subnav-coming-soon">
                    {t("navBar.comingSoon", { label: t(item.labelKey) })}
                  </span>
                ) : (
                  <CapabilityGate
                    capability={item.capability}
                    strict={item.strict}
                    orCapability={item.orCapability}
                    navigable
                  >
                    <NavLink to={item.to}>{t(labelKeyFor(item, capabilities))}</NavLink>
                  </CapabilityGate>
                )}
              </span>
            ))}
          <div className="subnav-extra" ref={setSubnavExtraNode} />
        </nav>
      )}
      {profileOpen && <UserProfileModal onClose={() => setProfileOpen(false)} />}
      <Modal
        opened={showUpgradeModal}
        onClose={() => setShowUpgradeModal(false)}
        title="Ready to Build?"
        centered
        size="sm"
      >
        <Stack gap="md">
          <Text size="sm">
            Your sandbox account is temporary and will be deleted after 30 days of inactivity. You
            can return to it anytime before then.
          </Text>
          <Text size="sm">
            Ready to create your own organization and start with a Starter plan? You'll get 14 days
            free.
          </Text>
          <Stack gap="xs">
            <Button fullWidth onClick={() => navigate("/admin/orgs")}>
              Create Organization
            </Button>
            <Button fullWidth variant="default" onClick={proceedWithLogout}>
              Just Log Out
            </Button>
          </Stack>
        </Stack>
      </Modal>
    </>
  );
}
