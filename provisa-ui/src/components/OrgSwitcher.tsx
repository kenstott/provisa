// Copyright (c) 2026 Kenneth Stott
// Canary: 50b5c7a7-1c30-467c-bf54-54a96f7c5dec
// Canary: placeholder

import { useEffect, useState } from "react";
import { Button, Menu, Text } from "@mantine/core";
import { Check, ChevronDown } from "lucide-react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";
import { fetchOrgs } from "../api/admin";
import type { Org } from "../api/admin";

export function OrgSwitcher() {
  const { t } = useTranslation();
  const { capabilities, orgMemberships, activeOrgId, selectOrg, multitenancy } = useAuth();
  const [allOrgs, setAllOrgs] = useState<Org[]>([]);

  // REQ-1337: listing every org is the cross_org RIGHT, not a role name. The seed decides who
  // holds it (platform_admin always; org_admin never, in either tenancy mode).
  const canSeeAllOrgs = capabilities.includes("cross_org");

  useEffect(() => {
    if (!canSeeAllOrgs) return;
    fetchOrgs()
      .then(setAllOrgs)
      .catch(() => {});
  }, [canSeeAllOrgs]);

  const orgs: Array<{ id: string; name: string }> = canSeeAllOrgs
    ? allOrgs.map((o) => ({ id: o.id, name: o.name }))
    : orgMemberships.map((m) => ({ id: m.org_id, name: m.org_name }));

  // A single-tenant deployment has exactly one org, so naming it in the navbar tells the reader
  // nothing they could act on and nothing they could change. The switcher is a multi-tenancy
  // control; without multi-tenancy there is nothing to switch between.
  if (!multitenancy) return null;

  const activeOrg = orgs.find((o) => o.id === activeOrgId);
  const orgName = activeOrg?.name ?? activeOrgId ?? "";

  if (!canSeeAllOrgs && orgMemberships.length <= 1) {
    if (orgMemberships.length === 0) return null;
    return <Text data-testid="org-switcher-static">{t("orgSwitcher.org", { org: orgName })}</Text>;
  }

  function handleSelect(orgId: string) {
    selectOrg(orgId);
  }

  return (
    <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }}>
      <Menu.Target>
        <Button
          variant="default"
          size="compact-sm"
          rightSection={<ChevronDown size={14} aria-hidden />}
          data-testid="org-switcher-trigger"
        >
          {t("orgSwitcher.org", { org: orgName })}
        </Button>
      </Menu.Target>
      <Menu.Dropdown>
        {orgs.map((o) => {
          const selected = o.id === activeOrgId;
          return (
            <Menu.Item
              key={o.id}
              role="option"
              aria-selected={selected}
              aria-current={selected ? "true" : undefined}
              leftSection={selected ? <Check size={14} aria-hidden /> : undefined}
              onClick={() => handleSelect(o.id)}
            >
              {o.name}
            </Menu.Item>
          );
        })}
      </Menu.Dropdown>
    </Menu>
  );
}
