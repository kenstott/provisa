// Copyright (c) 2026 Kenneth Stott
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
  const { capabilities, orgMemberships, activeOrgId, selectOrg } = useAuth();
  const [allOrgs, setAllOrgs] = useState<Org[]>([]);

  const isSuperAdmin = capabilities.includes("superadmin") || capabilities.includes("admin");

  useEffect(() => {
    if (!isSuperAdmin) return;
    fetchOrgs().then(setAllOrgs).catch(() => {});
  }, [isSuperAdmin]);

  const orgs: Array<{ id: string; name: string }> = isSuperAdmin
    ? allOrgs.map((o) => ({ id: o.id, name: o.name }))
    : orgMemberships.map((m) => ({ id: m.org_id, name: m.org_name }));

  const activeOrg = orgs.find((o) => o.id === activeOrgId);
  const orgName = activeOrg?.name ?? activeOrgId ?? "";

  if (!isSuperAdmin && orgMemberships.length <= 1) {
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
