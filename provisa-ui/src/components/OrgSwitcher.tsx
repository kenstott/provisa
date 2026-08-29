// Copyright (c) 2026 Kenneth Stott
// Canary: 50b5c7a7-1c30-467c-bf54-54a96f7c5dec
// Canary: placeholder

import { Button, Menu, Text } from "@mantine/core";
import { Check, ChevronDown } from "lucide-react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";

export function OrgSwitcher() {
  const { t } = useTranslation();
  const { orgMemberships, activeOrgId, selectOrg, multitenancy } = useAuth();

  // REQ-1605: this switcher scopes the data plane (Team, sources, branding, ...) to an org the
  // caller can actually read. Holding cross_org (platform_admin) lets an identity ACT in any org
  // via dedicated admin surfaces, but must never list an org here that it holds no admin-plane
  // membership row in — that membership is exactly what the data-plane endpoints require.
  // REQ-1602: the default org displays as "Platform" in multi-tenant mode (control plane label)
  const orgs: Array<{ id: string; name: string }> = orgMemberships.map((m) => ({
    id: m.org_id,
    name: multitenancy && m.org_id === "default" ? "Platform" : m.org_name,
  }));

  // A single-tenant deployment has exactly one org, so naming it in the navbar tells the reader
  // nothing they could act on and nothing they could change. The switcher is a multi-tenancy
  // control; without multi-tenancy there is nothing to switch between.
  if (!multitenancy) return null;

  const activeOrg = orgs.find((o) => o.id === activeOrgId);
  const orgName = activeOrg?.name ?? activeOrgId ?? "";

  if (orgMemberships.length <= 1) {
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
