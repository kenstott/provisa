// Copyright (c) 2026 Kenneth Stott
// Canary: 5d96f9dd-29c6-4a4e-b32c-009c45f1dc2b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Tabs } from "@mantine/core";
import { SecurityTab } from "./SecurityTab";
import { EncryptionTab } from "./EncryptionTab";
import { AuthTab } from "./AuthTab";
import { LocalUsersTab } from "./LocalUsersTab";
import { SecretsTab } from "./SecretsTab";
import { useCapability } from "../../hooks/useCapability";

// Consolidated Security area (cache-page style sub-tabs): posture, encryption, authentication,
// local users, and the org's secrets all live under one Security section.
const TAB_KEYS = ["posture", "encryption", "authentication", "localUsers", "secrets"] as const;
type TabKey = (typeof TAB_KEYS)[number];

// REQ-1558, REQ-1361: which capability each sub-tab answers to. The first four describe the
// DEPLOYMENT, so they are the platform administrator's. Secrets are the ORG'S -- an org_admin
// manages them and a platform admin has no read of their values -- so the two sets are gated
// separately even though they share a section. Nobody is shown a tab their capability does not
// carry, and the section is not shown at all when neither set is theirs.
const TAB_CAPABILITY = {
  posture: "platform_settings",
  encryption: "platform_settings",
  authentication: "platform_settings",
  localUsers: "platform_settings",
  secrets: "org_settings",
} as const;

interface SecurityManagerProps {
  allRoles: string[];
  allDomains: string[];
  /** Sub-tab to open first (deep-linked from legacy /admin/encryption, /admin/auth, …). */
  initialTab?: TabKey;
}

export function SecurityManager({ allRoles, allDomains, initialTab }: SecurityManagerProps) {
  const { t } = useTranslation();
  const deployment = useCapability("platform_settings");
  const org = useCapability("org_settings");
  const visible = useMemo(
    () => TAB_KEYS.filter((k) => (TAB_CAPABILITY[k] === "org_settings" ? org : deployment)),
    [deployment, org],
  );
  // An initialTab the caller deep-linked to is honoured only when it is one this person may see;
  // otherwise the first tab they may see opens. `visible` is never empty here -- the route itself
  // is gated, so a person with neither capability never reaches this component.
  const first = visible[0];
  const [tab, setTab] = useState<TabKey>(
    initialTab && visible.includes(initialTab) ? initialTab : first,
  );
  const active = visible.includes(tab) ? tab : first;
  return (
    <div>
      <Tabs value={active} onChange={(v) => setTab((v as TabKey) ?? first)} mb="md">
        <Tabs.List>
          {visible.map((k) => (
            <Tabs.Tab key={k} value={k} data-testid={`security-tab-${k}`}>
              {t(`securityManager.tabs.${k}`)}
            </Tabs.Tab>
          ))}
        </Tabs.List>
      </Tabs>
      {active === "posture" && <SecurityTab />}
      {active === "encryption" && <EncryptionTab />}
      {active === "authentication" && <AuthTab />}
      {active === "localUsers" && <LocalUsersTab allRoles={allRoles} allDomains={allDomains} />}
      {active === "secrets" && <SecretsTab />}
    </div>
  );
}
