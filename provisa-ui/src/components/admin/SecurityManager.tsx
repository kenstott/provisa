// Copyright (c) 2026 Kenneth Stott
// Canary: 5d96f9dd-29c6-4a4e-b32c-009c45f1dc2b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Tabs } from "@mantine/core";
import { SecurityTab } from "./SecurityTab";
import { EncryptionTab } from "./EncryptionTab";
import { AuthTab } from "./AuthTab";
import { LocalUsersTab } from "./LocalUsersTab";

// Consolidated Security area (cache-page style sub-tabs): posture, encryption, authentication,
// and local users all live under one Security section.
const TAB_KEYS = ["posture", "encryption", "authentication", "localUsers"] as const;
type TabKey = (typeof TAB_KEYS)[number];

interface SecurityManagerProps {
  allRoles: string[];
  allDomains: string[];
  /** Sub-tab to open first (deep-linked from legacy /admin/encryption, /admin/auth, …). */
  initialTab?: TabKey;
}

export function SecurityManager({ allRoles, allDomains, initialTab }: SecurityManagerProps) {
  const { t } = useTranslation();
  const [tab, setTab] = useState<TabKey>(initialTab ?? "posture");
  return (
    <div>
      <Tabs value={tab} onChange={(v) => setTab((v as TabKey) ?? "posture")} mb="md">
        <Tabs.List>
          {TAB_KEYS.map((k) => (
            <Tabs.Tab key={k} value={k} data-testid={`security-tab-${k}`}>
              {t(`securityManager.tabs.${k}`)}
            </Tabs.Tab>
          ))}
        </Tabs.List>
      </Tabs>
      {tab === "posture" && <SecurityTab />}
      {tab === "encryption" && <EncryptionTab />}
      {tab === "authentication" && <AuthTab />}
      {tab === "localUsers" && <LocalUsersTab allRoles={allRoles} allDomains={allDomains} />}
    </div>
  );
}
