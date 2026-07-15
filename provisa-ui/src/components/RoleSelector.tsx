// Copyright (c) 2026 Kenneth Stott
// Canary: b7e1733a-e373-4838-b104-3007b8107524
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Badge, Button, Menu } from "@mantine/core";
import { Check, ChevronDown } from "lucide-react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";
import type { Role } from "../types/auth";

/** Role picker for the nav bar. Backed by Mantine Menu so it gains keyboard
 *  navigation, focus management, and correct `menuitem` roles (REQ-1013). */
export function RoleSelector() {
  const { t } = useTranslation();
  const { selectedRole, availableRoles, selectRole, devMode } = useAuth();

  if (availableRoles.length === 0) return <span>{t("roleSelector.none")}</span>;

  const allSelected = selectedRole === "all";
  const label = allSelected ? t("roleSelector.all") : (selectedRole as Role).id;

  function handleSelect(value: Role | "all") {
    selectRole(value);
  }

  return (
    <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }}>
      <Menu.Target>
        <Button
          variant="default"
          size="compact-sm"
          rightSection={<ChevronDown size={14} aria-hidden />}
          data-testid="role-selector-trigger"
        >
          {t("roleSelector.role", { role: label })}
          {devMode && (
            <Badge ml="xs" size="xs" color="orange" variant="filled" autoContrast>
              {t("roleSelector.dev")}
            </Badge>
          )}
        </Button>
      </Menu.Target>
      <Menu.Dropdown>
        <Menu.Item
          aria-label={t("roleSelector.all")}
          aria-current={allSelected ? "true" : undefined}
          leftSection={allSelected ? <Check size={14} aria-hidden /> : undefined}
          onClick={() => handleSelect("all")}
        >
          {t("roleSelector.all")}
        </Menu.Item>
        {availableRoles.map((r) => {
          const selected = !allSelected && (selectedRole as Role).id === r.id;
          return (
            <Menu.Item
              key={r.id}
              aria-label={r.id}
              aria-current={selected ? "true" : undefined}
              leftSection={selected ? <Check size={14} aria-hidden /> : undefined}
              onClick={() => handleSelect(r)}
            >
              {r.id}
            </Menu.Item>
          );
        })}
      </Menu.Dropdown>
    </Menu>
  );
}
