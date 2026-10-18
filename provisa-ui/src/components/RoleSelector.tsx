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
  const {
    selectedRole,
    availableRoles,
    selectRole,
    devMode,
    loading,
    derivedRoles,
    selectedDerivedRoles,
    toggleDerivedRole,
  } = useAuth();

  // REQ-1430: an in-flight bootstrap has no roles yet. "No roles configured" states a settled fact
  // about the account, so it must wait until the roles have actually been fetched.
  if (loading)
    return <span data-testid="role-selector-checking">{t("roleSelector.checking")}</span>;
  if (availableRoles.length === 0) return <span>{t("roleSelector.none")}</span>;

  const allSelected = selectedRole === "all";
  const roleLabel = allSelected ? t("roleSelector.all") : (selectedRole as Role).id;
  // REQ-2057: the derived roles selected beside the roles are part of what every request names.
  const label = [roleLabel, ...selectedDerivedRoles].join(" + ");

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
          // A demo's tour picks a role here by its id (demoTour.ts); the "All" set is "all".
          data-role-option="all"
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
              data-role-option={r.id}
            >
              {r.id}
            </Menu.Item>
          );
        })}
        {derivedRoles.length > 0 && (
          <>
            <Menu.Divider />
            <Menu.Label>{t("roleSelector.derivedRoles")}</Menu.Label>
            {derivedRoles.map((p) => {
              const on = selectedDerivedRoles.includes(p.id);
              return (
                <Menu.Item
                  key={p.id}
                  aria-label={p.id}
                  aria-checked={on}
                  role="menuitemcheckbox"
                  closeMenuOnClick={false}
                  leftSection={on ? <Check size={14} aria-hidden /> : undefined}
                  onClick={() => toggleDerivedRole(p.id)}
                  data-testid={`role-selector-claim-${p.id}`}
                >
                  {p.id}
                </Menu.Item>
              );
            })}
          </>
        )}
      </Menu.Dropdown>
    </Menu>
  );
}
