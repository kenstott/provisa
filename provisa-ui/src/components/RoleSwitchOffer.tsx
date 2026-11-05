// Copyright (c) 2026 Kenneth Stott
// Canary: 3ee11f22-5e81-4f5b-a3a5-dc978afcca2c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2060: rights follow the roles a request acts as, so a page the selection does not grant may
// still be granted by another role the person holds. Under the refusal, each such role is offered:
// one click makes it the acting role and the page opens. Held derived roles carry no rights and are
// never offered.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Button, Group, Stack, Text } from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { meetsRequirement, type CapabilityRequirement } from "../lib/capabilities";

export function RoleSwitchOffer({
  requirement,
  children,
}: {
  requirement: CapabilityRequirement;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const { availableRoles, selectRole } = useAuth();
  const granting = availableRoles.filter((r) => meetsRequirement(r.effectiveCapabilities, requirement));
  return (
    <Stack gap={0} data-testid="role-switch-offer">
      {children}
      {granting.length === 0 ? (
        <Text size="sm" c="dimmed" className="page" pt={0} data-testid="role-switch-none">
          {t("roleSwitch.noneHeld")}
        </Text>
      ) : (
        <Stack gap="xs" className="page" pt={0}>
          <Text size="sm">{t("roleSwitch.notGranted")}</Text>
          <Group gap="xs">
            {granting.map((r) => (
              <Button
                key={r.id}
                size="xs"
                variant="light"
                onClick={() => selectRole(r)}
                data-testid={`role-switch-${r.id}`}
              >
                {t("roleSwitch.switchTo", { role: r.id })}
              </Button>
            ))}
          </Group>
        </Stack>
      )}
    </Stack>
  );
}
