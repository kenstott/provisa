// Copyright (c) 2026 Kenneth Stott
// Canary: e7f2a1b3-c4d5-6789-abcd-ef0123456789
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Badge, Group, Modal, Stack, Table, Text, Title } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";

interface Props {
  onClose: () => void;
}

export function UserProfileModal({ onClose }: Props) {
  const { t } = useTranslation();
  const { displayName, email, userId, devMode, availableRoles, assignments, capabilities, orgMemberships, activeOrgId } = useAuth();

  return (
    <Modal opened onClose={onClose} title={t("userProfileModal.title")} size={560} centered data-testid="user-profile-modal">
      <Stack gap="lg">
        <section>
          <Title order={4} tt="uppercase" fz="0.75rem" c="dimmed" fw={600} mb="xs" style={{ letterSpacing: "0.05em" }}>
            {t("userProfileModal.identity")}
          </Title>
          <Table withRowBorders={false} verticalSpacing={4} fz="0.85rem">
            <Table.Tbody>
              {displayName && (
                <Table.Tr>
                  <Table.Td c="dimmed" style={{ width: "max-content" }}>{t("userProfileModal.name")}</Table.Td>
                  <Table.Td>{displayName}</Table.Td>
                </Table.Tr>
              )}
              {email && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.email")}</Table.Td>
                  <Table.Td>{email}</Table.Td>
                </Table.Tr>
              )}
              {userId && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.userId")}</Table.Td>
                  <Table.Td ff="monospace">{userId}</Table.Td>
                </Table.Tr>
              )}
              {activeOrgId && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.org")}</Table.Td>
                  <Table.Td>{orgMemberships.find((m) => m.org_id === activeOrgId)?.org_name ?? activeOrgId}</Table.Td>
                </Table.Tr>
              )}
              {devMode && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.mode")}</Table.Td>
                  <Table.Td>
                    <Badge size="xs" color="gray" variant="filled">
                      {t("userProfileModal.devBadge")}
                    </Badge>
                  </Table.Td>
                </Table.Tr>
              )}
            </Table.Tbody>
          </Table>
        </section>

        <section>
          <Title order={4} tt="uppercase" fz="0.75rem" c="dimmed" fw={600} mb="xs" style={{ letterSpacing: "0.05em" }}>
            {t("userProfileModal.rolesAndDomainAccess")}
          </Title>
          {availableRoles.length === 0 ? (
            <Text fz="0.85rem" c="dimmed">{t("userProfileModal.noRolesAssigned")}</Text>
          ) : (
            <Table fz="0.82rem" withTableBorder={false}>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th c="dimmed" fw={500}>{t("userProfileModal.role")}</Table.Th>
                  <Table.Th c="dimmed" fw={500}>{t("userProfileModal.domains")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {availableRoles.map((role) => {
                  const domains = assignments
                    .filter((a) => a.role_id === role.id)
                    .map((a) => a.domain_id);
                  return (
                    <Table.Tr key={role.id}>
                      <Table.Td ff="monospace">{role.id}</Table.Td>
                      <Table.Td>
                        {domains.length === 0 ? (
                          <Text component="span" c="dimmed">—</Text>
                        ) : domains.includes("*") ? (
                          <Text component="span" c="var(--approve)">{t("userProfileModal.allDomains")}</Text>
                        ) : (
                          domains.join(", ")
                        )}
                      </Table.Td>
                    </Table.Tr>
                  );
                })}
              </Table.Tbody>
            </Table>
          )}
        </section>

        <section>
          <Title order={4} tt="uppercase" fz="0.75rem" c="dimmed" fw={600} mb="xs" style={{ letterSpacing: "0.05em" }}>
            {t("userProfileModal.capabilities")}
          </Title>
          {capabilities.length === 0 ? (
            <Text fz="0.85rem" c="dimmed">{t("userProfileModal.noCapabilities")}</Text>
          ) : (
            <Group gap="xs">
              {capabilities.map((cap) => (
                <Badge key={cap} size="sm" variant="outline" color="gray" ff="monospace" tt="none" fw={400}>
                  {cap}
                </Badge>
              ))}
            </Group>
          )}
        </section>
      </Stack>
    </Modal>
  );
}
