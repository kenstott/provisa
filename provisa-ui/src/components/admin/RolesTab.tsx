// Copyright (c) 2026 Kenneth Stott
// Canary: 5f6d7095-5246-445f-bb37-5106cc619ea2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Group, Pagination, Stack, Table, Text, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Trash2 } from "lucide-react";
import { fetchOrgRoles, deleteOrgRole } from "../../api/admin";
import type { Role } from "../../types/auth";

const PAGE_SIZE = 50;

interface RolesTabProps {
  orgId: string;
}

export function RolesTab({ orgId }: RolesTabProps) {
  const { t } = useTranslation();
  const [orgRoles, setOrgRoles] = useState<Role[]>([]);
  const [rolePage, setRolePage] = useState(1);

  useEffect(() => {
    fetchOrgRoles(orgId)
      .then(setOrgRoles)
      .catch(() => setOrgRoles([]));
  }, [orgId]);

  const handleDeleteOrgRole = async (roleId: string) => {
    await deleteOrgRole(orgId, roleId);
    setOrgRoles((prev) => prev.filter((r) => r.id !== roleId));
    notifications.show({ message: t("rolesTab.deleted", { roleId }) });
  };

  const totalPages = Math.max(1, Math.ceil(orgRoles.length / PAGE_SIZE));
  const paged = orgRoles.slice((rolePage - 1) * PAGE_SIZE, rolePage * PAGE_SIZE);

  return (
    <Stack gap="md">
      <Title order={4}>{t("rolesTab.heading", { orgId })}</Title>
      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("rolesTab.colId")}</Table.Th>
              <Table.Th>{t("rolesTab.colCapabilities")}</Table.Th>
              <Table.Th>{t("rolesTab.colDomainAccess")}</Table.Th>
              <Table.Th>
                <Text span visibleFrom="xs" fz="sm" fw={600}>
                  {t("rolesTab.colActions")}
                </Text>
              </Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {orgRoles.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={4} ta="center" c="dimmed">
                  {t("rolesTab.empty")}
                </Table.Td>
              </Table.Tr>
            )}
            {paged.map((role) => (
              <Table.Tr key={role.id}>
                <Table.Td>{role.id}</Table.Td>
                <Table.Td>{role.capabilities.join(", ")}</Table.Td>
                <Table.Td>{role.domain_access.join(", ")}</Table.Td>
                <Table.Td>
                  <ActionIcon
                    variant="subtle"
                    color="red"
                    aria-label={t("rolesTab.deleteRole", { roleId: role.id })}
                    data-testid={`delete-role-${role.id}`}
                    onClick={() => handleDeleteOrgRole(role.id)}
                  >
                    <Trash2 size={14} />
                  </ActionIcon>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {totalPages > 1 && (
        <Group justify="flex-end">
          <Pagination total={totalPages} value={rolePage} onChange={setRolePage} size="sm" />
        </Group>
      )}
    </Stack>
  );
}
