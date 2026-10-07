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
import { ActionIcon, Group, Pagination, Stack, Table, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Trash2 } from "lucide-react";
import { fetchOrgRoles, deleteOrgRole } from "../../api/admin";
import type { Role } from "../../types/auth";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../list/ListTable";
import { useListSortGroup, pageItems, type ListColumn } from "../list/useListSortGroup";

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
  // REQ-1940: sort and group are the shared list mechanism.
  const roleColumns: ListColumn<Role>[] = [
    { key: "id", label: t("rolesTab.colId"), sortValue: (r) => r.id },
    {
      key: "capabilities",
      label: t("rolesTab.colCapabilities"),
      sortValue: (r) => r.capabilities.join(", "),
      groupValue: (r) => r.capabilities.join(", "),
    },
    {
      key: "domains",
      label: t("rolesTab.colDomainAccess"),
      sortValue: (r) => r.domain_access.join(", "),
      groupValue: (r) => r.domain_access.join(", ") || t("rolesTab.noDomains"),
    },
  ];
  const sortGroup = useListSortGroup(orgRoles, roleColumns, "org-roles");

  return (
    <Stack gap="md">
      <Title order={4}>{t("rolesTab.heading", { orgId })}</Title>
      <ListTable minWidth={640} testId="org-roles-list">
          <ListHead
            sortGroup={sortGroup}
            columns={[
              { col: "id" },
              { col: "capabilities" },
              { col: "domains" },
              t("rolesTab.colActions"),
            ]}
          />
          <Table.Tbody>
            {orgRoles.length === 0 && <ListEmpty colSpan={4}>{t("rolesTab.empty")}</ListEmpty>}
            <ListItems
              state={sortGroup}
              items={pageItems(sortGroup, rolePage - 1, PAGE_SIZE)}
              colSpan={4}
              rowKey={(role) => role.id}
              render={(role) => (
              <ListRow>
                <Table.Td>
                  {role.id}
                </Table.Td>
                {role.detailsHidden ? (
                  <Table.Td colSpan={3} c="dimmed" data-testid={`role-details-hidden-${role.id}`}>
                    {t("rolesTab.detailsHidden")}
                  </Table.Td>
                ) : (
                  <>
                    <Table.Td>{role.capabilities.join(", ")}</Table.Td>
                    <Table.Td data-testid={`role-domains-${role.id}`}>
                      {/* A role reaches the domains it lists; an empty list is none, not all. */}
                      {role.domain_access.join(", ") || t("rolesTab.noDomains")}
                    </Table.Td>
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
                  </>
                )}
              </ListRow>
              )}
            />
          </Table.Tbody>
      </ListTable>
      {totalPages > 1 && sortGroup.groupBy.length === 0 && (
        <Group justify="flex-end">
          <Pagination total={totalPages} value={rolePage} onChange={setRolePage} size="sm" />
        </Group>
      )}
    </Stack>
  );
}
