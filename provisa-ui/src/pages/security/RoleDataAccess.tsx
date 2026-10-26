// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161: the Data access tab of a role's panel -- every table the role reaches once scope is
// applied (its domains, and the public columns of others), each expanding to its columns with
// scope, read or write, mask, row rule and the granting role. Only what the viewer may read is
// listed, and the page's domain filter narrows the list as on every list page.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Group, Loader, Table, Text } from "@mantine/core";
import { CollapsibleItem, Collapsibles } from "../../components/collapsible/Collapsible";
import { useDomainFilter } from "../../context/DomainFilterContext";
import { fetchRoleEffective, type RoleTableAccess } from "../../api/roleRights";
import { AccessCells, AccessHeads } from "./AccessCells";

export function RoleDataAccess({ roleId }: { roleId: string }) {
  const { t } = useTranslation();
  const { checkedDomains } = useDomainFilter();
  const [read, setRead] = useState<{ data: RoleTableAccess[] | null; error: string | null }>({
    data: null,
    error: null,
  });
  useEffect(() => {
    let live = true;
    fetchRoleEffective(roleId).then(
      (view) => live && setRead({ data: view.data, error: null }),
      (e: unknown) =>
        live && setRead({ data: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, [roleId]);
  if (read.error) {
    return (
      <Alert color="red" data-testid="role-data-error">
        {read.error}
      </Alert>
    );
  }
  if (!read.data) return <Loader size="sm" data-testid="role-data-loading" />;
  const tables = read.data.filter(
    (tb) => checkedDomains.size === 0 || checkedDomains.has(tb.domain),
  );
  if (tables.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="role-data-empty">
        {t("roleRights.dataEmpty")}
      </Text>
    );
  }
  return (
    <div data-testid={`role-data-${roleId}`}>
      <Text size="sm" c="dimmed" mb="xs">
        {t("roleRights.dataHint")}
      </Text>
      <Collapsibles id="security.role-data">
        {tables.map((tb) => (
          <CollapsibleItem
            key={tb.table_id}
            value={String(tb.table_id)}
            testId={`role-data-table-${tb.table_id}`}
            controlTestId={`role-data-table-${tb.table_id}-toggle`}
            title={
              <Group gap={6} wrap="nowrap">
                <Text size="sm" ff="monospace" fw={500}>
                  {tb.name}
                </Text>
                <Badge size="xs" variant="light" color="gray">
                  {tb.columns.length}
                </Badge>
                {tb.reach === "across" && (
                  <Badge size="xs" variant="light" color="grape">
                    {t("roleRights.across")}
                  </Badge>
                )}
              </Group>
            }
          >
            <Table data-testid={`role-data-columns-${tb.table_id}`}>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("roleRights.column")}</Table.Th>
                  <Table.Th>{t("roleRights.scope")}</Table.Th>
                  <AccessHeads />
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {tb.columns.map((c) => (
                  <Table.Tr key={c.name} data-testid={`role-data-column-${tb.table_id}-${c.name}`}>
                    <Table.Td ff="monospace">{c.name}</Table.Td>
                    <Table.Td>{t(`roleRights.scope_${c.scope}`)}</Table.Td>
                    <AccessCells access={c} />
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </CollapsibleItem>
        ))}
      </Collapsibles>
    </div>
  );
}
