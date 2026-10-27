// Copyright (c) 2026 Kenneth Stott
// Canary: c56594c7-7099-489d-81c4-b61a5576d24f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2162: who can read or write each column of a table -- for a chosen column, every role that
// reads it, writes it, sees it masked or has a row rule on it, with the grant and inheritance
// behind each. The inverse of a role's Data access tab (REQ-2161), resolved by the same rule. Only
// columns the viewer may read can be chosen.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Loader, Select, Table, Text } from "@mantine/core";
import { FormField, FormLayout } from "../../components/form/FormLayout";
import { fetchTableAccess, type TableAccess } from "../../api/roleRights";
import { AccessCells, AccessHeads } from "../security/AccessCells";

export function TableAccessPanel({ tableId }: { tableId: number | null }) {
  const { t } = useTranslation();
  const [read, setRead] = useState<{ access: TableAccess | null; error: string | null }>({
    access: null,
    error: null,
  });
  const [column, setColumn] = useState<string | null>(null);
  useEffect(() => {
    if (tableId === null) return;
    let live = true;
    fetchTableAccess(tableId).then(
      (access) => live && setRead({ access, error: null }),
      (e: unknown) =>
        live && setRead({ access: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, [tableId]);
  if (tableId === null) {
    return (
      <Text size="sm" c="dimmed" data-testid="table-access-unsaved">
        {t("roleRights.accessUnsaved")}
      </Text>
    );
  }
  if (read.error) {
    return (
      <Alert color="red" data-testid="table-access-error">
        {read.error}
      </Alert>
    );
  }
  if (!read.access) return <Loader size="sm" data-testid="table-access-loading" />;
  const columns = read.access.columns;
  const chosen = columns.find((c) => c.name === (column ?? columns[0]?.name));
  return (
    <div data-testid="table-access">
      <FormLayout>
        <FormField size="medium" hint={t("roleRights.accessHint")}>
          <Select
            label={t("roleRights.column")}
            data={columns.map((c) => c.name)}
            value={chosen?.name ?? null}
            onChange={setColumn}
            allowDeselect={false}
            searchable
            data-testid="table-access-column"
          />
        </FormField>
      </FormLayout>
      {chosen &&
        (chosen.access.length === 0 ? (
          <Text size="sm" c="dimmed" data-testid="table-access-none">
            {t("roleRights.accessNone")}
          </Text>
        ) : (
          <Table data-testid="table-access-roles">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("roleRights.role")}</Table.Th>
                <AccessHeads />
                <Table.Th>{t("roleRights.reach")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {chosen.access.map((a) => (
                <Table.Tr key={a.role} data-testid={`table-access-role-${a.role}`}>
                  <Table.Td>
                    <Text component="span" size="sm" ff="monospace">
                      {a.role}
                    </Text>
                    {a.standard && (
                      <Badge size="xs" variant="light" ml={6}>
                        {t("roleRights.kind_standard")}
                      </Badge>
                    )}
                  </Table.Td>
                  <AccessCells access={a} />
                  <Table.Td>{t(`roleRights.reach_${a.reach}`)}</Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        ))}
    </div>
  );
}
