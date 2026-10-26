// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161: the Capabilities tab of a role's panel -- its lineage from the standard roles, drawn
// with the lineage diagram, and every capability it holds with the role it enters at.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Loader, Table, Text } from "@mantine/core";
import { LineageDag } from "../../components/lineage/LineageDag";
import { fetchRoleEffective, type RoleEffective } from "../../api/roleRights";
import { roleLineageGraph } from "./roleLineage";

export function RoleCapabilities({ roleId }: { roleId: string }) {
  const { t } = useTranslation();
  const [read, setRead] = useState<{ view: RoleEffective | null; error: string | null }>({
    view: null,
    error: null,
  });
  useEffect(() => {
    let live = true;
    fetchRoleEffective(roleId).then(
      (view) => live && setRead({ view, error: null }),
      (e: unknown) =>
        live && setRead({ view: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, [roleId]);
  const view = read.view;
  const graph = useMemo(
    () => (view ? roleLineageGraph(view, (k) => t(`roleRights.kind_${k}`)) : null),
    [view, t],
  );
  if (read.error) {
    return (
      <Alert color="red" data-testid="role-capabilities-error">
        {read.error}
      </Alert>
    );
  }
  if (!view || !graph) return <Loader size="sm" data-testid="role-capabilities-loading" />;
  const standards = new Set(view.standards);
  return (
    <div data-testid={`role-capabilities-${roleId}`}>
      <Text size="sm" c="dimmed" mb="xs">
        {t("roleRights.lineageHint")}
      </Text>
      <LineageDag graph={graph} height={360} allColumns />
      <Table mt="md" data-testid="role-capabilities-list">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("roleRights.capability")}</Table.Th>
            <Table.Th>{t("roleRights.from")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {view.capabilities.map((c) => (
            <Table.Tr key={c.right} data-testid={`role-capability-${c.right}`}>
              <Table.Td ff="monospace">{c.right}</Table.Td>
              <Table.Td>
                <Text component="span" ff="monospace" size="sm">
                  {c.from === view.role ? t("roleRights.own") : c.from}
                </Text>
                {standards.has(c.from) && (
                  <Badge size="xs" variant="light" ml={6}>
                    {t("roleRights.kind_standard")}
                  </Badge>
                )}
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </div>
  );
}
