// Copyright (c) 2026 Kenneth Stott
// Canary: 5a391a64-88d0-4f62-94ea-0e81a47578a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161, REQ-2162: what a role does with a column, the same cells in the role's Data access tab
// and in a table's who-can-read list -- read or write, mask, row rule, and the grant behind it.

import { useTranslation } from "react-i18next";
import { Badge, Group, Table, Text } from "@mantine/core";
import type { ColumnAccess } from "../../api/roleRights";

export function AccessCells({ access }: { access: ColumnAccess }) {
  const { t } = useTranslation();
  return (
    <>
      <Table.Td>
        <Group gap={4}>
          {access.read && (
            <Badge size="xs" variant="light" color="blue">
              {t("roleRights.read")}
            </Badge>
          )}
          {access.write.map((op) => (
            <Badge key={op} size="xs" variant="light" color="orange">
              {t(`roleRights.write_${op}`)}
            </Badge>
          ))}
        </Group>
      </Table.Td>
      <Table.Td>
        {access.mask ? (
          <Text size="xs" ff="monospace">
            {access.mask.type}
          </Text>
        ) : (
          "—"
        )}
      </Table.Td>
      <Table.Td>
        {access.row_rule ? (
          <Text size="xs" ff="monospace">
            {access.row_rule}
          </Text>
        ) : (
          "—"
        )}
      </Table.Td>
      <Table.Td>
        {access.granted_by === null ? (
          "—"
        ) : (
          <Text size="xs" ff="monospace">
            {access.granted_by}
            {access.via.length > 2 && (
              <Text component="span" c="dimmed" size="xs" ff="text">
                {" "}
                ({t("roleRights.via", { path: access.via.slice(1, -1).join(" → ") })})
              </Text>
            )}
          </Text>
        )}
      </Table.Td>
    </>
  );
}

/** The header cells over AccessCells. */
export function AccessHeads() {
  const { t } = useTranslation();
  return (
    <>
      <Table.Th>{t("roleRights.access")}</Table.Th>
      <Table.Th>{t("roleRights.mask")}</Table.Th>
      <Table.Th>{t("roleRights.rowRule")}</Table.Th>
      <Table.Th>{t("roleRights.grantedBy")}</Table.Th>
    </>
  );
}
