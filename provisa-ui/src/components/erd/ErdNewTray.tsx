// Copyright (c) 2026 Kenneth Stott
// Canary: 41f8c2a6-d07b-4e39-8c55-a9e3b16d0f72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2189: the tables new since the layout was saved, in a domain it shows. They are not placed
// on their own: each waits here until it is placed, which edits the layout.

import { useTranslation } from "react-i18next";
import { Button, Group, Stack, Text } from "@mantine/core";
import { catalogTableName } from "../../naming";
import type { ListedTable } from "../../types/admin";
import { Tip } from "../Tip";

export function ErdNewTray({
  tables,
  onPlace,
}: {
  tables: ListedTable[];
  onPlace: (tableId: number) => void;
}) {
  const { t } = useTranslation();
  return (
    <Stack gap={4} className="erd-stage__tray" data-testid="erd-new-tray">
      <Text size="xs" fw={600}>
        {t("erdLayouts.newSinceSaved", { count: tables.length })}
      </Text>
      {tables.map((table) => (
        <Group key={table.id} gap="xs" wrap="nowrap" justify="space-between">
          <Text size="xs" truncate>
            {catalogTableName(table)}
          </Text>
          <Tip text={t("erdLayouts.placeHint")}>
            <Button
              size="compact-xs"
              variant="default"
              onClick={() => onPlace(table.id)}
              data-testid={`erd-new-place-${table.id}`}
            >
              {t("erdLayouts.place")}
            </Button>
          </Tip>
        </Group>
      ))}
    </Stack>
  );
}
