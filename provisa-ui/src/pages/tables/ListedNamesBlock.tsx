// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e2d14-3c67-4f91-a0d8-7e1f4b9c2a36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2153: the names other domains list -- each table's name and its listed columns' names,
// nothing more. REQ-2180 (amended 2026-10-28, SUGGEST A PRODUCT FROM A LISTED TABLE): a listed
// table is not requested directly; its reader suggests a product of it.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Stack, Text } from "@mantine/core";
import { relationName } from "../../naming";
import type { ListedNames } from "../../types/admin";
import { SuggestProductModal, type SuggestTarget } from "./SuggestProductModal";

export function ListedNamesBlock({ listed }: { listed: ListedNames[] }) {
  const { t } = useTranslation();
  const [target, setTarget] = useState<SuggestTarget | null>(null);
  if (listed.length === 0) return null;
  const suggest = (n: ListedNames) =>
    setTarget({
      domainId: n.domainId,
      tableName: n.tableName,
      domainTables: listed.filter((m) => m.domainId === n.domainId).map((m) => m.tableName),
    });
  return (
    <Stack gap="0.25rem" py="sm" data-testid="tables-listed-names">
      <Text size="sm" fw={600}>
        {t("tablesPage.listedElsewhere")}
      </Text>
      <Text size="xs" c="dimmed">
        {t("tablesPage.listedElsewhereHint")}
      </Text>
      {listed.map((n) => (
        <Stack key={relationName(n)} gap={0} align="flex-start">
          <Text size="sm" style={{ fontFamily: "monospace" }}>
            {relationName(n)}: {n.columnNames.join(", ")}
          </Text>
          <Button
            size="compact-xs"
            variant="subtle"
            onClick={() => suggest(n)}
            data-testid={`suggest-product-${relationName(n)}`}
          >
            {t("tablesPage.suggestProduct")}
          </Button>
        </Stack>
      ))}
      <SuggestProductModal target={target} onClose={() => setTarget(null)} />
    </Stack>
  );
}
