// Copyright (c) 2026 Kenneth Stott
// Canary: 533751d3-423b-424c-a381-af763cdd03b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: a table or view that keeps its versions says so in its detail, with the way to read
// them: the SQL explorer, opened on the table, whose as-of and as-recorded pickers (REQ-1163) take
// the time to read it at.

import { Alert, Anchor, Text } from "@mantine/core";
import { History as HistoryIcon } from "lucide-react";
import { useTranslation } from "react-i18next";
import type { NavigateFunction } from "react-router-dom";
import type { RegisteredTable } from "../../types/admin";
import { tableSqlRef } from "../../naming";

export function KeepsHistoryNote({
  table,
  navigate,
}: {
  table: RegisteredTable;
  navigate: NavigateFunction;
}) {
  const { t } = useTranslation();
  if (table.businessKey.length === 0) return null;
  return (
    <Alert
      variant="light"
      color="orange"
      icon={<HistoryIcon size={16} aria-hidden />}
      mb="sm"
      data-testid="table-keeps-history"
    >
      <Text size="sm">
        {t("tableReadView.keepsHistory")}{" "}
        <Anchor
          component="button"
          type="button"
          size="sm"
          data-testid="table-keeps-history-query"
          onClick={() =>
            navigate("/sql", { state: { sql: `select * from ${tableSqlRef(table)}` } })
          }
        >
          {t("tableReadView.keepsHistoryQuery")}
        </Anchor>
      </Text>
    </Alert>
  );
}
