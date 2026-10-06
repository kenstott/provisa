// Copyright (c) 2026 Kenneth Stott
// Canary: 8cd04276-6f89-424b-adcb-313c16ec3bbc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { SegmentedControl, Stack } from "@mantine/core";
import { useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import type { RegisteredTable } from "../../../types/admin";
import { TestDataColumns } from "./TestDataColumns";

type Update = (i: number, key: string, value: string | string[] | boolean) => void;

// REQ-1494: the table editor's column list in one of two modes -- its metadata, or its test data
// (each column's fake, synthetic rule and stable flag).
export function ColumnModes({
  table,
  metadata,
  updateEditCol,
}: {
  table: RegisteredTable;
  metadata: ReactNode;
  updateEditCol: Update;
}) {
  const { t } = useTranslation();
  const [mode, setMode] = useState<"metadata" | "testdata">("metadata");
  return (
    <Stack gap="xs">
      <SegmentedControl
        size="xs"
        style={{ alignSelf: "flex-start" }}
        value={mode}
        onChange={(v) => setMode(v as "metadata" | "testdata")}
        data={[
          { value: "metadata", label: t("testData.modeMetadata") },
          { value: "testdata", label: t("testData.modeTestData") },
        ]}
        data-tour="table-columns-mode"
        data-testid="table-columns-mode"
      />
      {mode === "metadata" ? (
        metadata
      ) : (
        <TestDataColumns table={table} updateEditCol={updateEditCol} />
      )}
    </Stack>
  );
}
