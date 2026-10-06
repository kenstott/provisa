// Copyright (c) 2026 Kenneth Stott
// Canary: 9b4f0d36-81e2-4c75-a3b9-2e6d8f1c0a57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Box, Table, Text } from "@mantine/core";
import { runSql } from "../../api/admin";
import { tableRef } from "../../components/nativeParams";
import { useAuth } from "../../context/AuthContext";
import type { RegisteredTable } from "../../types/admin";

const RUN_KEY = new Set(["run_id", "run_time"]);

function cell(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "number")
    return Number.isInteger(value) ? String(value) : value.toPrecision(6);
  return String(value);
}

// REQ-1934: the latest run of a table's profile, read from its registered profile columns table as
// the viewer, through the governed pipeline — so the viewer sees exactly what that table's grants
// and masks give them, and nothing when it was never registered (the caller renders no panel).
export function LatestProfilePanel({ profileTable }: { profileTable: RegisteredTable }) {
  const { t } = useTranslation();
  const { role } = useAuth();
  const [result, setResult] = useState<{
    columns: string[];
    rows: Record<string, unknown>[];
    error?: string;
  } | null>(null);

  useEffect(() => {
    if (!role) return;
    const ref = tableRef(profileTable);
    runSql(
      `SELECT * FROM ${ref} WHERE "run_time" = (SELECT MAX("run_time") FROM ${ref})`,
      role.id,
    ).then(setResult);
  }, [profileTable, role]);

  if (result == null) return null;
  return (
    <Box
      style={{ borderTop: "1px solid var(--border)" }}
      px="0.75rem"
      py="0.5rem"
      data-testid="latest-profile-panel"
    >
      <Text fz="0.75rem" c="dimmed" mb="0.4rem">
        {result.rows.length > 0
          ? t("latestProfilePanel.heading", { time: cell(result.rows[0].run_time) })
          : t("latestProfilePanel.none")}
      </Text>
      {result.error && (
        <Text fz="0.8rem" c="var(--destructive)" data-testid="latest-profile-error">
          {result.error}
        </Text>
      )}
      {result.rows.length > 0 && (
        <Table.ScrollContainer minWidth={640}>
          <Table className="data-table" style={{ fontSize: "0.72rem" }}>
            <Table.Thead>
              <Table.Tr>
                {result.columns
                  .filter((c) => !RUN_KEY.has(c))
                  .map((c) => (
                    <Table.Th key={c}>{c}</Table.Th>
                  ))}
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {result.rows.map((r, i) => (
                <Table.Tr key={i}>
                  {result.columns
                    .filter((c) => !RUN_KEY.has(c))
                    .map((c) => (
                      <Table.Td key={c}>{cell(r[c])}</Table.Td>
                    ))}
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </Box>
  );
}
