// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2f9d14-c3a8-4e57-90b1-d4e7a2c8f531
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Loader, ScrollArea, Table, Text } from "@mantine/core";
import { fetchMeasureHistory, type MeasureHistory, type MeasureKey } from "../../api/profiler";

type Row = Record<string, unknown>;

function num(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "number")
    return Number.isInteger(value) ? String(value) : value.toPrecision(4);
  return String(value);
}

function keyOf(row: Row): MeasureKey {
  return {
    scope: String(row.scope),
    measure: String(row.measure),
    column: (row.column_name as string | null) ?? null,
    subject: (row.subject as string | null) ?? null,
  };
}

// The drift columns shown, in order; the rest of a drift row (its run key, involved columns) is
// what identifies it.
const SHOWN = [
  "previous",
  "current",
  "change",
  "ks_previous",
  "psi_previous",
  "window_runs",
  "baseline",
  "spread",
  "distance",
  "slope_spread",
  "ks",
  "psi",
] as const;

function History({ history }: { history: MeasureHistory }) {
  const { t } = useTranslation();
  return (
    <Table withTableBorder fz="xs" mt="xs" data-testid="profile-drift-history">
      <Table.Thead>
        <Table.Tr>
          <Table.Th>{t("profileDriftPanel.runTime")}</Table.Th>
          <Table.Th>{t("profileDriftPanel.value")}</Table.Th>
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {history.points.map((p) => (
          <Table.Tr
            key={p.runId}
            fw={p.current ? 700 : undefined}
            data-testid={`profile-drift-point-${p.runId}`}
          >
            <Table.Td>
              {new Date(p.runTime).toLocaleString()}
              {p.current && history.drift.drifting === true && (
                <Badge ml="xs" size="xs" color="red">
                  {t("profileDriftPanel.drifting")}
                </Badge>
              )}
            </Table.Td>
            <Table.Td>{num(p.value)}</Table.Td>
          </Table.Tr>
        ))}
      </Table.Tbody>
    </Table>
  );
}

// REQ-1934: a run's comparison with the one before and its drift against the window of earlier
// runs, a drifting measure marked; picking a measure shows its history across the window.
export function ProfileDriftPanel({
  tableId,
  runId,
  role,
  rows,
}: {
  tableId: number;
  runId: string;
  role: string;
  rows: Row[];
}) {
  const { t } = useTranslation();
  const [picked, setPicked] = useState<MeasureKey | null>(null);
  const [history, setHistory] = useState<MeasureHistory | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (picked == null) return;
    fetchMeasureHistory(tableId, runId, role, picked)
      .then(setHistory)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [tableId, runId, role, picked]);

  if (rows.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="profile-rows-drift-empty">
        {t("profileRunsModal.noRows")}
      </Text>
    );
  }
  return (
    <>
      <ScrollArea h={300}>
        <Table striped highlightOnHover withTableBorder fz="xs" data-testid="profile-rows-drift">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("profileDriftPanel.measure")}</Table.Th>
              {SHOWN.map((k) => (
                <Table.Th key={k}>{t(`profileDriftPanel.col_${k}`)}</Table.Th>
              ))}
              <Table.Th>{t("profileDriftPanel.drift")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map((r, i) => {
              const k = keyOf(r);
              const label = [k.scope, k.column, k.subject, k.measure]
                .filter((p) => p != null)
                .join(" · ");
              return (
                <Table.Tr
                  key={i}
                  style={{ cursor: "pointer" }}
                  onClick={() => {
                    setHistory(null);
                    setError(null);
                    setPicked(k);
                  }}
                  data-testid={`profile-drift-row-${i}`}
                >
                  <Table.Td>
                    {label}
                    {r.detail != null && ` (${String(r.detail)})`}
                  </Table.Td>
                  {SHOWN.map((c) => (
                    <Table.Td key={c}>{num(r[c])}</Table.Td>
                  ))}
                  <Table.Td>
                    {r.drifting === true && (
                      <Badge size="xs" color="red" data-testid={`profile-drift-flag-${i}`}>
                        {String(r.drift_reason)}
                      </Badge>
                    )}
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      </ScrollArea>
      {error && (
        <Alert color="red" mt="xs" data-testid="profile-drift-error">
          {error}
        </Alert>
      )}
      {picked != null && history == null && !error && <Loader size="sm" mt="xs" />}
      {history != null && <History history={history} />}
    </>
  );
}
