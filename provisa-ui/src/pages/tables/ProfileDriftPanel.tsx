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
import { Alert, Badge, Box, Loader, Text } from "@mantine/core";
import { ProfileResultTable } from "./ProfileResultTable";
import { fetchMeasureHistory, type MeasureHistory, type MeasureKey } from "../../api/profiler";

type Row = Record<string, unknown>;

function num(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "number")
    return Number.isInteger(value) ? String(value) : value.toPrecision(4);
  return String(value);
}

function label(row: Row): string {
  const k = keyOf(row);
  return [k.scope, k.column, k.subject, k.measure].filter((p) => p != null).join(" · ");
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
    <Box mt="xs">
      <ProfileResultTable
        rows={history.points}
        height={240}
        title={t("profileDriftPanel.value")}
        testId="profile-drift-history"
        rowKey={(p) => p.runId}
        rowTestId={(p) => `profile-drift-point-${p.runId}`}
        rowProps={(p) => ({ fw: p.current ? 700 : undefined })}
        columns={[
          {
            key: "run_time",
            label: t("profileDriftPanel.runTime"),
            value: (p) => p.runTime,
            cell: (p) => (
              <>
                {new Date(p.runTime).toLocaleString()}
                {p.current && history.drift.drifting === true && (
                  <Badge ml="xs" size="xs" color="red">
                    {t("profileDriftPanel.drifting")}
                  </Badge>
                )}
              </>
            ),
          },
          {
            key: "value",
            label: t("profileDriftPanel.value"),
            value: (p) => p.value,
            cell: (p) => num(p.value),
          },
        ]}
      />
    </Box>
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
      <ProfileResultTable
        rows={rows}
        height={300}
        title={t("profileRunsModal.kind_drift")}
        testId="profile-rows-drift"
        rowKey={(_r, i) => i}
        rowTestId={(_r, i) => `profile-drift-row-${i}`}
        onRowClick={(r) => {
          setHistory(null);
          setError(null);
          setPicked(keyOf(r));
        }}
        columns={[
          {
            key: "measure",
            label: t("profileDriftPanel.measure"),
            value: (r) => label(r),
            cell: (r) => (
              <>
                {label(r)}
                {r.detail != null && ` (${String(r.detail)})`}
              </>
            ),
          },
          ...SHOWN.map((c) => ({
            key: c,
            label: t(`profileDriftPanel.col_${c}`),
            value: (r: Row) => r[c],
            cell: (r: Row) => num(r[c]),
          })),
          {
            key: "drift",
            label: t("profileDriftPanel.drift"),
            value: (r) => (r.drifting === true ? String(r.drift_reason) : ""),
            cell: (r) =>
              r.drifting === true && (
                <Badge size="xs" color="red" data-testid={`profile-drift-flag-${rows.indexOf(r)}`}>
                  {String(r.drift_reason)}
                </Badge>
              ),
          },
        ]}
      />
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
