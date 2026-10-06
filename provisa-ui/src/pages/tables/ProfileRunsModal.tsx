// Copyright (c) 2026 Kenneth Stott
// Canary: 0a9d3e51-7f26-4c8b-b1e4-d6c2a8f0e917
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Loader, Modal, ScrollArea, Table, Tabs, Text } from "@mantine/core";
import {
  fetchProfileRun,
  fetchProfileRuns,
  type ProfileRun,
  type ProfileRunResults,
} from "../../api/profiler";
import { useAuth } from "../../context/AuthContext";
import { ProfileDriftPanel } from "./ProfileDriftPanel";

// The result relations shown for a run, in this order; ``runs`` is the list itself.
const KINDS = [
  "drift",
  "columns",
  "plausible_type",
  "quantiles",
  "histogram",
  "top_values",
  "shapes",
  "fits",
  "fit_quality",
  "fanout_runs",
  "fanout",
  "duplicates",
  "repeats",
] as const;
const RUN_KEY = new Set(["run_id", "run_time"]);

function cell(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "number")
    return Number.isInteger(value) ? String(value) : value.toPrecision(6);
  return String(value);
}

function RowsTable({ rows, testId }: { rows: Record<string, unknown>[]; testId: string }) {
  const { t } = useTranslation();
  if (rows.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid={`${testId}-empty`}>
        {t("profileRunsModal.noRows")}
      </Text>
    );
  }
  const keys = Object.keys(rows[0]).filter((k) => !RUN_KEY.has(k));
  return (
    <ScrollArea h={360}>
      <Table striped withTableBorder fz="xs" data-testid={testId}>
        <Table.Thead>
          <Table.Tr>
            {keys.map((k) => (
              <Table.Th key={k}>{k}</Table.Th>
            ))}
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {rows.map((r, i) => (
            <Table.Tr key={i}>
              {keys.map((k) => (
                <Table.Td key={k}>{cell(r[k])}</Table.Td>
              ))}
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </ScrollArea>
  );
}

// REQ-1934: a member table's run history and one run's results, as the profiler wrote them. Opened
// only from the table editor, so only those who may edit the table see it.
export function ProfileRunsModal({
  tableId,
  tableName,
  onClose,
}: {
  tableId: number;
  tableName: string;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const { role } = useAuth();
  const [runs, setRuns] = useState<ProfileRun[] | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  // The results of one run, kept with the run they answer: a selection whose results have not come
  // back yet shows a loader rather than the previous run's.
  const [loaded, setLoaded] = useState<{ runId: string; data: ProfileRunResults } | null>(null);
  const results = loaded != null && loaded.runId === selected ? loaded.data : null;
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchProfileRuns(tableId)
      .then((r) => {
        setRuns(r);
        const latest = r.find((run) => run.status === "succeeded");
        if (latest) setSelected(latest.run_id);
      })
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [tableId]);

  useEffect(() => {
    if (selected == null || !role) return;
    fetchProfileRun(tableId, selected, role.id)
      .then((data) => setLoaded({ runId: selected, data }))
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [tableId, selected, role]);

  return (
    <Modal
      opened
      onClose={onClose}
      size="90%"
      title={t("profileRunsModal.title", { table: tableName })}
      data-testid="profile-runs-modal"
    >
      {error && (
        <Alert color="red" data-testid="profile-runs-error">
          {error}
        </Alert>
      )}
      {runs == null && !error && <Loader size="sm" />}
      {runs != null && runs.length === 0 && (
        <Text size="sm" c="dimmed" data-testid="profile-runs-none">
          {t("profileRunsModal.none")}
        </Text>
      )}
      {runs != null && runs.length > 0 && (
        <ScrollArea h={200} mb="md">
          <Table highlightOnHover withTableBorder fz="xs" data-testid="profile-runs-list">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("profileRunsModal.runTime")}</Table.Th>
                <Table.Th>{t("profileRunsModal.status")}</Table.Th>
                <Table.Th>{t("profileRunsModal.rowCount")}</Table.Th>
                <Table.Th>{t("profileRunsModal.profiledRows")}</Table.Th>
                <Table.Th>{t("profileRunsModal.method")}</Table.Th>
                <Table.Th>{t("profileRunsModal.duplicates")}</Table.Th>
                <Table.Th>{t("profileRunsModal.duration")}</Table.Th>
                <Table.Th>{t("profileRunsModal.error")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {runs.map((run) => (
                <Table.Tr
                  key={run.run_id}
                  style={{ cursor: run.status === "succeeded" ? "pointer" : undefined }}
                  bg={run.run_id === selected ? "var(--mantine-color-blue-light)" : undefined}
                  onClick={() => run.status === "succeeded" && setSelected(run.run_id)}
                  data-testid={`profile-run-${run.run_id}`}
                >
                  <Table.Td>{new Date(run.run_time).toLocaleString()}</Table.Td>
                  <Table.Td>
                    <Badge size="xs" color={run.status === "succeeded" ? "green" : "red"}>
                      {t(`profileRunsModal.status_${run.status}`)}
                    </Badge>
                  </Table.Td>
                  <Table.Td>{cell(run.row_count)}</Table.Td>
                  <Table.Td>
                    {cell(run.profiled_rows)}
                    {run.sampled ? ` (${t("profileRunsModal.sampled")})` : ""}
                  </Table.Td>
                  <Table.Td data-testid={`profile-run-method-${run.run_id}`}>
                    {run.sample_method == null
                      ? ""
                      : t(`profileRunsModal.method_${run.sample_method}`)}
                    {run.sampled && run.sample_fraction != null
                      ? ` · ${t("profileRunsModal.share", {
                          pct: (run.sample_fraction * 100).toFixed(1),
                        })}`
                      : ""}
                  </Table.Td>
                  <Table.Td data-testid={`profile-run-duplicates-${run.run_id}`}>
                    {run.duplicate_rows == null
                      ? ""
                      : t("profileRunsModal.duplicateRows", {
                          rows: run.duplicate_rows,
                          pct: ((run.duplicate_share ?? 0) * 100).toFixed(1),
                        })}
                  </Table.Td>
                  <Table.Td>{t("profileRunsModal.ms", { ms: run.duration_ms })}</Table.Td>
                  <Table.Td>{run.error ?? ""}</Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </ScrollArea>
      )}
      {selected != null && results == null && !error && <Loader size="sm" />}
      {results != null && (
        <Tabs defaultValue="columns" keepMounted={false}>
          <Tabs.List>
            {KINDS.map((k) => (
              <Tabs.Tab key={k} value={k} data-testid={`profile-tab-${k}`}>
                {t(`profileRunsModal.kind_${k}`)}
              </Tabs.Tab>
            ))}
          </Tabs.List>
          {KINDS.map((k) => (
            <Tabs.Panel key={k} value={k} pt="xs">
              {k === "drift" && selected != null && role ? (
                <ProfileDriftPanel
                  tableId={tableId}
                  runId={selected}
                  role={role.id}
                  rows={results.drift ?? []}
                />
              ) : (
                <RowsTable rows={results[k] ?? []} testId={`profile-rows-${k}`} />
              )}
            </Tabs.Panel>
          ))}
        </Tabs>
      )}
    </Modal>
  );
}
