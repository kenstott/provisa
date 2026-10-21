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
import { Alert, Box, Loader, Modal, Tabs, Text } from "@mantine/core";
import {
  fetchProfileRun,
  fetchProfileRuns,
  type ProfileRun,
  type ProfileRunResults,
} from "../../api/profiler";
import { useAuth } from "../../context/AuthContext";
import { ProfileChecksBar } from "./ProfileChecksBar";
import { ProfileDriftPanel } from "./ProfileDriftPanel";
import { ProfileResultTable } from "./ProfileResultTable";
import { ProfileDriftStrata } from "./profile3d/drift/ProfileDriftStrata";
import { ProfileJointCloud } from "./profile3d/joint/ProfileJointCloud";
import { ProfileRelations } from "./profile3d/relations/ProfileRelations";
import { ProfileTerrain } from "./profile3d/terrain/ProfileTerrain";

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
  "correlations",
  "dependencies",
  "joint_counts",
  "constraints",
  "constraint_checks",
  "duplicates",
  "repeats",
] as const;
const RUN_KEY = new Set(["run_id", "run_time"]);
// The key a run's row keeps its run's id under; never shown.
const RUN_ID = "\u0000run_id";
// REQ-2123, REQ-2124: the 3D views of a run, as tabs ahead of the result relations.
const VIEWS_3D = ["terrain", "relationsMap", "jointCloud", "driftStrata"] as const;

function cell(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "number")
    return Number.isInteger(value) ? String(value) : value.toPrecision(6);
  return String(value);
}

function RowsTable({
  rows,
  testId,
  title,
}: {
  rows: Record<string, unknown>[];
  testId: string;
  title: string;
}) {
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
    <ProfileResultTable rows={rows} columns={keys} testId={testId} title={title} height={360} />
  );
}

// REQ-1934: a member table's run history and one run's results, as the profiler wrote them. Opened
// from the table editor and, since REQ-2123's amendment, from its read-only panel too (not for a
// governance-only viewer, who profiles nothing).
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

  // The runs as the grid shows them: one plain value per column, each row keeping its run's id.
  const runColumns = [
    t("profileRunsModal.runTime"),
    t("profileRunsModal.status"),
    t("profileRunsModal.rowCount"),
    t("profileRunsModal.profiledRows"),
    t("profileRunsModal.method"),
    t("profileRunsModal.duplicates"),
    t("profileRunsModal.duration"),
    t("profileRunsModal.error"),
  ];
  const runRows = (runs ?? []).map((run) => {
    const values = [
      new Date(run.run_time).toLocaleString(),
      t(`profileRunsModal.status_${run.status}`),
      run.row_count,
      `${cell(run.profiled_rows)}${run.sampled ? ` (${t("profileRunsModal.sampled")})` : ""}`,
      `${run.sample_method == null ? "" : t(`profileRunsModal.method_${run.sample_method}`)}${
        run.sampled && run.sample_fraction != null
          ? ` · ${t("profileRunsModal.share", { pct: (run.sample_fraction * 100).toFixed(1) })}`
          : ""
      }`,
      run.duplicate_rows == null
        ? null
        : t("profileRunsModal.duplicateRows", {
            rows: run.duplicate_rows,
            pct: ((run.duplicate_share ?? 0) * 100).toFixed(1),
          }),
      t("profileRunsModal.ms", { ms: run.duration_ms }),
      run.error,
    ];
    return {
      [RUN_ID]: run.run_id,
      ...Object.fromEntries(runColumns.map((c, i) => [c, values[i]])),
    };
  });

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
      <ProfileChecksBar tableId={tableId} />
      {runs == null && !error && <Loader size="sm" />}
      {runs != null && runs.length === 0 && (
        <Text size="sm" c="dimmed" data-testid="profile-runs-none">
          {t("profileRunsModal.none")}
        </Text>
      )}
      {runs != null && runs.length > 0 && (
        <Box mb="md">
          <ProfileResultTable
            rows={runRows}
            columns={runColumns}
            expandable={false}
            height={220}
            title={t("profileRunsModal.title", { table: tableName })}
            testId="profile-runs-list"
            selectedRow={(row) => row[RUN_ID] === selected}
            onRowClick={(row) => {
              const run = runs.find((r) => r.run_id === row[RUN_ID]);
              if (run?.status === "succeeded") setSelected(run.run_id);
            }}
          />
        </Box>
      )}
      {selected != null && results == null && !error && <Loader size="sm" />}
      {results != null && (
        // REQ-2123 (amended): the many result tabs read down the side, each title on its own line.
        <Tabs
          defaultValue="columns"
          keepMounted={false}
          orientation="vertical"
          // A panel beside the tabs is as wide as is left: a wide table scrolls inside it.
          styles={{ panel: { minWidth: 0 } }}
        >
          <Tabs.List>
            {/* REQ-2123, REQ-2124: the run drawn in 3D, ahead of its result relations. */}
            {VIEWS_3D.map((v) => (
              <Tabs.Tab key={v} value={v} data-testid={`profile-tab-${v}`}>
                {t(`profileRunsModal.kind_${v}`)}
              </Tabs.Tab>
            ))}
            {KINDS.map((k) => (
              <Tabs.Tab key={k} value={k} data-testid={`profile-tab-${k}`}>
                {t(`profileRunsModal.kind_${k}`)}
              </Tabs.Tab>
            ))}
          </Tabs.List>
          <Tabs.Panel value="terrain" pl="sm">
            <ProfileTerrain results={results} tableName={tableName} />
          </Tabs.Panel>
          <Tabs.Panel value="relationsMap" pl="sm">
            <ProfileRelations results={results} tableName={tableName} />
          </Tabs.Panel>
          <Tabs.Panel value="jointCloud" pl="sm">
            <ProfileJointCloud results={results} tableName={tableName} />
          </Tabs.Panel>
          <Tabs.Panel value="driftStrata" pl="sm">
            {selected != null && runs != null && role && (
              <ProfileDriftStrata
                tableId={tableId}
                role={role.id}
                runs={runs}
                selectedRunId={selected}
                results={results}
                tableName={tableName}
              />
            )}
          </Tabs.Panel>
          {KINDS.map((k) => (
            <Tabs.Panel key={k} value={k} pl="sm">
              {k === "drift" && selected != null && role ? (
                <ProfileDriftPanel
                  tableId={tableId}
                  runId={selected}
                  role={role.id}
                  rows={results.drift ?? []}
                />
              ) : (
                <RowsTable
                  rows={results[k] ?? []}
                  testId={`profile-rows-${k}`}
                  title={`${tableName} · ${t(`profileRunsModal.kind_${k}`)}`}
                />
              )}
            </Tabs.Panel>
          ))}
        </Tabs>
      )}
    </Modal>
  );
}
