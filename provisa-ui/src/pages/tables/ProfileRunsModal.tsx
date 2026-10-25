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
import { Alert, Box, Loader, Text } from "@mantine/core";
import { FitModal } from "./profile3d/Expandable3D";
import { VerticalNav } from "../../components/VerticalNav";
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
  "classifications",
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
    <ProfileResultTable rows={rows} columns={keys} testId={testId} title={title} height="fill" />
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
  const [tab, setTab] = useState<string>("columns");

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
    // The modal never scrolls: the runs keep their height, and beneath them the menu and the chosen
    // result each scroll on their own, side by side, down to the dialog's bottom edge.
    <FitModal
      opened
      onClose={onClose}
      size="90%"
      title={t("profileRunsModal.title", { table: tableName })}
      testId="profile-runs-modal"
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
        <Box mb="md" style={{ flex: "none" }}>
          <ProfileResultTable
            rows={runRows}
            columns={runColumns}
            expandable={false}
            height={220}
            fitRows
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
        // REQ-2123 (amended): the many results read down the side, each title on its own line, in
        // the app's one vertical menu; the chosen one fills what is left of the dialog.
        <div className="profile-runs-results">
          <VerticalNav
            label={t("profileRunsModal.resultsMenu")}
            items={[...VIEWS_3D, ...KINDS].map((k) => ({
              value: k,
              label: t(`profileRunsModal.kind_${k}`),
              testId: `profile-tab-${k}`,
            }))}
            value={tab}
            onChange={setTab}
            className="profile-runs-menu"
            testId="profile-runs-menu"
          />
          <div className="profile-runs-panel" data-testid={`profile-panel-${tab}`}>
            {tab === "terrain" ? (
              <ProfileTerrain results={results} tableId={tableId} tableName={tableName} />
            ) : tab === "relationsMap" ? (
              <ProfileRelations results={results} tableId={tableId} tableName={tableName} />
            ) : tab === "jointCloud" ? (
              <ProfileJointCloud results={results} tableId={tableId} tableName={tableName} />
            ) : tab === "driftStrata" ? (
              selected != null &&
              runs != null &&
              role && (
                <ProfileDriftStrata
                  tableId={tableId}
                  role={role.id}
                  runs={runs}
                  selectedRunId={selected}
                  results={results}
                  tableName={tableName}
                />
              )
            ) : tab === "drift" && selected != null && role ? (
              <ProfileDriftPanel
                tableId={tableId}
                runId={selected}
                role={role.id}
                rows={results.drift ?? []}
              />
            ) : (
              <RowsTable
                rows={results[tab as (typeof KINDS)[number]] ?? []}
                testId={`profile-rows-${tab}`}
                title={`${tableName} · ${t(`profileRunsModal.kind_${tab}`)}`}
              />
            )}
          </div>
        </div>
      )}
    </FitModal>
  );
}
