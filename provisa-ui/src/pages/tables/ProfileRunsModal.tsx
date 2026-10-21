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
import { Alert, Badge, Box, Loader, Modal, Tabs, Text } from "@mantine/core";
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
    <ProfileResultTable<Record<string, unknown>>
      rows={rows}
      columns={keys.map((k) => ({ key: k, label: k, value: (r) => r[k], cell: (r) => cell(r[k]) }))}
      rowKey={(_r, i) => i}
      testId={testId}
      title={title}
      height={360}
    />
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
            rows={runs}
            expandable={false}
            height={200}
            title={t("profileRunsModal.title", { table: tableName })}
            testId="profile-runs-list"
            rowKey={(run) => run.run_id}
            rowTestId={(run) => `profile-run-${run.run_id}`}
            selected={(run) => run.run_id === selected}
            onRowClick={(run) => run.status === "succeeded" && setSelected(run.run_id)}
            columns={[
              {
                key: "run_time",
                label: t("profileRunsModal.runTime"),
                value: (run) => run.run_time,
                cell: (run) => new Date(run.run_time).toLocaleString(),
              },
              {
                key: "status",
                label: t("profileRunsModal.status"),
                value: (run) => t(`profileRunsModal.status_${run.status}`),
                cell: (run) => (
                  <Badge size="xs" color={run.status === "succeeded" ? "green" : "red"}>
                    {t(`profileRunsModal.status_${run.status}`)}
                  </Badge>
                ),
              },
              {
                key: "row_count",
                label: t("profileRunsModal.rowCount"),
                value: (run) => run.row_count,
                cell: (run) => cell(run.row_count),
              },
              {
                key: "profiled_rows",
                label: t("profileRunsModal.profiledRows"),
                value: (run) => run.profiled_rows,
                cell: (run) =>
                  `${cell(run.profiled_rows)}${run.sampled ? ` (${t("profileRunsModal.sampled")})` : ""}`,
              },
              {
                key: "method",
                label: t("profileRunsModal.method"),
                value: (run) =>
                  run.sample_method == null
                    ? ""
                    : t(`profileRunsModal.method_${run.sample_method}`),
                cell: (run) => (
                  <span data-testid={`profile-run-method-${run.run_id}`}>
                    {run.sample_method == null
                      ? ""
                      : t(`profileRunsModal.method_${run.sample_method}`)}
                    {run.sampled && run.sample_fraction != null
                      ? ` · ${t("profileRunsModal.share", {
                          pct: (run.sample_fraction * 100).toFixed(1),
                        })}`
                      : ""}
                  </span>
                ),
              },
              {
                key: "duplicates",
                label: t("profileRunsModal.duplicates"),
                value: (run) => run.duplicate_rows,
                cell: (run) => (
                  <span data-testid={`profile-run-duplicates-${run.run_id}`}>
                    {run.duplicate_rows == null
                      ? ""
                      : t("profileRunsModal.duplicateRows", {
                          rows: run.duplicate_rows,
                          pct: ((run.duplicate_share ?? 0) * 100).toFixed(1),
                        })}
                  </span>
                ),
              },
              {
                key: "duration",
                label: t("profileRunsModal.duration"),
                value: (run) => run.duration_ms,
                cell: (run) => t("profileRunsModal.ms", { ms: run.duration_ms }),
              },
              {
                key: "error",
                label: t("profileRunsModal.error"),
                value: (run) => run.error ?? "",
              },
            ]}
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
