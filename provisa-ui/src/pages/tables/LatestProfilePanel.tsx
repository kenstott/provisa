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
import { Box, Button, Group, SegmentedControl, Table, Text } from "@mantine/core";
import { runSql } from "../../api/admin";
import type { ProfileRun, ProfileRunResults } from "../../api/profiler";
import { tableSqlRef } from "../../naming";
import { useAuth } from "../../context/AuthContext";
import type { RegisteredTable } from "../../types/admin";
import { ProfileClassificationsModal } from "./ProfileClassificationsModal";
import { ProfileConstraintsModal } from "./ProfileConstraintsModal";
import { ProfileRunsModal } from "./ProfileRunsModal";
import { ProfileTerrain } from "./profile3d/terrain/ProfileTerrain";

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
  const { actingRoles } = useAuth();
  const [result, setResult] = useState<ProfileRows | null>(null);

  useEffect(() => {
    if (!actingRoles) return;
    const ref = tableSqlRef(profileTable);
    runSql(
      `SELECT * FROM ${ref} WHERE "run_time" = (SELECT MAX("run_time") FROM ${ref})`,
      actingRoles,
    ).then(setResult);
  }, [profileTable, actingRoles]);

  if (result == null) return null;
  return <ProfileColumnsTable result={result} />;
}

interface ProfileRows {
  columns: string[];
  rows: Record<string, unknown>[];
  error?: string;
}

/** A profile the server has already sanitized for its viewer (REQ-2180, A PROFILE SHOWN IS
 *  SANITIZED): the profiler's own run and results, for one table. */
export interface SanitizedProfile {
  tableId: number;
  tableName: string;
  run: ProfileRun | null;
  results: ProfileRunResults | null;
}

/**
 * REQ-2239 (amended 2026-11-11): the same latest profile, fed a profile the server has sanitized
 * rather than read here. ``owner`` mode is the full panel: the run history and its drift, the
 * constraints and the classification edits; ``consumer`` mode is read-only, with those off. Both
 * toggle between the columns table and its terrain.
 */
export function SanitizedProfilePanel({
  profile,
  mode,
  testId,
}: {
  profile: SanitizedProfile;
  mode: "owner" | "consumer";
  testId: string;
}) {
  const { t } = useTranslation();
  const [view, setView] = useState<"table" | "terrain">("table");
  const [open, setOpen] = useState<"runs" | "constraints" | "classifications" | null>(null);
  const rows = profile.results?.columns ?? [];
  const result: ProfileRows = {
    columns: rows.length > 0 ? ["run_time", ...Object.keys(rows[0])] : [],
    rows: rows.map((r) => ({ run_time: profile.run?.run_time ?? "", ...r })),
  };
  const close = () => setOpen(null);
  return (
    <div data-testid={testId}>
      <Group gap="xs" justify="space-between" wrap="wrap">
        {profile.results != null && (
          <SegmentedControl
            size="xs"
            value={view}
            onChange={(v) => setView(v as "table" | "terrain")}
            data={[
              { value: "table", label: t("latestProfilePanel.viewTable") },
              { value: "terrain", label: t("latestProfilePanel.viewTerrain") },
            ]}
            data-testid={`${testId}-view`}
          />
        )}
        {mode === "owner" && (
          <Group gap="xs" data-testid={`${testId}-owner`}>
            <Button size="xs" variant="light" onClick={() => setOpen("runs")}>
              {t("profilerPanel.viewRuns")}
            </Button>
            <Button size="xs" variant="light" onClick={() => setOpen("constraints")}>
              {t("profilerPanel.constraints")}
            </Button>
            <Button size="xs" variant="light" onClick={() => setOpen("classifications")}>
              {t("profilerPanel.classifications")}
            </Button>
          </Group>
        )}
      </Group>
      {profile.results == null ? (
        <Text size="sm" c="dimmed" mt="xs" data-testid={`${testId}-none`}>
          {t("latestProfilePanel.none")}
        </Text>
      ) : view === "terrain" ? (
        <ProfileTerrain
          results={profile.results}
          tableId={profile.tableId}
          tableName={profile.tableName}
        />
      ) : (
        <ProfileColumnsTable result={result} />
      )}
      {open === "runs" && (
        <ProfileRunsModal tableId={profile.tableId} tableName={profile.tableName} onClose={close} />
      )}
      {open === "constraints" && (
        <ProfileConstraintsModal
          tableId={profile.tableId}
          tableName={profile.tableName}
          onClose={close}
        />
      )}
      {open === "classifications" && (
        <ProfileClassificationsModal
          tableId={profile.tableId}
          tableName={profile.tableName}
          onClose={close}
        />
      )}
    </div>
  );
}

/** A latest run's per-column rows, the run's keys left out. */
function ProfileColumnsTable({ result }: { result: ProfileRows }) {
  const { t } = useTranslation();
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
