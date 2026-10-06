// Copyright (c) 2026 Kenneth Stott
// Canary: e4b71c08-29d5-4a3f-b6e8-0c5d1f9a7b23
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Select, Text } from "@mantine/core";
import type { RegisteredTable } from "../../types/admin";
import { fetchProfilers, runProfileNow, type Profiler } from "../../api/profiler";
import { CollapsibleSection } from "./CollapsibleSection";
import { ProfileRunsModal } from "./ProfileRunsModal";
import { requiredParamColumns } from "../../components/nativeParams";
import { ProfileConstraintsModal } from "./ProfileConstraintsModal";

// REQ-1934: a table's Data Profiler membership. Joining or leaving is a field of the table, saved
// with it; Run Profile Now and View Profile Runs act on the saved member. The editor is shown only to
// those who may edit the table, which is who may see its runs.
export function ProfilerPanel({
  editingTable,
  savedProfilerId,
  setEditingTable,
}: {
  editingTable: RegisteredTable;
  /** The membership as last saved — Run Now and the runs act on what the server holds. */
  savedProfilerId: string | null;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
}) {
  const { t } = useTranslation();
  // A table whose rows need a value for a required filter (_nf_* path parameters) has no whole
  // table to profile, so it cannot join a profiler.
  const needsFilter = requiredParamColumns(editingTable).length > 0;
  const [profilers, setProfilers] = useState<Profiler[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [runMessage, setRunMessage] = useState<{ ok: boolean; text: string } | null>(null);
  const [runsOpen, setRunsOpen] = useState(false);
  const [constraintsOpen, setConstraintsOpen] = useState(false);

  const memberOf = editingTable.profilerSourceId ?? null;
  // Loaded when needed — to show a member's schedule, or when the picker opens — so an editor of a
  // table that never looks at profiling asks nothing of the server.
  const [wanted, setWanted] = useState(memberOf != null);
  useEffect(() => {
    if (!wanted || profilers != null) return;
    fetchProfilers()
      .then(setProfilers)
      .catch((e: unknown) => setLoadError(e instanceof Error ? e.message : String(e)));
  }, [wanted, profilers]);

  const profiler = profilers?.find((p) => p.id === memberOf) ?? null;
  const unsaved = memberOf !== savedProfilerId;
  const schedule = (p: Profiler) =>
    p.sampleAboveCells == null
      ? t("profilerPanel.scheduleWhole", { cron: p.cron })
      : t("profilerPanel.scheduleSample", { cron: p.cron, cells: p.sampleAboveCells });

  const runNow = async () => {
    setRunning(true);
    setRunMessage(null);
    try {
      const run = await runProfileNow(editingTable.id);
      setRunMessage({
        ok: true,
        text: t("profilerPanel.runDone", { rows: run.profiledRows ?? 0 }),
      });
    } catch (e: unknown) {
      setRunMessage({ ok: false, text: e instanceof Error ? e.message : String(e) });
    } finally {
      setRunning(false);
    }
  };

  if (needsFilter) return null;
  return (
    <div style={{ paddingInline: "1.5rem" }}>
      <CollapsibleSection
        title={t("profilerPanel.title")}
        testId="profiler-panel"
        tourId="profiler-panel"
        badge={memberOf ?? undefined}
        defaultOpen={memberOf != null}
      >
        {loadError && (
          <Alert color="red" data-testid="profiler-load-error">
            {loadError}
          </Alert>
        )}
        {memberOf == null ? (
          <Select
            label={t("profilerPanel.addLabel")}
            placeholder={t("profilerPanel.addPlaceholder")}
            description={t("profilerPanel.addDescription")}
            data-testid="profiler-add"
            data={(profilers ?? []).map((p) => ({
              value: p.id,
              label: `${p.id} — ${schedule(p)}`,
            }))}
            value={null}
            onDropdownOpen={() => setWanted(true)}
            nothingFoundMessage={t("profilerPanel.none")}
            onChange={(v) => v && setEditingTable({ ...editingTable, profilerSourceId: v })}
          />
        ) : (
          <>
            <Text size="sm" data-testid="profiler-member">
              {t("profilerPanel.memberOf", { profiler: memberOf })}
              {profiler ? ` — ${schedule(profiler)}` : ""}
            </Text>
            {unsaved && (
              <Text size="xs" c="dimmed" data-testid="profiler-unsaved">
                {t("profilerPanel.unsaved")}
              </Text>
            )}
            <Group gap="xs" mt="xs">
              <Button
                size="xs"
                variant="default"
                data-testid="profiler-remove"
                onClick={() => setEditingTable({ ...editingTable, profilerSourceId: null })}
              >
                {t("profilerPanel.remove")}
              </Button>
              <Button
                size="xs"
                data-testid="profiler-run-now"
                loading={running}
                disabled={unsaved}
                onClick={runNow}
              >
                {t("profilerPanel.runNow")}
              </Button>
              <Button
                size="xs"
                variant="light"
                data-testid="profiler-view-runs"
                disabled={unsaved}
                onClick={() => setRunsOpen(true)}
              >
                {t("profilerPanel.viewRuns")}
              </Button>
              <Button
                size="xs"
                variant="light"
                data-testid="profiler-constraints"
                disabled={unsaved}
                onClick={() => setConstraintsOpen(true)}
              >
                {t("profilerPanel.constraints")}
              </Button>
            </Group>
          </>
        )}
        {memberOf == null && unsaved && (
          <Text size="xs" c="dimmed" data-testid="profiler-unsaved">
            {t("profilerPanel.unsaved")}
          </Text>
        )}
        {runMessage && (
          <Alert color={runMessage.ok ? "green" : "red"} mt="xs" data-testid="profiler-run-message">
            {runMessage.text}
          </Alert>
        )}
      </CollapsibleSection>
      {runsOpen && (
        <ProfileRunsModal
          tableId={editingTable.id}
          tableName={editingTable.tableName}
          onClose={() => setRunsOpen(false)}
        />
      )}
      {constraintsOpen && (
        <ProfileConstraintsModal
          tableId={editingTable.id}
          tableName={editingTable.tableName}
          onClose={() => setConstraintsOpen(false)}
        />
      )}
    </div>
  );
}
