// Copyright (c) 2026 Kenneth Stott
// Canary: 8159e9f3-f071-45bc-aa4b-592d1481a773
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386: admin Reports viewer. A report IS an ops-domain registered table:
// the seeded management views appear automatically, and a custom report is a
// governed Provisa view registered in the ops domain (so it is cataloged,
// exportable, and access-controlled like any table). Rows load through the one
// governed SQL pipeline; sort/filter/group happen client-side in ResultsGrid.

import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Button,
  Group,
  Loader,
  Modal,
  NavLink,
  Stack,
  Text,
  TextInput,
  Textarea,
  Tooltip,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Plus, Trash2 } from "lucide-react";
import { useDeleteTable, useRegisterTable, useTables } from "../../hooks/useAdminQueries";
import { DERIVED_SOURCE_ID } from "../../types/admin";
import type { RegisteredTable } from "../../types/admin";
import { GovernedTableViewer } from "../GovernedTableViewer";

export function ReportsTab() {
  const { t } = useTranslation();
  const { tables, loading: tablesLoading, refetch: refetchTables } = useTables();
  const { registerTable } = useRegisterTable();
  const { deleteTable } = useDeleteTable();

  const reports = useMemo(
    () =>
      tables
        .filter((tbl) => tbl.domainId === "ops")
        .sort((a, b) => (a.alias || a.tableName).localeCompare(b.alias || b.tableName)),
    [tables],
  );

  // First report auto-selects via the derived fallback; the viewer remounts per
  // report (key) and owns fetching + grid state.
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const selected = reports.find((r) => r.id === selectedId) ?? reports[0] ?? null;

  const [listWidth, setListWidth] = useState(240);
  const startDrag = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      const startX = e.clientX;
      const startW = listWidth;
      const onMove = (ev: MouseEvent) =>
        setListWidth(Math.max(160, Math.min(560, startW + ev.clientX - startX)));
      const onUp = () => {
        window.removeEventListener("mousemove", onMove);
        window.removeEventListener("mouseup", onUp);
      };
      window.addEventListener("mousemove", onMove);
      window.addEventListener("mouseup", onUp);
    },
    [listWidth],
  );

  const [addOpen, setAddOpen] = useState(false);
  const [addName, setAddName] = useState("");
  const [addDescription, setAddDescription] = useState("");
  const [addSql, setAddSql] = useState("");
  const [addSaving, setAddSaving] = useState(false);
  const [addError, setAddError] = useState("");

  const handleSelect = (report: RegisteredTable) => {
    setSelectedId(report.id);
  };

  const handleAdd = async () => {
    if (!addName.trim() || !addSql.trim()) return;
    setAddSaving(true);
    setAddError("");
    try {
      await registerTable({
        sourceId: DERIVED_SOURCE_ID,
        domainId: "ops",
        schemaName: "views",
        tableName: addName.trim(),
        alias: addName.trim(),
        description: addDescription.trim() || undefined,
        viewSql: addSql.trim(),
        columns: [],
      });
      notifications.show({ color: "green", message: t("reportsTab.added", { name: addName.trim() }) });
      setAddOpen(false);
      setAddName("");
      setAddDescription("");
      setAddSql("");
      refetchTables();
    } catch (e) {
      setAddError(e instanceof Error ? e.message : String(e));
    } finally {
      setAddSaving(false);
    }
  };

  const handleDelete = async (report: RegisteredTable) => {
    if (!window.confirm(t("reportsTab.deleteConfirm", { name: report.alias || report.tableName })))
      return;
    const result = await deleteTable(report.id);
    if (result.success) {
      notifications.show({
        color: "green",
        message: t("reportsTab.deleted", { name: report.alias || report.tableName }),
      });
      if (selectedId === report.id) setSelectedId(null);
      refetchTables();
    } else {
      notifications.show({ color: "red", message: result.message });
    }
  };

  return (
    <div style={{ display: "flex", gap: "0.75rem", height: "calc(100vh - 180px)", minHeight: 420 }}>
      <div
        style={{
          width: listWidth,
          flexShrink: 0,
          display: "flex",
          flexDirection: "column",
        }}
        data-testid="reports-list"
      >
        <Group justify="space-between" px="xs" py={6}>
          <Text size="sm" fw={600}>
            {t("reportsTab.title")}
          </Text>
          <Button
            size="compact-xs"
            variant="default"
            leftSection={<Plus size={12} />}
            onClick={() => setAddOpen(true)}
            data-testid="add-report-btn"
          >
            {t("reportsTab.add")}
          </Button>
        </Group>
        <div style={{ flex: 1, overflow: "auto" }}>
          {tablesLoading && reports.length === 0 ? (
            <Group justify="center" py="md">
              <Loader size="xs" />
            </Group>
          ) : reports.length === 0 ? (
            <Text size="xs" c="dimmed" px="xs" py="sm">
              {t("reportsTab.empty")}
            </Text>
          ) : (
            reports.map((report) => {
              const custom = report.sourceId === DERIVED_SOURCE_ID;
              return (
                <NavLink
                  key={report.id}
                  active={report.id === selectedId}
                  label={report.alias || report.tableName}
                  description={report.description || undefined}
                  onClick={() => handleSelect(report)}
                  data-testid={`report-item-${report.tableName}`}
                  rightSection={
                    custom ? (
                      <Tooltip label={t("reportsTab.delete")}>
                        <ActionIcon
                          variant="subtle"
                          color="red"
                          size="sm"
                          aria-label={t("reportsTab.delete")}
                          onClick={(e) => {
                            e.stopPropagation();
                            void handleDelete(report);
                          }}
                        >
                          <Trash2 size={13} />
                        </ActionIcon>
                      </Tooltip>
                    ) : undefined
                  }
                />
              );
            })
          )}
        </div>
      </div>

      {/* The rule between the list and the report IS the grab handle: a 1px line drawn inside a
          wider hit area so it stays easy to grab without thickening the divider. */}
      <div
        role="separator"
        aria-orientation="vertical"
        aria-label={t("reportsTab.resize")}
        onMouseDown={startDrag}
        data-testid="reports-resize-handle"
        style={{
          flexShrink: 0,
          width: 9,
          marginInline: "-0.375rem",
          cursor: "col-resize",
          display: "flex",
          justifyContent: "center",
        }}
      >
        <div style={{ width: 1, height: "100%", background: "var(--border)" }} />
      </div>

      {selected ? (
        <GovernedTableViewer key={selected.id} table={selected} showTitle />
      ) : (
        <div style={{ flex: 1 }} />
      )}

      <Modal
        opened={addOpen}
        onClose={() => setAddOpen(false)}
        title={t("reportsTab.addTitle")}
        size="lg"
      >
        <Stack gap="sm">
          <Text size="xs" c="dimmed">
            {t("reportsTab.addHint")}
          </Text>
          <TextInput
            label={t("reportsTab.nameLabel")}
            value={addName}
            onChange={(e) => setAddName(e.currentTarget.value)}
            data-testid="report-name-input"
            required
          />
          <TextInput
            label={t("reportsTab.descriptionLabel")}
            value={addDescription}
            onChange={(e) => setAddDescription(e.currentTarget.value)}
            data-testid="report-description-input"
          />
          <Textarea
            label={t("reportsTab.sqlLabel")}
            value={addSql}
            onChange={(e) => setAddSql(e.currentTarget.value)}
            autosize
            minRows={5}
            styles={{ input: { fontFamily: "monospace" } }}
            data-testid="report-sql-input"
            required
          />
          {addError && (
            <Alert color="red" data-testid="report-add-error">
              {addError}
            </Alert>
          )}
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setAddOpen(false)}>
              {t("reportsTab.cancel")}
            </Button>
            <Button
              onClick={() => void handleAdd()}
              loading={addSaving}
              disabled={!addName.trim() || !addSql.trim()}
              data-testid="report-save-btn"
            >
              {t("reportsTab.save")}
            </Button>
          </Group>
        </Stack>
      </Modal>
    </div>
  );
}
