// Copyright (c) 2026 Kenneth Stott
// Canary: 62aca526-f20e-4b92-8d60-f8278b278cb4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { Alert, Checkbox, Group, NumberInput, Select, Text, Textarea, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable } from "../../types/admin";
import { useMaterializeStoreInfo } from "../../hooks/useAdminQueries";
import { CollapsibleSection } from "./CollapsibleSection";
import { FieldLabel } from "./FieldLabel";
import { SnapshotSchedulePanel } from "./SnapshotSchedulePanel";

// REQ-961/962/965/969/970/1168/1169: the materialized-view configuration cluster — refresh cadence &
// NRT debounce, fleet consistency, time-travel (bitemporal) mode, the calendar snapshot schedule, and
// the persistence outcome (replace/append/upsert, incremental, preprocess). All panels read and write
// the staged RegisteredTable; extracting them keeps the parent form under its size budget.
export function MaterializedViewPanels({
  editingTable,
  setEditingTable,
}: {
  editingTable: RegisteredTable;
  setEditingTable: (t: RegisteredTable) => void;
}) {
  const { t } = useTranslation();
  const { materializeStoreInfo: storeInfo } = useMaterializeStoreInfo();
  // Store As… is ONE decision spanning two engine axes: keep history (Time Travel / bitemporal) or
  // not. When history is kept the write mode is snapshot (append all) vs delta (append revisions);
  // when it is not, it is replace vs upsert (current state). Bitemporal mode supersedes the persist
  // axis in the engine, so they are surfaced as a single control here rather than two panels.
  const timeTravel = Boolean(editingTable.mvBitemporalMode);
  const storeAsValue = timeTravel
    ? (editingTable.mvBitemporalMode as string)
    : editingTable.mvPersist === "upsert"
      ? "upsert"
      : "replace";
  return (
    <>
      {editingTable.materialize && (
        <CollapsibleSection
          title={t("tableEditForm.refreshPanel")}
          testId="mv-refresh-panel"
          defaultOpen
        >
          <NumberInput
            label={
              <FieldLabel
                text={t("tableEditForm.refreshIntervalLabel")}
                help={t("tableEditForm.refreshIntervalHelp")}
              />
            }
            min={30}
            value={editingTable.mvRefreshInterval}
            onChange={(v) =>
              setEditingTable({
                ...editingTable,
                mvRefreshInterval: typeof v === "number" ? v : parseInt(String(v), 10) || 300,
              })
            }
          />
          <div title={t("tableEditForm.nrtDebounceTitle")}>
            <Text size="sm" c="dimmed" mb={4}>
              {t("tableEditForm.nrtDebounceLabel")}
            </Text>
            <Group gap="xs">
              <NumberInput
                min={0}
                step={0.5}
                placeholder={t("tableEditForm.nrtQuietPlaceholder")}
                aria-label={t("tableEditForm.nrtQuietAria")}
                data-testid="mv-debounce-quiet"
                value={editingTable.mvDebounceQuiet}
                onChange={(v) =>
                  setEditingTable({
                    ...editingTable,
                    mvDebounceQuiet: typeof v === "number" ? v : parseFloat(String(v)) || 0,
                  })
                }
              />
              <NumberInput
                min={0}
                step={0.5}
                placeholder={t("tableEditForm.nrtMaxDelayPlaceholder")}
                aria-label={t("tableEditForm.nrtMaxDelayAria")}
                data-testid="mv-debounce-max-delay"
                value={editingTable.mvDebounceMaxDelay}
                onChange={(v) =>
                  setEditingTable({
                    ...editingTable,
                    mvDebounceMaxDelay: typeof v === "number" ? v : parseFloat(String(v)) || 0,
                  })
                }
              />
            </Group>
          </div>
        </CollapsibleSection>
      )}
      {/* Consistency is NOT a per-MV choice — it follows the deployment's materialization store,
          which is set by the engine default / Settings store URL. So instead of a dropdown, warn only
          when the resolved store is instance-local (a per-instance copy that diverges across
          instances). A shared store needs no note. */}
      {editingTable.materialize && storeInfo?.instanceLocalStore && (
        <Alert
          style={{ gridColumn: "1 / -1" }}
          variant="light"
          color="yellow"
          title={t("tableEditForm.localStoreWarnTitle")}
          data-testid="mv-local-store-warning"
        >
          <Text size="sm">{t("tableEditForm.localStoreWarnBody")}</Text>
        </Alert>
      )}
      {editingTable.materialize && (
        <SnapshotSchedulePanel editingTable={editingTable} setEditingTable={setEditingTable} />
      )}
      {/* Store As… — ONE control. Options depend on the Time Travel (keep-history) toggle: current
          state (replace / upsert) when off, history (snapshot = append all / delta = append revisions)
          when on. In the engine a bitemporal mode SUPERSEDES the persist axis, so this is one decision,
          not two overlapping panels (REQ-965/1162). Incremental is the separate compute optimization. */}
      {editingTable.materialize && (
        <Select
          label={
            <FieldLabel
              text={t("tableEditForm.persistLabel")}
              help={t("tableEditForm.persistHelp")}
            />
          }
          aria-label={t("tableEditForm.persistAria")}
          data-testid="mv-persist"
          data={
            timeTravel
              ? [
                  { value: "snapshot", label: t("tableEditForm.storeSnapshot") },
                  { value: "delta", label: t("tableEditForm.storeDelta") },
                ]
              : [
                  { value: "replace", label: t("tableEditForm.storeReplace") },
                  { value: "upsert", label: t("tableEditForm.storeUpsert") },
                ]
          }
          value={storeAsValue}
          onChange={(v) => {
            const nv = v || (timeTravel ? "delta" : "replace");
            if (timeTravel) {
              setEditingTable({ ...editingTable, mvBitemporalMode: nv });
            } else {
              setEditingTable({ ...editingTable, mvPersist: nv });
            }
          }}
          comboboxProps={{ withinPortal: true }}
          allowDeselect={false}
        />
      )}
      {editingTable.materialize && (
        <Checkbox
          mt="1.75rem"
          data-testid="mv-timetravel"
          checked={timeTravel}
          label={
            <FieldLabel
              text={t("tableEditForm.timeTravelToggleLabel")}
              help={t("tableEditForm.bitemporalHelp")}
            />
          }
          onChange={(e) =>
            setEditingTable({
              ...editingTable,
              // ON → default to delta (compact history); OFF → clear the mode so the engine uses the
              // current-state persist axis (replace/upsert).
              mvBitemporalMode: e.currentTarget.checked
                ? editingTable.mvBitemporalMode || "delta"
                : null,
            })
          }
        />
      )}
      {editingTable.materialize && !timeTravel && (
        <Checkbox
          mt="1.75rem"
          data-testid="mv-incremental"
          checked={editingTable.mvIncremental}
          label={
            <FieldLabel
              text={t("tableEditForm.incrementalLabel")}
              help={t("tableEditForm.incrementalHelp")}
            />
          }
          onChange={(e) =>
            setEditingTable({ ...editingTable, mvIncremental: e.currentTarget.checked })
          }
        />
      )}
      {editingTable.materialize && timeTravel && (
        <TextInput
          label={
            <FieldLabel
              text={t("tableEditForm.bitemporalKeyLabel")}
              help={t("tableEditForm.bitemporalKeyHelp")}
            />
          }
          aria-label={t("tableEditForm.bitemporalKeyAria")}
          data-testid="mv-bitemporal-key"
          placeholder={t("tableEditForm.bitemporalKeyPlaceholder")}
          value={editingTable.mvBitemporalKey.join(", ")}
          onChange={(e) =>
            setEditingTable({
              ...editingTable,
              mvBitemporalKey: e.currentTarget.value
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean),
            })
          }
        />
      )}
      {/* REQ-970: upsert / incremental need a row-identity key. That key IS the table's Primary Key,
          set via the per-column checkboxes in the columns list below — no second definition here.
          Surface what's derived (or prompt to set it) so the requirement is visible where it's used. */}
      {editingTable.materialize &&
        !editingTable.mvBitemporalMode &&
        (editingTable.mvPersist === "upsert" || editingTable.mvIncremental) &&
        (() => {
          const pkCols = editingTable.columns
            .filter((c) => c.isPrimaryKey)
            .map((c) => c.columnName);
          return pkCols.length > 0 ? (
            <Text size="sm" style={{ gridColumn: "1 / -1" }} data-testid="mv-primary-key">
              {t("tableEditForm.mvPrimaryKeyDerived")} <code>{pkCols.join(", ")}</code>
            </Text>
          ) : (
            <Alert
              style={{ gridColumn: "1 / -1" }}
              variant="light"
              color="yellow"
              data-testid="mv-primary-key-missing"
            >
              {t("tableEditForm.mvPrimaryKeyMissing")}
            </Alert>
          );
        })()}
      {/* Plain-English summary of the chosen update strategy: current-state (replace/upsert, with
          optional incremental compute) vs history (snapshot = append all / delta = append revisions). */}
      {editingTable.materialize && (
        <Alert
          style={{ gridColumn: "1 / -1" }}
          variant="light"
          color="blue"
          title={t("tableEditForm.updateStrategyLabel")}
          data-testid="mv-update-strategy"
        >
          <Text size="sm">
            {timeTravel
              ? t(
                  editingTable.mvBitemporalMode === "snapshot"
                    ? "tableEditForm.updateStrategySnapshot"
                    : "tableEditForm.updateStrategyDelta",
                )
              : editingTable.mvIncremental
                ? t("tableEditForm.updateStrategyIncremental")
                : t(
                    editingTable.mvPersist === "upsert"
                      ? "tableEditForm.updateStrategyUpsert"
                      : "tableEditForm.updateStrategyReplace",
                  )}
          </Text>
        </Alert>
      )}
      {editingTable.materialize && (
        <Textarea
          style={{ gridColumn: "1 / -1" }}
          label={
            <FieldLabel
              text={t("tableEditForm.preprocessLabel")}
              help={t("tableEditForm.preprocessHelp")}
            />
          }
          aria-label={t("tableEditForm.preprocessAria")}
          data-testid="mv-preprocess"
          placeholder={t("tableEditForm.preprocessPlaceholder")}
          autosize
          minRows={4}
          maxRows={16}
          spellCheck={false}
          styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
          value={editingTable.mvPreprocess ?? ""}
          onChange={(e) =>
            setEditingTable({ ...editingTable, mvPreprocess: e.currentTarget.value || null })
          }
        />
      )}
    </>
  );
}
