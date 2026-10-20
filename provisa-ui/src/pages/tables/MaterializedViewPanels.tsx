// Copyright (c) 2026 Kenneth Stott
// Canary: 62aca526-f20e-4b92-8d60-f8278b278cb4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { Alert, Checkbox, NumberInput, Select, Text, Textarea } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable } from "../../types/admin";
import { useMaterializeStoreInfo } from "../../hooks/useAdminOpsQueries";
import { CollapsibleSection } from "./CollapsibleSection";
import { SnapshotSchedulePanel } from "./SnapshotSchedulePanel";
import { FieldGroup, FormChecks, FormField, FormLayout } from "../../components/form/FormLayout";
import { OffPeakWindowFields } from "../../components/admin/OffPeakWindowFields";
import { TimeTravelEditor } from "./TimeTravelSection";
import { useHistoryChoice } from "./timeTravel";

// REQ-961/962/965/969/970/1168/1169: the materialized-view configuration cluster — refresh cadence &
// NRT debounce, fleet consistency, keeping history (REQ-2109), the calendar snapshot schedule, and
// the persistence outcome (replace/append/upsert, incremental, preprocess). All panels read and write
// the staged RegisteredTable; extracting them keeps the parent form under its size budget.
export function MaterializedViewPanels({
  editingTable,
  setEditingTable,
  saved,
}: {
  editingTable: RegisteredTable;
  setEditingTable: (t: RegisteredTable) => void;
  /** The view as last saved, whether its versions exist is judged on it; null while created. */
  saved: RegisteredTable | null;
}) {
  const { t } = useTranslation();
  const { materializeStoreInfo: storeInfo } = useMaterializeStoreInfo();
  // REQ-2109: a view keeps history by declaring its business key, valid time and recorded time;
  // Provisa picks how the versions are stored. Without history the store is replace or upsert.
  const choice = useHistoryChoice(editingTable, (next) =>
    setEditingTable({ ...editingTable, ...next }),
  );
  const timeTravel = choice.on;
  const storeAsValue = editingTable.mvPersist === "upsert" ? "upsert" : "replace";
  return (
    <>
      {editingTable.materialize && (
        <CollapsibleSection
          title={t("tableEditForm.refreshPanel")}
          testId="mv-refresh-panel"
          // On a view's Load and timeliness tab, open: the tab is its section.
          flat
        >
          <FormField help={t("tableEditForm.refreshIntervalHelp")} size="short">
            <NumberInput
              label={t("tableEditForm.refreshIntervalLabel")}
              min={30}
              value={editingTable.mvRefreshInterval}
              onChange={(v) =>
                setEditingTable({
                  ...editingTable,
                  mvRefreshInterval: typeof v === "number" ? v : parseInt(String(v), 10) || 300,
                })
              }
            />
          </FormField>
          <FormField size="full" title={t("tableEditForm.nrtDebounceTitle")}>
            <Text size="sm" c="dimmed" mb={4}>
              {t("tableEditForm.nrtDebounceLabel")}
            </Text>
            <FieldGroup>
              <FormField size="short">
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
              </FormField>
              <FormField size="short">
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
              </FormField>
            </FieldGroup>
          </FormField>
          {/* REQ-2066: the table's load management window; on a view, when it may be read. */}
          <FormField size="full">
            <FormLayout>
              <OffPeakWindowFields
                window={editingTable.offPeakWindow ?? ""}
                tz={editingTable.offPeakTz ?? ""}
                windowHelp={t("tableEditForm.mvReadWindowHelp")}
                windowHint={t("tableEditForm.mvReadWindowHint")}
                onWindowChange={(window) =>
                  setEditingTable({ ...editingTable, offPeakWindow: window || null })
                }
                onTzChange={(tz) => setEditingTable({ ...editingTable, offPeakTz: tz || null })}
              />
            </FormLayout>
          </FormField>
        </CollapsibleSection>
      )}
      {/* Consistency is NOT a per-MV choice — it follows the deployment's materialization store,
          which is set by the engine default / Settings store URL. So instead of a dropdown, warn only
          when the resolved store is instance-local (a per-instance copy that diverges across
          instances). A shared store needs no note. */}
      {editingTable.materialize && storeInfo?.instanceLocalStore && (
        <FormField size="full">
          <Alert
            variant="light"
            color="yellow"
            title={t("tableEditForm.localStoreWarnTitle")}
            data-testid="mv-local-store-warning"
          >
            <Text size="sm">{t("tableEditForm.localStoreWarnBody")}</Text>
          </Alert>
        </FormField>
      )}
      {editingTable.materialize && (
        <SnapshotSchedulePanel editingTable={editingTable} setEditingTable={setEditingTable} />
      )}
      {editingTable.materialize && (
        <TimeTravelEditor
          editingTable={editingTable}
          setEditingTable={setEditingTable}
          saved={saved}
          choice={choice}
        />
      )}
      {/* Store As… — current state (replace / upsert), offered when the view keeps no history: a view
          keeping history appends its versions (REQ-965/1162/2109). Incremental is the separate
          compute optimization. */}
      {editingTable.materialize && !timeTravel && (
        <FormField help={t("tableEditForm.persistHelp")} size="medium">
          <Select
            label={t("tableEditForm.persistLabel")}
            aria-label={t("tableEditForm.persistAria")}
            data-testid="mv-persist"
            data={[
              { value: "replace", label: t("tableEditForm.storeReplace") },
              { value: "upsert", label: t("tableEditForm.storeUpsert") },
            ]}
            value={storeAsValue}
            onChange={(v) => setEditingTable({ ...editingTable, mvPersist: v || "replace" })}
            comboboxProps={{ withinPortal: true }}
            allowDeselect={false}
          />
        </FormField>
      )}
      <FormChecks>
        {editingTable.materialize && !timeTravel && (
          <FormField help={t("tableEditForm.incrementalHelp")} size="short">
            <Checkbox
              mt="1.75rem"
              data-testid="mv-incremental"
              checked={editingTable.mvIncremental}
              label={t("tableEditForm.incrementalLabel")}
              onChange={(e) =>
                setEditingTable({ ...editingTable, mvIncremental: e.currentTarget.checked })
              }
            />
          </FormField>
        )}
      </FormChecks>
      {/* REQ-970: upsert / incremental need a row-identity key. That key IS the table's Primary Key,
          set via the per-column checkboxes in the columns list below — no second definition here.
          Surface what's derived (or prompt to set it) so the requirement is visible where it's used. */}
      {editingTable.materialize &&
        !timeTravel &&
        (editingTable.mvPersist === "upsert" || editingTable.mvIncremental) &&
        (() => {
          const pkCols = editingTable.columns
            .filter((c) => c.isPrimaryKey)
            .map((c) => c.columnName);
          return pkCols.length > 0 ? (
            <FormField size="full">
              <Text size="sm" data-testid="mv-primary-key">
                {t("tableEditForm.mvPrimaryKeyDerived")} <code>{pkCols.join(", ")}</code>
              </Text>
            </FormField>
          ) : (
            <FormField size="full">
              <Alert variant="light" color="yellow" data-testid="mv-primary-key-missing">
                {t("tableEditForm.mvPrimaryKeyMissing")}
              </Alert>
            </FormField>
          );
        })()}
      {/* Plain-English summary of the chosen update strategy: current-state (replace/upsert, with
          optional incremental compute) vs history (every version appended). */}
      {editingTable.materialize && (
        <FormField size="full">
          <Alert
            variant="light"
            color="blue"
            title={t("tableEditForm.updateStrategyLabel")}
            data-testid="mv-update-strategy"
          >
            <Text size="sm">
              {timeTravel
                ? t("tableEditForm.updateStrategyHistory")
                : editingTable.mvIncremental
                  ? t("tableEditForm.updateStrategyIncremental")
                  : t(
                      editingTable.mvPersist === "upsert"
                        ? "tableEditForm.updateStrategyUpsert"
                        : "tableEditForm.updateStrategyReplace",
                    )}
            </Text>
          </Alert>
        </FormField>
      )}
      {editingTable.materialize && (
        <FormField help={t("tableEditForm.preprocessHelp")} size="full">
          <Textarea
            label={t("tableEditForm.preprocessLabel")}
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
        </FormField>
      )}
    </>
  );
}

/** A view's Materialized View tick, which its materialization panels follow. */
export function ViewMaterializeTick({
  editingTable,
  setEditingTable,
}: {
  editingTable: RegisteredTable;
  setEditingTable: (t: RegisteredTable) => void;
}) {
  const { t } = useTranslation();
  return (
    <FormChecks>
      <FormField
        size="short"
        help={`${t("tableEditForm.materializedViewDesc")} ${t("tableEditForm.materializedViewHelp")}`}
      >
        <Checkbox
          checked={editingTable.materialize}
          onChange={(e) =>
            setEditingTable({ ...editingTable, materialize: e.currentTarget.checked })
          }
          label={t("tableEditForm.materializedViewLabel")}
        />
      </FormField>
    </FormChecks>
  );
}
