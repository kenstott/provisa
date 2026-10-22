// Copyright (c) 2026 Kenneth Stott
// Canary: 41c22c45-1a70-40af-add1-772d4eee90ef
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The load and recency controls of the table edit form: the cache lifetime and its per-role list,
// paging, replication and load protection with its off-peak window.

import { NEVER_EXPIRES, cacheTtlHint } from "../../components/form/cacheTtl";
import { Text, NumberInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable, Source } from "../../types/admin";
import type { Role } from "../../types/auth";
import { ReplicateSelect } from "../../components/admin/ReplicateSelect";
import { FormField, FormLayout, LabelWithHelp } from "../../components/form/FormLayout";
import { PagingField } from "./PagingField";
import { ReplicaBuildLine } from "./ReplicaBuildLine";
import { RoleTtlField } from "./RoleTtlField";
import type { CacheTtlEdit } from "./tableEditTypes";
import { OffPeakWindowFields } from "../../components/admin/OffPeakWindowFields";
import { useSourceKind } from "../sources/sourceKind";

export function TableLoadManagementFields({
  editingTable,
  setEditingTable,
  editSource,
  roles,
  cacheTtlEdits,
  setCacheTtlEdits,
  floorTtl,
  ttlSignalError,
  effLoadProtected,
}: {
  editingTable: RegisteredTable;
  setEditingTable: (table: RegisteredTable) => void;
  editSource: Source | undefined;
  roles: Role[];
  cacheTtlEdits: Record<number, CacheTtlEdit>;
  setCacheTtlEdits: React.Dispatch<React.SetStateAction<Record<number, CacheTtlEdit>>>;
  floorTtl: number | null;
  ttlSignalError: boolean;
  effLoadProtected: boolean;
}) {
  const { t } = useTranslation();
  // REQ-2000: a live-only kind's tables are read live, never replicated.
  const liveOnly = useSourceKind(editSource?.type ?? "")?.live_only === true;
  return (
    <FormField size="full">
      {/* The operator's load and recency controls, on the table's Load and timeliness tab: cache
          TTL (the floor), the per-role TTL list (REQ-1907), materialization and load protection
          (REQ-826/1141). Shown in full; the tab is their section. */}
      <div data-testid="load-management-panel">
        <Text size="xs" c="dimmed" component="div" data-testid="load-management-help">
          <LabelWithHelp
            label={t("tableEditForm.loadManagementHelp")}
            help={t("tableEditForm.loadManagementInfo")}
          />
        </Text>
        <FormLayout>
          {/* The three settings share a line; what each one lists or reports follows beneath. */}
          <FormField
            hint={cacheTtlHint(cacheTtlEdits[editingTable.id]?.value ?? editingTable.cacheTtl, t)}
            help={t("tableEditForm.cacheTtlHelp")}
            size="short"
          >
            <NumberInput
              min={NEVER_EXPIRES}
              label={t("tableEditForm.cacheTtlLabel")}
              value={
                cacheTtlEdits[editingTable.id]?.value ??
                (editingTable.cacheTtl != null ? editingTable.cacheTtl : "")
              }
              onChange={(v) =>
                setCacheTtlEdits((prev) => ({
                  ...prev,
                  [editingTable.id]: {
                    ...prev[editingTable.id],
                    value: v === "" ? "" : String(v),
                    dirty: true,
                  },
                }))
              }
              placeholder={t("tableEditForm.cacheTtlPlaceholder")}
              error={
                ttlSignalError
                  ? t("tableEditForm.cacheTtlRequiredForSignal", {
                      signal: editingTable.changeSignal ?? editSource?.changeSignal,
                    })
                  : undefined
              }
            />
          </FormField>
          {!liveOnly && (
            <FormField size="medium">
              <ReplicateSelect
                value={editingTable.replicate}
                onChange={(replicate) => setEditingTable({ ...editingTable, replicate })}
                scope="table"
                // REQ-1141: load protection is the list's last choice. Default inherits both settings
                // from the source; every other choice is this table's own.
                loadProtected={editingTable.loadProtected === true}
                onScheduledChange={(replicate, loadProtected) =>
                  setEditingTable({ ...editingTable, replicate, loadProtected })
                }
                testId="table-replicate-select"
              />
            </FormField>
          )}
          {/* A table stored as not read only on schedule while its source is: no choice of the
              drop-down makes that pair (it comes from config or the API), so the drop-down shows
              Default and this line says what is stored. Choosing Default stores Default. */}
          {editingTable.loadProtected === false &&
            editingTable.replicate === null &&
            editSource?.loadProtected === true && (
              <FormField size="full">
                <Text size="xs" c="dimmed" data-testid="table-not-scheduled-like-source">
                  {t("tableSections.notScheduledLikeSource")}
                </Text>
              </FormField>
            )}
          {!liveOnly && (
            <FormField size="full">
              <ReplicaBuildLine
                sourceId={editingTable.sourceId}
                schemaName={editingTable.schemaName}
                tableName={editingTable.tableName}
              />
            </FormField>
          )}
          <FormField size="full">
            <RoleTtlField
              rows={editingTable.roleTtl}
              onChange={(roleTtl) => setEditingTable({ ...editingTable, roleTtl })}
              roles={roles}
              floorTtl={floorTtl}
            />
          </FormField>
          {editingTable.pagingKind !== null && (
            <FormField size="full">
              <PagingField
                kind={editingTable.pagingKind}
                paging={editingTable.pagination}
                onChange={(pagination) => setEditingTable({ ...editingTable, pagination })}
                ceilingRows={editingTable.pagingCeilingRows}
              />
            </FormField>
          )}
          {effLoadProtected && (
            <FormField size="full">
              <div data-testid="mv-protection-panel">
                <Text fw={600} size="sm" mb={4}>
                  {t("tableEditForm.sourceProtectionPanel")}
                </Text>
                {/* Said where the table has not chosen it itself: the source is read only on
                    schedule and this table follows it. */}
                {editingTable.loadProtected == null && (
                  <Text size="xs" c="dimmed" data-testid="off-peak-inherited">
                    {t("tableEditForm.offPeakInherited")}
                  </Text>
                )}
                <FormLayout>
                  <OffPeakWindowFields
                    window={editingTable.offPeakWindow ?? ""}
                    tz={editingTable.offPeakTz ?? ""}
                    onWindowChange={(window) =>
                      setEditingTable({ ...editingTable, offPeakWindow: window || null })
                    }
                    onTzChange={(tz) => setEditingTable({ ...editingTable, offPeakTz: tz || null })}
                  />
                </FormLayout>
              </div>
            </FormField>
          )}
        </FormLayout>
      </div>
    </FormField>
  );
}
