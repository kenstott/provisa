// Copyright (c) 2026 Kenneth Stott
// Canary: 54cacfc8-11b1-4d9d-991e-ed7b9b2bd588
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-874: the incremental-reload (delta) declaration for a replicated table. A delta refreshes
// the whole-copy replica by applying only the rows past the stored watermark, instead of re-pulling
// the whole table. The cursor field IS the table's watermark column (set in Live Delivery); a delta
// is defined only for a SQL source (the server refuses it by name otherwise).

import {
  Alert,
  Checkbox,
  Fieldset,
  NumberInput,
  Select,
  Stack,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { Info } from "lucide-react";
import { useTranslation } from "react-i18next";
import type { DeltaConfig, RegisteredTable } from "../../types/admin";

interface DeltaFieldsetProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
}

const DEFAULT_DELTA: DeltaConfig = {
  query: null,
  apply: "upsert",
  deletes: "none",
  tombstoneColumn: null,
  rebuildEvery: null,
};

export function DeltaFieldset({ editingTable, setEditingTable }: DeltaFieldsetProps) {
  const { t } = useTranslation();
  const delta = editingTable.delta ?? null;
  const setDelta = (next: DeltaConfig | null) => setEditingTable({ ...editingTable, delta: next });
  const patch = (p: Partial<DeltaConfig>) => delta && setDelta({ ...delta, ...p });

  return (
    <Fieldset legend={t("tableEditForm.delta.legend")} data-testid="delta-fieldset">
      <Stack gap="xs">
        <Checkbox
          label={t("tableEditForm.delta.enable")}
          checked={delta !== null}
          onChange={(e) => setDelta(e.currentTarget.checked ? { ...DEFAULT_DELTA } : null)}
          data-testid="delta-enable"
        />
        {delta !== null && (
          <>
            {!editingTable.watermarkColumn && (
              <Alert icon={<Info size={16} />} color="yellow" data-testid="delta-no-watermark">
                {t("tableEditForm.delta.needsWatermark")}
              </Alert>
            )}
            <Text size="xs" c="dimmed">
              {t("tableEditForm.delta.cursorNote", {
                watermark: editingTable.watermarkColumn ?? "—",
              })}
            </Text>
            <Select
              label={t("tableEditForm.delta.apply")}
              description={t("tableEditForm.delta.applyHelp")}
              value={delta.apply}
              onChange={(v) => v && patch({ apply: v as DeltaConfig["apply"] })}
              data={[
                { value: "upsert", label: t("tableEditForm.delta.applyUpsert") },
                { value: "append", label: t("tableEditForm.delta.applyAppend") },
              ]}
              data-testid="delta-apply"
            />
            <Select
              label={t("tableEditForm.delta.deletes")}
              description={t("tableEditForm.delta.deletesHelp")}
              value={delta.deletes}
              onChange={(v) => v && patch({ deletes: v as DeltaConfig["deletes"] })}
              data={[
                { value: "none", label: t("tableEditForm.delta.deletesNone") },
                { value: "tombstone", label: t("tableEditForm.delta.deletesTombstone") },
              ]}
              data-testid="delta-deletes"
            />
            {delta.deletes === "tombstone" && (
              <TextInput
                label={t("tableEditForm.delta.tombstoneColumn")}
                value={delta.tombstoneColumn ?? ""}
                onChange={(e) => patch({ tombstoneColumn: e.target.value || null })}
                data-testid="delta-tombstone-column"
              />
            )}
            <NumberInput
              label={t("tableEditForm.delta.rebuildEvery")}
              description={t("tableEditForm.delta.rebuildEveryHelp")}
              value={delta.rebuildEvery ?? undefined}
              min={1}
              onChange={(v) => patch({ rebuildEvery: typeof v === "number" ? v : null })}
              data-testid="delta-rebuild-every"
            />
            <Textarea
              label={t("tableEditForm.delta.query")}
              description={t("tableEditForm.delta.queryHelp")}
              value={delta.query ?? ""}
              autosize
              minRows={2}
              onChange={(e) => patch({ query: e.target.value || null })}
              data-testid="delta-query"
            />
          </>
        )}
      </Stack>
    </Fieldset>
  );
}
