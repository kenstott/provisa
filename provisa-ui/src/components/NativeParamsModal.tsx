// Copyright (c) 2026 Kenneth Stott
// Canary: 23fa4a0d-8ab9-4ec8-b189-aee663bf22d9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Collects a table's native API params (required path params + optional query
// params) before a governed sample can run (Profile / Preview flows).

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Group, Modal, Stack, Text, TextInput } from "@mantine/core";
import type { RegisteredTable } from "../types/admin";
import {
  optionalParamColumns,
  requiredParamColumns,
  requiredParamsSatisfied,
} from "./nativeParams";

interface NativeParamsModalProps {
  table: RegisteredTable | null;
  onClose: () => void;
  onSubmit: (values: Record<string, string>) => void;
}

export function NativeParamsModal({ table, onClose, onSubmit }: NativeParamsModalProps) {
  const { t } = useTranslation();
  // Parent remounts this modal (key=table.id) per table, so state starts fresh.
  const [values, setValues] = useState<Record<string, string>>({});

  if (!table) return null;
  const requiredCols = requiredParamColumns(table);
  const optionalCols = optionalParamColumns(table);
  const canRun = requiredParamsSatisfied(table, values);

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("nativeParams.title", { name: table.alias || table.tableName })}
      data-testid="native-params-modal"
    >
      <Stack gap="sm">
        <Text size="xs" c="dimmed">
          {t("nativeParams.hint")}
        </Text>
        {[...requiredCols, ...optionalCols].map((c) => (
          <TextInput
            key={c.columnName}
            label={c.alias || c.columnName}
            required={c.nativeFilterType === "path_param"}
            value={values[c.columnName] ?? ""}
            onChange={(e) => {
              // React may run a functional updater on a later render pass, by which point the
              // synthetic event has been pooled and currentTarget is null. Read it here.
              const next = e.currentTarget.value;
              setValues((prev) => ({ ...prev, [c.columnName]: next }));
            }}
            data-testid={`native-param-${c.columnName}`}
          />
        ))}
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>
            {t("nativeParams.cancel")}
          </Button>
          <Button
            disabled={!canRun}
            onClick={() => onSubmit(values)}
            data-testid="native-params-run-btn"
          >
            {t("nativeParams.run")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
