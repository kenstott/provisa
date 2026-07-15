// Copyright (c) 2026 Kenneth Stott
// Canary: b4e9c3a1-5f2d-4b8e-9c7a-6d0e1f2a3b4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Button, Group, Select, Stack, Table, Text, TextInput, Tooltip } from "@mantine/core";
import { Info, Plus, X } from "lucide-react";
import type { ColumnPreset } from "../../types/admin";

interface Props {
  presets: ColumnPreset[];
  columns: string[];
  columnTypes?: Record<string, string>;
  onChange: (presets: ColumnPreset[]) => void;
}

const TIMESTAMP_TYPES = new Set(["timestamp", "timestamp with time zone", "timestamp without time zone", "timestamptz", "datetime"]);
const DATE_TYPES = new Set(["date"]);
const TIME_TYPES = new Set(["time", "time with time zone", "time without time zone", "timetz"]);
const NUMERIC_TYPES = new Set(["integer", "int", "bigint", "smallint", "tinyint", "float", "double", "real", "decimal", "numeric", "double precision"]);
const BOOL_TYPES = new Set(["boolean", "bool"]);

function normalizeType(t: string): string {
  return t.toLowerCase().split("(")[0].trim();
}

function isTemporalType(t: string): boolean {
  const n = normalizeType(t);
  return TIMESTAMP_TYPES.has(n) || DATE_TYPES.has(n) || TIME_TYPES.has(n);
}

const EMPTY: ColumnPreset = { column: "", source: "now", name: null, value: null, dataType: null };

export function ColumnPresetsEditor({ presets, columns, columnTypes, onChange }: Props) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState<ColumnPreset>({ ...EMPTY });

  const SOURCES: { value: ColumnPreset["source"]; label: string }[] = [
    { value: "now", label: t("columnPresetsEditor.sourceNow") },
    { value: "header", label: t("columnPresetsEditor.sourceHeaderOption") },
    { value: "literal", label: t("columnPresetsEditor.sourceLiteral") },
  ];

  function getLiteralInput(colType: string | undefined, value: string | null, onChangeValue: (v: string | null) => void) {
    const n = colType ? normalizeType(colType) : null;
    if (n && NUMERIC_TYPES.has(n)) {
      return (
        <TextInput
          type="number"
          aria-label={t("columnPresetsEditor.numericValuePlaceholder")}
          placeholder={t("columnPresetsEditor.numericValuePlaceholder")}
          value={value ?? ""}
          onChange={(e) => onChangeValue(e.currentTarget.value || null)}
          data-testid="column-presets-literal-input"
        />
      );
    }
    if (n && TIMESTAMP_TYPES.has(n)) {
      return (
        <TextInput
          type="datetime-local"
          aria-label={t("columnPresetsEditor.valueHeader")}
          value={value ?? ""}
          onChange={(e) => onChangeValue(e.currentTarget.value || null)}
          data-testid="column-presets-literal-input"
        />
      );
    }
    if (n && DATE_TYPES.has(n)) {
      return (
        <TextInput
          type="date"
          aria-label={t("columnPresetsEditor.valueHeader")}
          value={value ?? ""}
          onChange={(e) => onChangeValue(e.currentTarget.value || null)}
          data-testid="column-presets-literal-input"
        />
      );
    }
    if (n && TIME_TYPES.has(n)) {
      return (
        <TextInput
          type="time"
          aria-label={t("columnPresetsEditor.valueHeader")}
          value={value ?? ""}
          onChange={(e) => onChangeValue(e.currentTarget.value || null)}
          data-testid="column-presets-literal-input"
        />
      );
    }
    if (n && BOOL_TYPES.has(n)) {
      return (
        <Select
          aria-label={t("columnPresetsEditor.valueHeader")}
          placeholder={t("columnPresetsEditor.selectPlaceholder")}
          value={value ?? null}
          onChange={(v) => onChangeValue(v)}
          data={[
            { value: "true", label: t("columnPresetsEditor.trueLabel") },
            { value: "false", label: t("columnPresetsEditor.falseLabel") },
          ]}
          data-testid="column-presets-literal-input"
        />
      );
    }
    return (
      <TextInput
        aria-label={t("columnPresetsEditor.literalValuePlaceholder")}
        placeholder={t("columnPresetsEditor.literalValuePlaceholder")}
        value={value ?? ""}
        onChange={(e) => onChangeValue(e.currentTarget.value || null)}
        data-testid="column-presets-literal-input"
      />
    );
  }

  const remove = (i: number) => {
    const next = presets.filter((_, idx) => idx !== i);
    onChange(next);
  };

  const add = () => {
    if (!draft.column) return;
    onChange([...presets, { ...draft }]);
    setDraft({ ...EMPTY });
  };

  return (
    <Stack gap="xs" data-testid="column-presets-editor">
      <Group gap={4} align="center">
        <Text size="sm" c="dimmed">
          {t("columnPresetsEditor.label")}
        </Text>
        <Tooltip label={t("columnPresetsEditor.infoTooltip")} multiline w={320}>
          <ActionIcon
            variant="subtle"
            color="gray"
            size="xs"
            aria-label={t("columnPresetsEditor.infoTooltip")}
          >
            <Info size={12} />
          </ActionIcon>
        </Tooltip>
      </Group>
      {presets.length > 0 && (
        <Table>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("columnPresetsEditor.columnHeader")}</Table.Th>
              <Table.Th>{t("columnPresetsEditor.sourceHeader")}</Table.Th>
              <Table.Th>{t("columnPresetsEditor.valueHeader")}</Table.Th>
              <Table.Th></Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {presets.map((p, i) => (
              <Table.Tr key={i}>
                <Table.Td>{p.column}</Table.Td>
                <Table.Td>{p.source}</Table.Td>
                <Table.Td>{p.source === "header" ? p.name : p.source === "literal" ? p.value : "—"}</Table.Td>
                <Table.Td>
                  <ActionIcon
                    variant="subtle"
                    color="gray"
                    aria-label={t("columnPresetsEditor.removeAction")}
                    onClick={() => remove(i)}
                    data-testid={`column-presets-remove-${i}`}
                  >
                    <X size={14} />
                  </ActionIcon>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
      <Group gap="sm" align="center" wrap="wrap">
        <Select
          aria-label={t("columnPresetsEditor.columnHeader")}
          placeholder={t("columnPresetsEditor.columnPlaceholder")}
          value={draft.column || null}
          onChange={(col) => {
            const colValue = col ?? "";
            const colType = columnTypes?.[colValue] ?? null;
            const temporal = colType ? isTemporalType(colType) : true;
            const nextSource = draft.source === "now" && !temporal ? "literal" : draft.source;
            setDraft((d) => ({ ...d, column: colValue, source: nextSource as ColumnPreset["source"], name: null, value: null, dataType: colType }));
          }}
          data={columns.map((c) => ({ value: c, label: c }))}
          data-testid="column-presets-column-select"
        />
        <Select
          aria-label={t("columnPresetsEditor.sourceHeader")}
          value={draft.source}
          onChange={(v) => setDraft((d) => ({ ...d, source: (v ?? "now") as ColumnPreset["source"], name: null, value: null }))}
          data={SOURCES.filter((s) => {
            if (s.value === "now" && draft.column && columnTypes?.[draft.column]) {
              return isTemporalType(columnTypes[draft.column]);
            }
            return true;
          }).map((s) => ({ value: s.value, label: s.label }))}
          data-testid="column-presets-source-select"
        />
        {draft.source === "header" && (
          <TextInput
            aria-label={t("columnPresetsEditor.headerNamePlaceholder")}
            placeholder={t("columnPresetsEditor.headerNamePlaceholder")}
            value={draft.name ?? ""}
            onChange={(e) => setDraft((d) => ({ ...d, name: e.currentTarget.value || null }))}
            data-testid="column-presets-header-name-input"
          />
        )}
        {draft.source === "literal" && getLiteralInput(
          draft.column ? columnTypes?.[draft.column] : undefined,
          draft.value,
          (v) => setDraft((d) => ({ ...d, value: v }))
        )}
        <Button
          onClick={add}
          disabled={!draft.column}
          leftSection={<Plus size={14} />}
          data-testid="column-presets-add-button"
        >
          {t("columnPresetsEditor.addAction")}
        </Button>
      </Group>
    </Stack>
  );
}
