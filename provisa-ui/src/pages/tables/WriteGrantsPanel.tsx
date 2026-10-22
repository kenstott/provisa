// Copyright (c) 2026 Kenneth Stott
// Canary: d0c37996-1126-4fce-9f81-5759eee902e7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2074: what each role may write to the table, per operation -- the table's Writes section.
// Read, it lists the grants; edited, they are saved together, apart from the table's other
// settings.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Button,
  Group,
  MultiSelect,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { Plus, X } from "lucide-react";
import type { MutationResult, WriteGrant, WriteOperation, WritePreset } from "../../types/admin";
import { FieldGroup, FormField, FormLayout } from "../../components/form/FormLayout";
import { serverMessage } from "../../i18n/serverMessage";
import { DetailFields } from "../../components/detail/DetailFields";

const OPERATIONS: WriteOperation[] = ["insert", "update", "delete"];
const SOURCES: WritePreset["source"][] = ["session", "literal", "now"];

interface Props {
  /** null: not shown to this caller (it does not hold view_governance). */
  grants: WriteGrant[] | null;
  columns: string[];
  roles: { id: string; label: string }[];
  /** Edited and saved through this; absent, the grants are only read. */
  onSave?: (grants: WriteGrant[]) => Promise<MutationResult>;
  /** REQ-2109: the table is being created: what onSave takes is kept and saved with the table. */
  kept: boolean;
}

function presetText(p: WritePreset, t: (k: string, o?: Record<string, unknown>) => string): string {
  if (p.source === "session")
    return t("writeGrants.presetFromSession", { column: p.column, name: p.name });
  if (p.source === "now") return t("writeGrants.presetNow", { column: p.column });
  return t("writeGrants.presetLiteral", { column: p.column, value: p.value ?? "" });
}

export function WriteGrantsPanel({ grants, columns, roles, onSave, kept }: Props) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState<WriteGrant[]>(grants ?? []);
  const [saving, setSaving] = useState(false);
  const [result, setResult] = useState<MutationResult | null>(null);
  // A grant list the page reloads (after a save) replaces the draft.
  const [shown, setShown] = useState(grants);
  if (grants !== shown) {
    setShown(grants);
    setDraft(grants ?? []);
  }

  if (grants === null)
    return (
      <Text size="sm" c="dimmed" data-testid="write-grants-not-shown">
        {t("writeGrants.notShown")}
      </Text>
    );

  const predicate = (value: string | null) =>
    value === null ? t("writeGrants.readFilter") : value;

  const heading = (g: WriteGrant) => `${g.role || "—"} · ${t(`writeGrants.op_${g.operation}`)}`;

  if (!onSave)
    return grants.length === 0 ? (
      <Text size="sm" c="dimmed" data-testid="write-grants-empty">
        {t("writeGrants.empty")}
      </Text>
    ) : (
      <Stack gap="md" data-testid="write-grants-read">
        {grants.map((g) => (
          <div key={`${g.role}:${g.operation}`} className="write-grant">
            <Text size="sm" fw={600} mb={4}>
              {heading(g)}
            </Text>
            <DetailFields
              fields={[
                ...(g.operation === "delete"
                  ? []
                  : [{ label: t("writeGrants.columns"), value: g.columns.join(", ") }]),
                ...(g.operation === "insert"
                  ? []
                  : [{ label: t("writeGrants.rows"), value: predicate(g.rows) }]),
                ...(g.operation === "delete"
                  ? []
                  : [
                      { label: t("writeGrants.check"), value: predicate(g.check) },
                      {
                        label: t("writeGrants.presets"),
                        value: g.presets.map((p) => presetText(p, t)).join("; "),
                      },
                    ]),
              ]}
            />
          </div>
        ))}
      </Stack>
    );

  const update = (i: number, patch: Partial<WriteGrant>) =>
    setDraft(draft.map((g, idx) => (idx === i ? { ...g, ...patch } : g)));
  const updatePreset = (i: number, j: number, patch: Partial<WritePreset>) =>
    update(i, { presets: draft[i].presets.map((p, idx) => (idx === j ? { ...p, ...patch } : p)) });
  const columnData = columns.map((c) => ({ value: c, label: c }));

  return (
    <div data-testid="write-grants-editor">
      <FormLayout>
        {draft.map((g, i) => (
          <FieldGroup key={i} className="write-grant" data-testid={`write-grant-${i}`}>
            <Text size="sm" fw={600}>
              {heading(g)}
            </Text>
            <FieldGroup row>
              <FormField size="medium">
                <Select
                  label={t("writeGrants.role")}
                  data={[
                    ...roles.map((r) => ({ value: r.id, label: r.label })),
                    // A grant names a role the list may not hold yet (it is still loading).
                    ...(g.role && !roles.some((r) => r.id === g.role)
                      ? [{ value: g.role, label: g.role }]
                      : []),
                  ]}
                  value={g.role}
                  onChange={(v) => update(i, { role: v ?? "" })}
                  searchable
                  data-testid={`write-grant-role-${i}`}
                />
              </FormField>
              <FormField size="short">
                <Select
                  label={t("writeGrants.operation")}
                  data={OPERATIONS.map((op) => ({ value: op, label: t(`writeGrants.op_${op}`) }))}
                  value={g.operation}
                  onChange={(v) => v && update(i, { operation: v as WriteOperation })}
                  allowDeselect={false}
                  data-testid={`write-grant-operation-${i}`}
                />
              </FormField>
              {g.operation !== "delete" && (
                <FormField size="long">
                  <MultiSelect
                    label={t("writeGrants.columns")}
                    data={columnData}
                    value={g.columns}
                    onChange={(cols) => update(i, { columns: cols })}
                    searchable
                    data-testid={`write-grant-columns-${i}`}
                  />
                </FormField>
              )}
              <ActionIcon
                variant="subtle"
                color="gray"
                aria-label={t("writeGrants.removeGrant")}
                onClick={() => setDraft(draft.filter((_, idx) => idx !== i))}
                data-testid={`write-grant-remove-${i}`}
              >
                <X size={16} />
              </ActionIcon>
            </FieldGroup>
            <FieldGroup row>
              {g.operation !== "insert" && (
                <FormField size="long" help={t("writeGrants.rowsHelp")}>
                  <TextInput
                    label={t("writeGrants.rows")}
                    placeholder={t("writeGrants.readFilter")}
                    value={g.rows ?? ""}
                    onChange={(e) => update(i, { rows: e.currentTarget.value || null })}
                    data-testid={`write-grant-rows-${i}`}
                  />
                </FormField>
              )}
              {g.operation !== "delete" && (
                <FormField size="long" help={t("writeGrants.checkHelp")}>
                  <TextInput
                    label={t("writeGrants.check")}
                    placeholder={t("writeGrants.readFilter")}
                    value={g.check ?? ""}
                    onChange={(e) => update(i, { check: e.currentTarget.value || null })}
                    data-testid={`write-grant-check-${i}`}
                  />
                </FormField>
              )}
            </FieldGroup>
            {g.operation !== "delete" &&
              g.presets.map((p, j) => (
                <FieldGroup row key={j} data-testid={`write-grant-preset-${i}-${j}`}>
                  <FormField size="medium">
                    <Select
                      label={t("writeGrants.presetColumn")}
                      data={columnData}
                      value={p.column}
                      onChange={(v) => updatePreset(i, j, { column: v ?? "" })}
                      searchable
                    />
                  </FormField>
                  <FormField size="medium">
                    <Select
                      label={t("writeGrants.presetSource")}
                      data={SOURCES.map((s) => ({ value: s, label: t(`writeGrants.source_${s}`) }))}
                      value={p.source}
                      onChange={(v) =>
                        v &&
                        updatePreset(i, j, {
                          source: v as WritePreset["source"],
                          name: null,
                          value: null,
                        })
                      }
                      allowDeselect={false}
                    />
                  </FormField>
                  {p.source === "session" && (
                    <FormField size="medium">
                      <TextInput
                        label={t("writeGrants.presetName")}
                        value={p.name ?? ""}
                        onChange={(e) => updatePreset(i, j, { name: e.currentTarget.value })}
                      />
                    </FormField>
                  )}
                  {p.source === "literal" && (
                    <FormField size="medium">
                      <TextInput
                        label={t("writeGrants.presetValue")}
                        value={p.value ?? ""}
                        onChange={(e) => updatePreset(i, j, { value: e.currentTarget.value })}
                      />
                    </FormField>
                  )}
                  <ActionIcon
                    variant="subtle"
                    color="gray"
                    aria-label={t("writeGrants.removePreset")}
                    onClick={() => update(i, { presets: g.presets.filter((_, idx) => idx !== j) })}
                  >
                    <X size={16} />
                  </ActionIcon>
                </FieldGroup>
              ))}
            {g.operation !== "delete" && (
              <FormField size="full">
                <Button
                  variant="subtle"
                  size="xs"
                  leftSection={<Plus size={14} />}
                  onClick={() =>
                    update(i, {
                      presets: [
                        ...g.presets,
                        { column: "", source: "session", name: "", value: null },
                      ],
                    })
                  }
                  data-testid={`write-grant-add-preset-${i}`}
                >
                  {t("writeGrants.addPreset")}
                </Button>
              </FormField>
            )}
          </FieldGroup>
        ))}
        <FormField size="full">
          <Group gap="xs">
            <Button
              variant="light"
              size="xs"
              leftSection={<Plus size={14} />}
              onClick={() =>
                setDraft([
                  ...draft,
                  {
                    role: "",
                    operation: "insert",
                    columns: [],
                    rows: null,
                    check: null,
                    presets: [],
                  },
                ])
              }
              data-testid="write-grant-add"
            >
              {t("writeGrants.addGrant")}
            </Button>
            <Button
              size="xs"
              loading={saving}
              onClick={async () => {
                setSaving(true);
                try {
                  setResult(await onSave(draft));
                } finally {
                  setSaving(false);
                }
              }}
              data-testid="write-grants-save"
            >
              {t("writeGrants.save")}
            </Button>
          </Group>
        </FormField>
        {result && (
          <FormField size="full">
            <Alert
              color={result.success ? "green" : "red"}
              data-testid={result.success ? "write-grants-saved" : "write-grants-refused"}
            >
              {result.success
                ? t(kept ? "writeGrants.kept" : "writeGrants.saved")
                : serverMessage(result, result.message)}
            </Alert>
          </FormField>
        )}
      </FormLayout>
    </div>
  );
}
