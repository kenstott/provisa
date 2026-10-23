// Copyright (c) 2026 Kenneth Stott
// Canary: b230250f-8593-4c78-a52f-b760014b9eb4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A column's ordered mask list, edited: numbered entries, each for the readers it names and the
// mask they read the column through (or none). A reader reads the column through the first entry
// it holds. As the list changes the server checks it: an entry no reader reaches is flagged with
// the entry before it that covers it, a list with no entry for everyone is flagged (readers it
// does not name read the column unmasked), and "Check access" shows the entry a reader acting as
// the chosen roles reads it through.

import { useEffect, useState, type CSSProperties } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Button,
  Group,
  Select,
  Stack,
  TagsInput,
  Text,
  TextInput,
} from "@mantine/core";
import { ArrowDown, ArrowUp, Trash2 } from "lucide-react";
import { GrantPicker } from "./GrantPicker";
import { RegexMaskFields } from "./RegexMaskFields";
import { checkMasks, maskPresets, type MaskPreset, type MasksReport } from "../api/admin";
import {
  EVERYONE,
  maskValueOf,
  moved,
  newMaskList,
  withNewEntry,
  type MaskEntry,
  type MaskKind,
  type MaskSpec,
} from "../lib/masks";

const CHECK_DELAY_MS = 300;

// One entry: its number, who it is for, its mask kind, the kind's fields, and its moves. The
// readers and the fields share what is left, so an entry fits the width it is given.
const ROW: CSSProperties = {
  display: "grid",
  gridTemplateColumns: "20px minmax(0, 1.2fr) 120px minmax(0, 1fr) auto",
  gap: 6,
  alignItems: "start",
};

export function MaskListEditor({
  value,
  onChange,
  roleOptions,
  disabled,
  testId = "mask-list",
}: {
  value: MaskEntry[];
  onChange: (next: MaskEntry[]) => void;
  roleOptions: { id: string; label: string }[];
  disabled?: boolean;
  testId?: string;
}) {
  const { t } = useTranslation();
  const [readers, setReaders] = useState<string[]>([]);
  const [report, setReport] = useState<MasksReport | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const [presets, setPresets] = useState<MaskPreset[]>([]);

  useEffect(() => {
    let gone = false;
    maskPresets()
      .then((p) => !gone && setPresets(p))
      .catch((e: unknown) => !gone && setProblem(e instanceof Error ? e.message : String(e)));
    return () => {
      gone = true;
    };
  }, []);

  useEffect(() => {
    // An empty list shows only its start button: nothing to check.
    if (value.length === 0) return;
    let stale = false;
    const timer = setTimeout(() => {
      checkMasks(value, readers.length > 0 ? readers : undefined)
        .then((r) => {
          if (stale) return;
          setReport(r);
          setProblem(null);
        })
        .catch((e: unknown) => {
          if (!stale) setProblem(e instanceof Error ? e.message : String(e));
        });
    }, CHECK_DELAY_MS);
    return () => {
      stale = true;
      clearTimeout(timer);
    };
  }, [value, readers]);

  if (value.length === 0) {
    return (
      <Button
        variant="light"
        size="xs"
        disabled={disabled}
        onClick={() => onChange(newMaskList())}
        data-testid={`${testId}-start`}
      >
        {t("maskList.start")}
      </Button>
    );
  }

  const options = [{ id: EVERYONE, label: t("maskList.everyone") }, ...roleOptions];
  const kinds = [
    { value: "", label: t("maskList.unmasked") },
    { value: "regex", label: t("maskList.regex") },
    { value: "constant", label: t("maskList.constant") },
    { value: "truncate", label: t("maskList.truncate") },
  ];
  const set = (i: number, entry: MaskEntry) => onChange(value.map((e, j) => (j === i ? entry : e)));
  const setMask = (i: number, patch: Partial<MaskSpec>) => {
    const mask = value[i].mask;
    if (mask !== null) set(i, { ...value[i], mask: { ...mask, ...patch } });
  };
  const unreachable = new Map((report?.unreachable ?? []).map((u) => [u.entry, u.covered_by]));

  return (
    <Stack gap={6} data-testid={testId}>
      {value.map((entry, i) => (
        <Stack
          key={i}
          gap={2}
          data-testid={`${testId}-entry-${i}`}
          data-applies={report?.applies === i ? "true" : undefined}
          style={
            report?.applies === i
              ? {
                  outline: "2px solid var(--mantine-primary-color-filled)",
                  borderRadius: 4,
                  padding: 4,
                }
              : { padding: 4 }
          }
        >
          <div style={ROW}>
            <Text size="sm" fw={600} mt={6}>
              {i + 1}.
            </Text>
            <GrantPicker
              grow
              options={options}
              value={entry.to}
              onChange={(to) => set(i, { ...entry, to })}
              ariaLabel={t("maskList.toAria", { n: i + 1 })}
              disabled={disabled}
            />
            <Select
              aria-label={t("maskList.kindAria", { n: i + 1 })}
              data={kinds}
              value={entry.mask?.type ?? ""}
              onChange={(v) =>
                set(i, {
                  ...entry,
                  mask: v
                    ? { type: v as MaskKind, ...(v === "constant" ? { value: null } : {}) }
                    : null,
                })
              }
              comboboxProps={{ withinPortal: true }}
              allowDeselect={false}
              disabled={disabled}
            />
            <Group gap={6} wrap="nowrap" grow style={{ minWidth: 0 }}>
              {entry.mask?.type === "regex" && (
                <RegexMaskFields
                  entry={i}
                  pattern={entry.mask.pattern ?? ""}
                  replace={entry.mask.replace ?? ""}
                  onChange={(patch) => setMask(i, patch)}
                  presets={presets}
                  verdicts={(report?.patterns ?? []).filter((v) => v.entry === i)}
                  checked={report !== null}
                  disabled={disabled}
                  testId={`${testId}-regex-${i}`}
                />
              )}
              {entry.mask?.type === "constant" && (
                <TextInput
                  aria-label={t("maskList.value")}
                  placeholder={t("maskList.valuePlaceholder")}
                  value={
                    entry.mask.value === null || entry.mask.value === undefined
                      ? ""
                      : String(entry.mask.value)
                  }
                  onChange={(e) => setMask(i, { value: maskValueOf(e.currentTarget.value) })}
                  disabled={disabled}
                />
              )}
              {entry.mask?.type === "truncate" && (
                <Select
                  aria-label={t("maskList.precision")}
                  placeholder={t("maskList.precision")}
                  data={[
                    { value: "year", label: t("maskList.year") },
                    { value: "month", label: t("maskList.month") },
                    { value: "day", label: t("maskList.day") },
                    { value: "hour", label: t("maskList.hour") },
                  ]}
                  value={entry.mask.precision ?? null}
                  onChange={(v) => setMask(i, { precision: v })}
                  comboboxProps={{ withinPortal: true }}
                  disabled={disabled}
                />
              )}
            </Group>
            <Group gap={0} wrap="nowrap">
              <ActionIcon
                variant="subtle"
                mt={4}
                aria-label={t("maskList.up", { n: i + 1 })}
                disabled={disabled || i === 0}
                onClick={() => onChange(moved(value, i, -1))}
              >
                <ArrowUp size={14} />
              </ActionIcon>
              <ActionIcon
                variant="subtle"
                mt={4}
                aria-label={t("maskList.down", { n: i + 1 })}
                disabled={disabled || i === value.length - 1}
                onClick={() => onChange(moved(value, i, 1))}
              >
                <ArrowDown size={14} />
              </ActionIcon>
              <ActionIcon
                variant="subtle"
                color="red"
                mt={4}
                aria-label={t("maskList.remove", { n: i + 1 })}
                disabled={disabled}
                onClick={() => onChange(value.filter((_, j) => j !== i))}
              >
                <Trash2 size={14} />
              </ActionIcon>
            </Group>
          </div>
          {unreachable.has(i) && (
            <Text size="xs" c="orange" ml={26} data-testid={`${testId}-unreachable-${i}`}>
              {unreachable.get(i) === null
                ? t("maskList.forNobody")
                : t("maskList.coveredBy", { n: (unreachable.get(i) ?? 0) + 1 })}
            </Text>
          )}
        </Stack>
      ))}
      <Group gap="xs">
        <Button
          variant="subtle"
          size="xs"
          disabled={disabled}
          onClick={() => onChange(withNewEntry(value))}
          data-testid={`${testId}-add`}
        >
          {t("maskList.add")}
        </Button>
      </Group>
      {report?.unlisted_unmasked && (
        <Text size="xs" c="orange" data-testid={`${testId}-unlisted`}>
          {t("maskList.unlistedUnmasked")}
        </Text>
      )}
      <Group gap="xs" align="flex-end" wrap="nowrap">
        <TagsInput
          label={t("maskList.checkLabel")}
          placeholder={t("maskList.checkPlaceholder")}
          data={roleOptions.map((o) => o.id)}
          value={readers}
          onChange={setReaders}
          size="xs"
          style={{ flex: 1, maxWidth: 360 }}
          data-testid={`${testId}-check`}
        />
        {readers.length > 0 && report !== null && (
          <Text size="xs" mb={6} data-testid={`${testId}-applies`}>
            {report.applies === null
              ? t("maskList.appliesNone")
              : t("maskList.applies", { n: report.applies + 1 })}
          </Text>
        )}
      </Group>
      {problem !== null && (
        <Text size="xs" c="red">
          {problem}
        </Text>
      )}
    </Stack>
  );
}
