// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-962: create / edit / delete a named, versioned snapshot-boundary calendar — the picker source
// for an MV snapshot schedule. Validation is server-side; this stages the fields and enforces a
// no-usage guard on delete (surfaced from the mutation result).

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Trash2 } from "lucide-react";
import {
  Modal,
  TextInput,
  Select,
  MultiSelect,
  NumberInput,
  Button,
  Group,
  Stack,
  Alert,
} from "@mantine/core";
import { DateInput, DatePickerInput } from "@mantine/dates";
import { useCreateCalendar, useDeleteCalendar } from "../../hooks/useAdminQueries";
import type { CalendarSummary } from "../../hooks/useAdminQueries";
import { IANA_TIME_ZONES } from "./constants";

// Localized month names for the fiscal-anchor picker (value = 1..12), no per-month i18n keys needed.
const MONTHS = Array.from({ length: 12 }, (_, i) => ({
  value: String(i + 1),
  label: new Intl.DateTimeFormat(undefined, { month: "long" }).format(new Date(2000, i, 1)),
}));

export function CalendarCreateModal({
  opened,
  onClose,
  onCreated,
  onDeleted,
  initialBaseSystem = "gregorian",
  editCalendar = null,
}: {
  opened: boolean;
  onClose: () => void;
  onCreated?: (name: string) => void;
  onDeleted?: (name: string) => void;
  initialBaseSystem?: string; // preselect gregorian | fiscal | retail_445
  editCalendar?: CalendarSummary | null; // when set, the modal edits this calendar in place
}) {
  const { t } = useTranslation();
  const { createCalendar, loading } = useCreateCalendar();
  const { deleteCalendar, loading: deleting } = useDeleteCalendar();
  const isEdit = editCalendar != null;
  const ec = editCalendar;
  const WEEKDAYS = [
    { value: "0", label: t("tableEditForm.calMon") },
    { value: "1", label: t("tableEditForm.calTue") },
    { value: "2", label: t("tableEditForm.calWed") },
    { value: "3", label: t("tableEditForm.calThu") },
    { value: "4", label: t("tableEditForm.calFri") },
    { value: "5", label: t("tableEditForm.calSat") },
    { value: "6", label: t("tableEditForm.calSun") },
  ];
  const [name, setName] = useState(ec?.name ?? "");
  const [version, setVersion] = useState(ec?.version ?? "v1");
  const [baseSystem, setBaseSystem] = useState(ec?.baseSystem ?? initialBaseSystem);
  const [tz, setTz] = useState(ec?.tz ?? "UTC");
  const [weekStart, setWeekStart] = useState(String(ec?.weekStart ?? 0));
  const [fiscalAnchorMonth, setFiscalAnchorMonth] = useState(String(ec?.fiscalAnchorMonth ?? 1));
  const [fiscalAnchorDay, setFiscalAnchorDay] = useState<number>(ec?.fiscalAnchorDay ?? 1);
  const [retailAnchor, setRetailAnchor] = useState<string | null>(ec?.retailAnchor ?? null);
  const [holidays, setHolidays] = useState<string[]>(ec?.holidays ?? []);
  const [weekend, setWeekend] = useState<string[]>((ec?.weekend ?? [5, 6]).map(String));
  const [error, setError] = useState<string | null>(null);

  // A retail_445 calendar is unusable without a reference year start; block save until it's set.
  const retailAnchorMissing = baseSystem === "retail_445" && !retailAnchor;

  const submit = async () => {
    setError(null);
    const res = await createCalendar({
      variables: {
        input: {
          name,
          version,
          baseSystem,
          tz,
          weekStart: parseInt(weekStart, 10),
          // REQ-962: fiscal → the year's anchor month/day; retail_445 → the reference-year start date.
          fiscalAnchorMonth: parseInt(fiscalAnchorMonth, 10),
          fiscalAnchorDay,
          retailAnchor: baseSystem === "retail_445" ? retailAnchor : null,
          holidays,
          weekend: weekend.map((w) => parseInt(w, 10)),
        },
      },
    });
    const result = (res.data as { createCalendar?: { success: boolean; message: string } } | null)
      ?.createCalendar;
    if (result && !result.success) {
      setError(result.message);
      return;
    }
    onCreated?.(name);
    onClose();
  };

  const remove = async () => {
    setError(null);
    const res = await deleteCalendar({ variables: { name } });
    const result = (res.data as { deleteCalendar?: { success: boolean; message: string } } | null)
      ?.deleteCalendar;
    if (result && !result.success) {
      setError(result.message); // e.g. "in use by N materialized view(s)"
      return;
    }
    onDeleted?.(name);
    onClose();
  };

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title={isEdit ? t("tableEditForm.calEditTitle") : t("tableEditForm.calModalTitle")}
      centered
    >
      <Stack gap="sm">
        {error && (
          <Alert color="red" data-testid="calendar-create-error">
            {error}
          </Alert>
        )}
        <TextInput
          label={t("tableEditForm.calNameLabel")}
          required
          data-testid="calendar-name"
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
          placeholder="fiscal-us"
          readOnly={isEdit} // the name is the identity; edit revises the definition in place
        />
        <TextInput
          label={t("tableEditForm.calVersionLabel")}
          required
          data-testid="calendar-version"
          value={version}
          onChange={(e) => setVersion(e.currentTarget.value)}
        />
        <Select
          label={t("tableEditForm.calBaseSystemLabel")}
          data-testid="calendar-base-system"
          data={[
            { value: "gregorian", label: t("tableEditForm.calBaseGregorian") },
            { value: "fiscal", label: t("tableEditForm.calBaseFiscal") },
            { value: "retail_445", label: t("tableEditForm.calBaseRetail") },
          ]}
          value={baseSystem}
          onChange={(v) => setBaseSystem(v || "gregorian")}
          comboboxProps={{ withinPortal: true }}
          allowDeselect={false}
        />
        {baseSystem === "fiscal" && (
          <Group grow align="flex-start" gap="xs">
            <Select
              label={t("tableEditForm.calFiscalAnchorMonthLabel")}
              data-testid="calendar-fiscal-month"
              data={MONTHS}
              value={fiscalAnchorMonth}
              onChange={(v) => setFiscalAnchorMonth(v || "1")}
              comboboxProps={{ withinPortal: true }}
              allowDeselect={false}
              description={t("tableEditForm.calFiscalAnchorHelp")}
            />
            <NumberInput
              label={t("tableEditForm.calFiscalAnchorDayLabel")}
              data-testid="calendar-fiscal-day"
              min={1}
              max={31}
              value={fiscalAnchorDay}
              onChange={(v) => setFiscalAnchorDay(typeof v === "number" ? v : 1)}
            />
          </Group>
        )}
        {baseSystem === "retail_445" && (
          <DateInput
            label={t("tableEditForm.calRetailAnchorLabel")}
            description={t("tableEditForm.calRetailAnchorHelp")}
            data-testid="calendar-retail-anchor"
            valueFormat="YYYY-MM-DD"
            value={retailAnchor}
            onChange={setRetailAnchor}
            popoverProps={{ withinPortal: true }}
            required
          />
        )}
        <Select
          label={t("tableEditForm.calTzLabel")}
          searchable
          data-testid="calendar-tz"
          data={IANA_TIME_ZONES}
          value={tz}
          onChange={(v) => setTz(v || "UTC")}
          comboboxProps={{ withinPortal: true }}
        />
        <Select
          label={t("tableEditForm.calWeekStartLabel")}
          data-testid="calendar-week-start"
          data={WEEKDAYS}
          value={weekStart}
          onChange={(v) => setWeekStart(v || "0")}
          comboboxProps={{ withinPortal: true }}
          allowDeselect={false}
        />
        <MultiSelect
          label={t("tableEditForm.calWeekendLabel")}
          description={t("tableEditForm.calWeekendHelp")}
          data-testid="calendar-weekend"
          data={WEEKDAYS}
          value={weekend}
          onChange={setWeekend}
          comboboxProps={{ withinPortal: true }}
          clearable
        />
        <DatePickerInput
          type="multiple"
          label={t("tableEditForm.calHolidaysLabel")}
          description={t("tableEditForm.calHolidaysHelp")}
          data-testid="calendar-holidays"
          valueFormat="YYYY-MM-DD"
          value={holidays}
          onChange={setHolidays}
          popoverProps={{ withinPortal: true }}
          clearable
        />
        <Group justify="space-between">
          {isEdit ? (
            <Button
              variant="light"
              color="red"
              leftSection={<Trash2 size={16} />}
              data-testid="calendar-delete"
              loading={deleting}
              onClick={remove}
            >
              {t("common.delete", "Delete")}
            </Button>
          ) : (
            <span />
          )}
          <Group gap="xs">
            <Button variant="default" onClick={onClose}>
              {t("tableEditForm.calCancel")}
            </Button>
            <Button
              data-testid="calendar-create-submit"
              loading={loading}
              disabled={!name || !version || retailAnchorMissing}
              onClick={submit}
            >
              {isEdit ? t("tableEditForm.calSave") : t("tableEditForm.calCreate")}
            </Button>
          </Group>
        </Group>
      </Stack>
    </Modal>
  );
}
