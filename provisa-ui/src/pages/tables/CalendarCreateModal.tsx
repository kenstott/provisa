// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9d1f47-8c26-4a53-9e08-5f2c7d4b6a91
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-962: create a named, versioned snapshot-boundary calendar — the picker source for an MV
// snapshot schedule. Validation is server-side (base_system / tz / anchors); this stages the fields.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Modal, TextInput, Select, NumberInput, Button, Group, Stack, Alert } from "@mantine/core";
import { DateInput } from "@mantine/dates";
import { useCreateCalendar } from "../../hooks/useAdminQueries";
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
  initialBaseSystem = "gregorian",
}: {
  opened: boolean;
  onClose: () => void;
  onCreated?: (name: string) => void;
  initialBaseSystem?: string; // preselect gregorian | fiscal | retail_445
}) {
  const { t } = useTranslation();
  const { createCalendar, loading } = useCreateCalendar();
  const WEEKDAYS = [
    { value: "0", label: t("tableEditForm.calMon") },
    { value: "1", label: t("tableEditForm.calTue") },
    { value: "2", label: t("tableEditForm.calWed") },
    { value: "3", label: t("tableEditForm.calThu") },
    { value: "4", label: t("tableEditForm.calFri") },
    { value: "5", label: t("tableEditForm.calSat") },
    { value: "6", label: t("tableEditForm.calSun") },
  ];
  const [name, setName] = useState("");
  const [version, setVersion] = useState("v1");
  const [baseSystem, setBaseSystem] = useState(initialBaseSystem);
  const [tz, setTz] = useState("UTC");
  const [weekStart, setWeekStart] = useState("0");
  const [fiscalAnchorMonth, setFiscalAnchorMonth] = useState("1");
  const [fiscalAnchorDay, setFiscalAnchorDay] = useState<number>(1);
  const [retailAnchor, setRetailAnchor] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // A retail_445 calendar is unusable without a reference year start; block Create until it's set.
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

  return (
    <Modal opened={opened} onClose={onClose} title={t("tableEditForm.calModalTitle")} centered>
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
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose}>
            {t("tableEditForm.calCancel")}
          </Button>
          <Button
            data-testid="calendar-create-submit"
            loading={loading}
            disabled={!name || !version || retailAnchorMissing}
            onClick={submit}
          >
            {t("tableEditForm.calCreate")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
