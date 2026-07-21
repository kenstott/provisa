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
import { Modal, TextInput, Select, Button, Group, Stack, Alert } from "@mantine/core";
import { useCreateCalendar } from "../../hooks/useAdminQueries";
import { IANA_TIME_ZONES } from "./constants";

export function CalendarCreateModal({
  opened,
  onClose,
  onCreated,
}: {
  opened: boolean;
  onClose: () => void;
  onCreated?: (name: string) => void;
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
  const [baseSystem, setBaseSystem] = useState("gregorian");
  const [tz, setTz] = useState("UTC");
  const [weekStart, setWeekStart] = useState("0");
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    setError(null);
    const res = await createCalendar({
      variables: {
        input: { name, version, baseSystem, tz, weekStart: parseInt(weekStart, 10) },
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
            disabled={!name || !version}
            onClick={submit}
          >
            {t("tableEditForm.calCreate")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
