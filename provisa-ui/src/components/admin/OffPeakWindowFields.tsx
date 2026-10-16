// Copyright (c) 2026 Kenneth Stott
// Canary: 8d23cce9-1114-41ed-b148-e767777c9a27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The off-peak window of a table or source read only on schedule (REQ-1141), shared by the table
// form and the source's Load Management panel: two time boxes (opens / closes) compose the stored
// "HH:MM-HH:MM", both blank is no window; the zone is picked from the runtime's own zone list, the
// identifiers the server's ZoneInfo accepts. Both are optional: with no window the refresh follows
// the Cache TTL or the change probe.

import { useTranslation } from "react-i18next";
import { Group, Input, Select } from "@mantine/core";
import { TimeInput } from "@mantine/dates";
import { FormField } from "../form/FormLayout";
import { IANA_TIME_ZONES } from "../../pages/tables/constants";

export function OffPeakWindowFields({
  window,
  tz,
  onWindowChange,
  onTzChange,
  windowHelp,
  windowHint,
}: {
  /** "HH:MM-HH:MM"; empty is no window. */
  window: string;
  /** An IANA zone; empty is none chosen. */
  tz: string;
  onWindowChange: (window: string) => void;
  onTzChange: (tz: string) => void;
  /** What the window means where it is shown, when not a source's loading: a materialized view's
   * is when it may be read (REQ-2066). */
  windowHelp?: string;
  /** The sentence beneath the window, likewise. */
  windowHint?: string;
}) {
  const { t } = useTranslation();
  const [opens = "", closes = ""] = window.split("-");
  const compose = (start: string, end: string) => (start || end ? `${start}-${end}` : "");
  return (
    <>
      {/* Two boxes under one label: the label, its (?) and the sentence beneath are the field's,
          as they are for a single box. */}
      <FormField
        size="medium"
        help={windowHelp ?? t("tableEditForm.offPeakWindowHelp")}
        hint={windowHint ?? t("sourceFormFieldsExtended.offPeakWindowHint")}
        data-testid="off-peak-window"
      >
        <Input.Wrapper label={t("tableEditForm.offPeakWindowLabel")}>
          <Group gap="xs" grow>
            <TimeInput
              aria-label={t("tableEditForm.offPeakOpensAria")}
              data-testid="off-peak-opens"
              label={t("tableEditForm.offPeakOpens")}
              value={opens}
              onChange={(e) => onWindowChange(compose(e.currentTarget.value, closes))}
            />
            <TimeInput
              aria-label={t("tableEditForm.offPeakClosesAria")}
              data-testid="off-peak-closes"
              label={t("tableEditForm.offPeakCloses")}
              value={closes}
              onChange={(e) => onWindowChange(compose(opens, e.currentTarget.value))}
            />
          </Group>
        </Input.Wrapper>
      </FormField>
      <FormField help={t("tableEditForm.offPeakTzHelp")} size="medium">
        <Select
          label={t("tableEditForm.offPeakTzLabel")}
          data={IANA_TIME_ZONES}
          value={tz}
          onChange={(v) => onTzChange(v ?? "")}
          searchable
          clearable
          placeholder="UTC"
          comboboxProps={{ withinPortal: true }}
          data-testid="off-peak-tz"
        />
      </FormField>
    </>
  );
}
