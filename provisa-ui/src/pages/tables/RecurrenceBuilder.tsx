// Copyright (c) 2026 Kenneth Stott
// Canary: fcc51f82-2792-4bac-bd08-639d3dd13618
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { Chip, Group, NumberInput, SegmentedControl, Select, Stack, Text } from "@mantine/core";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { RRule, Weekday } from "rrule";

// The snapshot-schedule GRAIN (REQ-962/1168/1169). Coarse presets are the fixed nesting grains the
// server tiles directly; "custom" opens an Outlook/iCalendar-style recurrence builder that emits an
// RFC 5545 RRULE string (parsed server-side by parse_grain_spec). The builder mirrors Outlook's
// recurrence dialog: Frequency > Every-N > (weekday chips | day-N-of-month | the-Nth-weekday).

const PRESETS = ["daily", "weekly", "monthly", "quarterly", "annual"] as const;
type Preset = (typeof PRESETS)[number];

const WEEKDAYS = [RRule.MO, RRule.TU, RRule.WE, RRule.TH, RRule.FR, RRule.SA, RRule.SU];
const WD_ABBR = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"];
const LEGACY_NTH = /^(L|[1-5])(MO|TU|WE|TH|FR|SA|SU)$/; // REQ-1168 shorthand, e.g. "3WE"/"LFR"

// The legacy "<ordinal><weekday>" shorthand upgrades to an equivalent RRULE for editing.
function legacyToRRule(spec: string): string | null {
  const m = LEGACY_NTH.exec(spec);
  if (!m) return null;
  const n = m[1] === "L" ? -1 : Number(m[1]);
  const wd = WEEKDAYS[WD_ABBR.indexOf(m[2])];
  return new RRule({ freq: RRule.MONTHLY, byweekday: [wd.nth(n)] }).toString();
}

function parseRule(spec: string): RRule | null {
  const upgraded = legacyToRRule(spec);
  try {
    return RRule.fromString(upgraded ?? spec.replace(/^RRULE:/i, ""));
  } catch {
    return null;
  }
}

export interface RecurrenceBuilderProps {
  value: string | null;
  onChange: (value: string | null) => void;
  placeholder: string;
  label: React.ReactNode;
  testId?: string;
}

// "monthly on day N" vs "monthly on the Nth weekday" — Outlook's two-mode monthly panel.
type MonthlyMode = "day" | "weekday";

export function RecurrenceBuilder({ value, onChange, placeholder, label, testId }: RecurrenceBuilderProps) {
  const { t } = useTranslation();

  const isPreset = value !== null && (PRESETS as readonly string[]).includes(value);
  const mode: Preset | "custom" | "" = value === null ? "" : isPreset ? (value as Preset) : "custom";
  const rule = useMemo(() => (mode === "custom" && value ? parseRule(value) : null), [mode, value]);
  const opts = rule?.origOptions;

  const summary = useMemo(() => {
    try {
      return rule ? rule.toText() : "";
    } catch {
      return "";
    }
  }, [rule]);

  // Emit a fresh RRULE string from a partial edit of the current custom rule's options.
  const emit = (patch: Partial<RRule["origOptions"]>) => {
    const base = opts ?? { freq: RRule.MONTHLY, interval: 1 };
    onChange(new RRule({ ...base, ...patch }).toString());
  };

  const freq = opts?.freq ?? RRule.MONTHLY;
  const byweekday = toArray(opts?.byweekday) as Weekday[];
  const bymonthday = firstNum(opts?.bymonthday);
  const monthlyMode: MonthlyMode = byweekday.some((w) => w.n) ? "weekday" : "day";

  return (
    <Stack gap="xs">
      <Select
        label={label}
        data-testid={testId}
        placeholder={placeholder}
        data={[
          { value: "daily", label: t("recurrence.daily") },
          { value: "weekly", label: t("recurrence.weekly") },
          { value: "monthly", label: t("recurrence.monthly") },
          { value: "quarterly", label: t("recurrence.quarterly") },
          { value: "annual", label: t("recurrence.annual") },
          { value: "custom", label: t("recurrence.custom") },
        ]}
        value={mode}
        onChange={(v) => {
          if (!v) return onChange(null);
          if (v === "custom") return emit({});
          onChange(v);
        }}
        comboboxProps={{ withinPortal: true }}
      />

      {mode === "custom" && (
        <Stack gap="xs" data-testid="recurrence-builder" pl="sm">
          <Group gap="xs" align="flex-end">
            <Select
              label={t("recurrence.frequency")}
              data-testid="recurrence-freq"
              w={140}
              data={[
                { value: String(RRule.DAILY), label: t("recurrence.freqDaily") },
                { value: String(RRule.WEEKLY), label: t("recurrence.freqWeekly") },
                { value: String(RRule.MONTHLY), label: t("recurrence.freqMonthly") },
                { value: String(RRule.YEARLY), label: t("recurrence.freqYearly") },
              ]}
              value={String(freq)}
              onChange={(v) => v && emit({ freq: Number(v), byweekday: undefined, bymonthday: undefined, bysetpos: undefined })}
              comboboxProps={{ withinPortal: true }}
            />
            <NumberInput
              label={t("recurrence.interval")}
              data-testid="recurrence-interval"
              w={120}
              min={1}
              value={opts?.interval ?? 1}
              onChange={(v) => emit({ interval: typeof v === "number" ? v : 1 })}
            />
            {(freq === RRule.MONTHLY || freq === RRule.YEARLY) && (
              <SegmentedControl
                size="xs"
                data-testid="recurrence-monthly-mode"
                value={monthlyMode}
                onChange={(m) =>
                  m === "day"
                    ? emit({ byweekday: undefined, bymonthday: 1 })
                    : emit({ bymonthday: undefined, byweekday: [RRule.MO.nth(1)] })
                }
                data={[
                  { value: "day", label: t("recurrence.onDay") },
                  { value: "weekday", label: t("recurrence.onThe") },
                ]}
              />
            )}
          </Group>

          {freq === RRule.WEEKLY && (
            <Chip.Group
              multiple
              value={byweekday.map((w) => WD_ABBR[w.weekday])}
              onChange={(vals) => emit({ byweekday: vals.map((a) => WEEKDAYS[WD_ABBR.indexOf(a)]) })}
            >
              <Group gap={4} data-testid="recurrence-weekdays">
                {WD_ABBR.map((a, i) => (
                  <Chip key={a} value={a} size="xs" variant="outline">
                    {t(`recurrence.wd${i}`)}
                  </Chip>
                ))}
              </Group>
            </Chip.Group>
          )}

          {(freq === RRule.MONTHLY || freq === RRule.YEARLY) && (
            <MonthlyPanel
              mode={monthlyMode}
              bymonthday={bymonthday}
              byweekday={byweekday}
              onDayChange={(n) => emit({ bymonthday: n, byweekday: undefined })}
              onWeekdayChange={(n, wd) => emit({ byweekday: [wd.nth(n)], bymonthday: undefined })}
            />
          )}

          {freq === RRule.YEARLY && (
            <Select
              label={t("recurrence.month")}
              data-testid="recurrence-month"
              w={160}
              data={Array.from({ length: 12 }, (_, i) => ({ value: String(i + 1), label: t(`recurrence.mo${i}`) }))}
              value={String(firstNum(opts?.bymonth) ?? 1)}
              onChange={(v) => v && emit({ bymonth: Number(v) })}
              comboboxProps={{ withinPortal: true }}
            />
          )}

          {summary && (
            <Text size="xs" c="dimmed" data-testid="recurrence-summary">
              {summary}
            </Text>
          )}
        </Stack>
      )}
    </Stack>
  );
}

const ORDINALS = [1, 2, 3, 4, -1];

function MonthlyPanel({
  mode,
  bymonthday,
  byweekday,
  onDayChange,
  onWeekdayChange,
}: {
  mode: MonthlyMode;
  bymonthday: number | undefined;
  byweekday: Weekday[];
  onDayChange: (n: number) => void;
  onWeekdayChange: (n: number, wd: Weekday) => void;
}) {
  const { t } = useTranslation();
  const nth = byweekday[0]?.n ?? 1;
  const wd = byweekday[0] ?? RRule.MO;

  // The day / nth-weekday inputs; the on-day vs on-the toggle lives up on the Frequency line.
  return mode === "day" ? (
        <NumberInput
          label={t("recurrence.dayOfMonth")}
          data-testid="recurrence-monthday"
          w={140}
          min={-1}
          max={31}
          value={bymonthday ?? 1}
          onChange={(v) => onDayChange(typeof v === "number" ? v : 1)}
        />
      ) : (
        <Group gap="xs" align="flex-end">
          <Select
            label={t("recurrence.ordinal")}
            data-testid="recurrence-ordinal"
            w={120}
            data={ORDINALS.map((n) => ({ value: String(n), label: t(`recurrence.ord${n}`) }))}
            value={String(nth)}
            onChange={(v) => v && onWeekdayChange(Number(v), new Weekday(wd.weekday))}
            comboboxProps={{ withinPortal: true }}
          />
          <Select
            label={t("recurrence.weekday")}
            data-testid="recurrence-weekday"
            w={140}
            data={WD_ABBR.map((_a, i) => ({ value: String(i), label: t(`recurrence.wdFull${i}`) }))}
            value={String(wd.weekday)}
            onChange={(v) => v !== null && onWeekdayChange(nth, new Weekday(Number(v)))}
            comboboxProps={{ withinPortal: true }}
          />
        </Group>
      );
}

function toArray<T>(v: T | T[] | null | undefined): T[] {
  if (v === null || v === undefined) return [];
  return Array.isArray(v) ? v : [v];
}

function firstNum(v: number | number[] | null | undefined): number | undefined {
  if (v === null || v === undefined) return undefined;
  return Array.isArray(v) ? v[0] : v;
}
