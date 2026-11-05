// Copyright (c) 2026 Kenneth Stott
// Canary: 92080ad1-fa0f-4c39-b92b-494b2cc31d19
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1174: a role's LLM budget -- the most one Polly turn or LLM service call may use, and the
// rolling windows its spend is held to, each in tokens or USD -- set on the role's form, and read
// on its page with what each window has spent and when room frees up.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Group, NumberInput, Select, Text } from "@mantine/core";
import { FormField } from "../../components/form/FormLayout";
import { DetailFields } from "../../components/detail/DetailFields";
import { fetchRoleLlmUsage, type LlmWindowUse } from "../../api/roleRights";
import { LLM_WINDOWS, type LlmLimit, type LlmWindow, type Role } from "../../types/auth";
import type { LlmBudgetForm, LlmLimitForm } from "../../lib/llmBudget";

function LimitField({
  label,
  testId,
  value,
  onChange,
}: {
  label: string;
  testId: string;
  value: LlmLimitForm;
  onChange: (value: LlmLimitForm) => void;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="medium">
      <Group gap="xs" align="flex-end" wrap="nowrap">
        <NumberInput
          label={label}
          placeholder={t("roleLlmBudget.noLimit")}
          min={0}
          decimalScale={value.unit === "usd" ? 4 : 0}
          style={{ flex: 1 }}
          data-testid={`${testId}-amount`}
          value={value.amount}
          onChange={(v) => onChange({ ...value, amount: typeof v === "number" ? v : "" })}
        />
        <Select
          aria-label={t("roleLlmBudget.unitOf", { limit: label })}
          w={110}
          allowDeselect={false}
          data-testid={`${testId}-unit`}
          data={[
            { value: "tokens", label: t("roleLlmBudget.unit_tokens") },
            { value: "usd", label: t("roleLlmBudget.unit_usd") },
          ]}
          value={value.unit}
          onChange={(u) => onChange({ ...value, unit: u as LlmLimit["unit"] })}
        />
      </Group>
    </FormField>
  );
}

/** The budget's fields on the role's form. Blank = no limit on that one. */
export function LlmBudgetFields({
  value,
  onChange,
}: {
  value: LlmBudgetForm;
  onChange: (value: LlmBudgetForm) => void;
}) {
  const { t } = useTranslation();
  return (
    <>
      <FormField size="full">
        <Text size="sm" fw={600}>
          {t("roleLlmBudget.heading")}
        </Text>
        <Text size="xs" c="dimmed">
          {t("roleLlmBudget.hint")}
        </Text>
      </FormField>
      <LimitField
        label={t("roleLlmBudget.callCap")}
        testId="role-llm-cap"
        value={value.cap}
        onChange={(cap) => onChange({ ...value, cap })}
      />
      {LLM_WINDOWS.map((w) => (
        <LimitField
          key={w}
          label={t(`roleLlmBudget.window_${w}`)}
          testId={`role-llm-${w}`}
          value={value.windows[w]}
          onChange={(limit) => onChange({ ...value, windows: { ...value.windows, [w]: limit } })}
        />
      ))}
    </>
  );
}

/** An amount in its unit: tokens whole, USD to four places. */
function useAmount() {
  const { t, i18n } = useTranslation();
  return (amount: number, unit: LlmLimit["unit"]) =>
    unit === "usd"
      ? new Intl.NumberFormat(i18n.language, {
          style: "currency",
          currency: "USD",
          maximumFractionDigits: 4,
        }).format(amount)
      : t("roleLlmBudget.tokens", {
          amount: new Intl.NumberFormat(i18n.language, { maximumFractionDigits: 0 }).format(amount),
        });
}

/** The budget on the role's page: each limit, and for each window what it has spent now and when
 *  its oldest spend leaves it. */
export function LlmBudgetRead({ role }: { role: Role }) {
  const { t, i18n } = useTranslation();
  const amount = useAmount();
  const rl = role.rateLimit;
  const [use, setUse] = useState<{ windows: LlmWindowUse[] | null; error: string | null }>({
    windows: null,
    error: null,
  });
  const hasWindows = LLM_WINDOWS.some((w) => rl?.llmWindows?.[w]);
  useEffect(() => {
    if (!hasWindows) return;
    let live = true;
    fetchRoleLlmUsage(role.id).then(
      (u) => live && setUse({ windows: u.windows, error: null }),
      (e: unknown) =>
        live && setUse({ windows: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, [role.id, hasWindows, rl]);
  const limit = (l: LlmLimit | null | undefined) => (l ? amount(l.amount, l.unit) : null);
  const at = (iso: string) =>
    new Intl.DateTimeFormat(i18n.language, { dateStyle: "medium", timeStyle: "short" }).format(
      new Date(iso),
    );
  const spent = (w: LlmWindow, l: LlmLimit) => {
    if (use.error) return `${limit(l)} · ${use.error}`;
    const u = use.windows?.find((x) => x.window === w);
    if (!u) return limit(l);
    const used = t("roleLlmBudget.used", {
      used: amount(u.used, u.unit),
      limit: amount(u.limit, u.unit),
    });
    return u.frees_at ? `${used} · ${t("roleLlmBudget.freesAt", { at: at(u.frees_at) })}` : used;
  };
  return (
    <>
      <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase" mb="xs">
        {t("roleLlmBudget.heading")}
      </Text>
      <DetailFields
        testId="role-read-llm-budget"
        fields={[
          {
            label: t("roleLlmBudget.callCap"),
            value: limit(rl?.llmCallCap),
            testId: `role-llm-cap-${role.id}`,
          },
          ...LLM_WINDOWS.map((w) => {
            const l = rl?.llmWindows?.[w];
            return {
              label: t(`roleLlmBudget.window_${w}`),
              value: l ? spent(w, l) : null,
              testId: `role-llm-${w}-${role.id}`,
            };
          }),
        ]}
      />
    </>
  );
}
