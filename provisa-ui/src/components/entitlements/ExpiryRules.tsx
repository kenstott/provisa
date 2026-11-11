// Copyright (c) 2026 Kenneth Stott
// Canary: 3774e274-c1ac-44d7-b229-be7f4970f96c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: a grant's end dates. Every expiry rule of the grant is listed with its date, what it
// ends and its reason: the ones still to come first, soonest first (the upcoming timeline), then
// the passed ones. A rule a list was set with is moved (extend) by setting the list again with
// the new date, the write API's own way of replacing the expiry a list was last set with.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Badge, Button, Stack, Table, Text, TextInput } from "@mantine/core";
import { DateInput } from "@mantine/dates";
import type { TFunction } from "i18next";
import {
  setList,
  type AccessView,
  type ExpiryRuleView,
  type PendingGrant,
} from "../../api/entitlements";
import { tomorrow, untilEndOf } from "../../lib/grantUntil";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

const EVERY = "*";

/** What `rule` ends, in words: the whole grant, or each capability and where. */
function describeEnds(t: TFunction, ends: Record<string, string[]>): string {
  const entries = Object.entries(ends);
  if (entries.length === 0) return t("entitlements.expiry.wholeGrant");
  return entries
    .map(([capability, identifiers]) => {
      const what =
        capability === EVERY
          ? t("entitlements.expiry.everyCapability")
          : t("entitlements.expiry.capability", { capability });
      return identifiers.includes(EVERY)
        ? t("entitlements.expiry.everywhere", { what })
        : t("entitlements.expiry.on", { what, entries: identifiers.join(", ") });
    })
    .join("; ");
}

/** The rules to come, soonest first, then the passed ones, latest first. */
function ordered(rules: ExpiryRuleView[]): ExpiryRuleView[] {
  const byDate = (a: ExpiryRuleView, b: ExpiryRuleView) => a.ends_at.localeCompare(b.ends_at);
  return [
    ...rules.filter((r) => !r.passed).sort(byDate),
    ...rules
      .filter((r) => r.passed)
      .sort(byDate)
      .reverse(),
  ];
}

const keyOf = (rule: ExpiryRuleView) => rule.rule_key ?? `${rule.list_kind}:${rule.ends_at}`;

export function ExpiryRules({
  principal,
  view,
  editable,
  onSaved,
  onError,
}: {
  principal: string;
  view: AccessView;
  /** False where every list comes from the identity provider: nothing here is offered. */
  editable: boolean;
  onSaved: (result: AccessView | PendingGrant) => void;
  onError: (e: unknown) => void;
}) {
  const { t } = useTranslation();
  const [extending, setExtending] = useState<string | null>(null);
  const [endsOn, setEndsOn] = useState<string | null>(null);
  const [reason, setReason] = useState("");

  // A rule a list was set with, still to come, on a list Provisa holds: setting the list again
  // with the new date moves it.
  const listOf = (rule: ExpiryRuleView) =>
    rule.list_kind === null || rule.passed
      ? undefined
      : view.lists.find((l) => l.kind === rule.list_kind && l.origin === "provisa");

  const open = (rule: ExpiryRuleView) => {
    setExtending(keyOf(rule));
    setEndsOn(null);
    setReason("");
  };

  const extend = async (rule: ExpiryRuleView) => {
    const list = listOf(rule);
    if (list === undefined || endsOn === null) return;
    try {
      const result = await setList({
        grantee: { principal },
        roleId: view.role_id,
        kind: list.kind,
        identifiers: list.identifiers,
        reason: reason.trim(),
        expiresAt: untilEndOf(endsOn),
      });
      setExtending(null);
      onSaved(result);
    } catch (e) {
      onError(e);
    }
  };

  const rules = ordered(view.expiries);
  return (
    <Stack gap="xs" data-testid="expiry-rules">
      <Text fw={500} size="sm">
        {t("entitlements.expiry.heading")}
      </Text>
      {rules.length === 0 ? (
        <Text size="sm" c="dimmed" data-testid="expiry-rules-none">
          {t("entitlements.expiry.none")}
        </Text>
      ) : (
        <Table withRowBorders data-testid="expiry-rules-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("entitlements.expiry.colEndsOn")}</Table.Th>
              <Table.Th>{t("entitlements.expiry.colEnds")}</Table.Th>
              <Table.Th>{t("entitlements.expiry.colReason")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rules.map((rule) => {
              const key = keyOf(rule);
              const extendable = editable && listOf(rule) !== undefined;
              return (
                <ExpiryRow
                  key={key}
                  testId={`expiry-rule-${key}`}
                  rule={rule}
                  ends={describeEnds(t, rule.ends)}
                  action={
                    extendable && extending !== key ? (
                      <Button
                        variant="default"
                        size="xs"
                        onClick={() => open(rule)}
                        data-testid={`expiry-rule-${key}-extend`}
                      >
                        {t("entitlements.expiry.extend")}
                      </Button>
                    ) : null
                  }
                />
              );
            })}
          </Table.Tbody>
        </Table>
      )}
      {rules
        .filter((rule) => extending === keyOf(rule) && listOf(rule) !== undefined)
        .map((rule) => (
          <FormLayout key={keyOf(rule)} data-testid="expiry-extend">
            <Text size="sm">
              {t("entitlements.expiry.extendPrompt", {
                kind: t(`entitlements.kind.${rule.list_kind}`),
                date: rule.ends_at.slice(0, 10),
              })}
            </Text>
            <FormField size="short" hint={t("entitlements.expiry.newDateHint")}>
              <DateInput
                label={t("entitlements.expiry.newDateLabel")}
                valueFormat="YYYY-MM-DD"
                minDate={tomorrow()}
                value={endsOn}
                onChange={setEndsOn}
                popoverProps={{ withinPortal: true }}
                data-testid="expiry-extend-date"
              />
            </FormField>
            <FormField size="long" hint={t("entitlements.form.reasonHint")}>
              <TextInput
                label={t("entitlements.form.reasonLabel")}
                required
                value={reason}
                onChange={(e) => setReason(e.currentTarget.value)}
                data-testid="expiry-extend-reason"
              />
            </FormField>
            <FormFooter
              onSave={() => extend(rule)}
              onCancel={() => setExtending(null)}
              saveProps={{
                disabled: endsOn === null || reason.trim() === "",
                "data-testid": "expiry-extend-save",
              }}
              cancelProps={{ "data-testid": "expiry-extend-cancel" }}
            />
          </FormLayout>
        ))}
    </Stack>
  );
}

function ExpiryRow({
  rule,
  ends,
  action,
  testId,
}: {
  rule: ExpiryRuleView;
  ends: string;
  action: React.ReactNode;
  testId: string;
}) {
  const { t } = useTranslation();
  const dim = rule.passed ? "dimmed" : undefined;
  return (
    <Table.Tr data-testid={testId} data-passed={rule.passed}>
      <Table.Td>
        <Text size="sm" c={dim} ff="monospace">
          {rule.ends_at.slice(0, 10)}
        </Text>
        {rule.passed && (
          <Badge size="xs" variant="light" color="gray">
            {t("entitlements.expiry.passed")}
          </Badge>
        )}
      </Table.Td>
      <Table.Td>
        <Text size="sm" c={dim}>
          {ends}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm" c={dim}>
          {rule.reason ?? t("entitlements.expiry.noReason")}
        </Text>
      </Table.Td>
      <Table.Td>{action}</Table.Td>
    </Table.Tr>
  );
}
