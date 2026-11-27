// Copyright (c) 2026 Kenneth Stott
// Canary: 2a8f6c13-7e49-4d05-9b3e-5c1d8f7a2e60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2278: the column rules of a tag or a classification kind, in its detail panel. A rule names a
// role and hides, or masks, every column carrying the key -- a table's or a command's output
// column -- without naming the columns. A column's own grant and masks still apply; the stricter
// result holds. ``readOnly`` lists the rules with no control that writes (the Context pages).

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ActionIcon,
  Alert,
  Button,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  deleteColumnRule,
  fetchColumnRules,
  saveColumnRule,
  type ColumnRule,
  type ColumnRuleAction,
  type ColumnRuleKey,
} from "../../api/columnRules";
import { useRoles } from "../../hooks/useAdminQueries";
import { grantLabel } from "../../lib/grants";
import { maskSpecLabel, maskValueOf, type MaskSpec } from "../../lib/masks";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";

/** The mask a rule reads its columns through: completely masked, a constant, or a regex. */
type MaskChoice = "complete" | "constant" | "regex";

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function ColumnRules({
  tagId,
  classification,
  readOnly = false,
  testId,
}: {
  /** The tag the rules are keyed on; or ``classification``, exactly one. */
  tagId?: string;
  classification?: string;
  readOnly?: boolean;
  testId: string;
}) {
  const { t } = useTranslation();
  const { roles } = useRoles();
  const [rules, setRules] = useState<ColumnRule[] | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  const [roleId, setRoleId] = useState<string | null>(null);
  const [action, setAction] = useState<ColumnRuleAction>("hide");
  const [maskChoice, setMaskChoice] = useState<MaskChoice>("complete");
  const [constant, setConstant] = useState("");
  const [pattern, setPattern] = useState("");
  const [replace, setReplace] = useState("");
  const [saving, setSaving] = useState(false);
  const ruleKey = useMemo<ColumnRuleKey>(() => {
    if (tagId !== undefined) return { tagId };
    if (classification !== undefined) return { classification };
    throw new Error("ColumnRules is keyed on a tag or a classification");
  }, [tagId, classification]);

  const load = useCallback(() => {
    fetchColumnRules(ruleKey)
      .then((r) => {
        setRules(r.rules);
        setProblem(null);
      })
      .catch((e: unknown) => setProblem(message(e)));
  }, [ruleKey]);

  // Each key's panel mounts its own list (the panels key it by the tag or the kind).
  useEffect(() => {
    load();
  }, [load]);

  const effect = (rule: ColumnRule) =>
    rule.action === "hide"
      ? t("columnRules.hidden")
      : rule.mask === null
        ? t("columnRules.maskedCompletely")
        : t("columnRules.maskedAs", { mask: maskSpecLabel(rule.mask, t) });

  const mask = (): MaskSpec | null => {
    if (maskChoice === "constant") return { type: "constant", value: maskValueOf(constant) };
    if (maskChoice === "regex") return { type: "regex", pattern, replace };
    return null;
  };

  const add = async () => {
    if (roleId === null) return;
    setSaving(true);
    try {
      await saveColumnRule(ruleKey, roleId, action, action === "mask" ? mask() : null);
      setRoleId(null);
      load();
    } catch (e: unknown) {
      setProblem(message(e));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (rule: ColumnRule) => {
    try {
      await deleteColumnRule(rule.id);
      load();
    } catch (e: unknown) {
      setProblem(message(e));
    }
  };

  const regexIncomplete = action === "mask" && maskChoice === "regex" && pattern.trim() === "";

  return (
    <Stack gap="xs" data-testid={testId}>
      <Title order={5}>{t("columnRules.title")}</Title>
      <Text size="xs" c="dimmed">
        {t("columnRules.hint")}
      </Text>
      {problem !== null && (
        <Alert color="red" data-testid={`${testId}-problem`}>
          {problem}
        </Alert>
      )}
      {rules !== null && rules.length === 0 && (
        <Text size="sm" c="dimmed" data-testid={`${testId}-empty`}>
          {t("columnRules.none")}
        </Text>
      )}
      {rules !== null && rules.length > 0 && (
        <Table data-testid={`${testId}-list`}>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("columnRules.colRole")}</Table.Th>
              <Table.Th>{t("columnRules.colEffect")}</Table.Th>
              {!readOnly && <Table.Th />}
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rules.map((rule) => (
              <Table.Tr key={rule.id} data-testid={`${testId}-rule-${rule.id}`}>
                <Table.Td>{grantLabel(rule.roleId, t)}</Table.Td>
                <Table.Td>{effect(rule)}</Table.Td>
                {!readOnly && (
                  <Table.Td>
                    <Tip text={t("columnRules.remove", { role: rule.roleId })}>
                      <ActionIcon
                        variant="subtle"
                        color="red"
                        aria-label={t("columnRules.remove", { role: rule.roleId })}
                        data-testid={`${testId}-remove-${rule.id}`}
                        onClick={() => void remove(rule)}
                      >
                        <Trash2 size={14} />
                      </ActionIcon>
                    </Tip>
                  </Table.Td>
                )}
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
      {!readOnly && (
        <FormLayout data-testid={`${testId}-form`}>
          <FormField size="medium">
            <Select
              label={t("columnRules.roleLabel")}
              data={roles.map((r) => ({ value: r.id, label: grantLabel(r.id, t) }))}
              value={roleId}
              onChange={setRoleId}
              comboboxProps={{ withinPortal: true }}
              data-testid={`${testId}-role`}
            />
          </FormField>
          <FormField size="short">
            <Select
              label={t("columnRules.actionLabel")}
              data={[
                { value: "hide", label: t("columnRules.actionHide") },
                { value: "mask", label: t("columnRules.actionMask") },
              ]}
              value={action}
              onChange={(v) => v !== null && setAction(v as ColumnRuleAction)}
              allowDeselect={false}
              comboboxProps={{ withinPortal: true }}
              data-testid={`${testId}-action`}
            />
          </FormField>
          {action === "mask" && (
            <FormField size="medium">
              <Select
                label={t("columnRules.maskLabel")}
                data={[
                  { value: "complete", label: t("columnRules.maskComplete") },
                  { value: "constant", label: t("maskList.constant") },
                  { value: "regex", label: t("maskList.regex") },
                ]}
                value={maskChoice}
                onChange={(v) => v !== null && setMaskChoice(v as MaskChoice)}
                allowDeselect={false}
                comboboxProps={{ withinPortal: true }}
                data-testid={`${testId}-mask`}
              />
            </FormField>
          )}
          {action === "mask" && maskChoice === "constant" && (
            <FormField size="medium">
              <TextInput
                label={t("columnRules.constantLabel")}
                value={constant}
                onChange={(e) => setConstant(e.currentTarget.value)}
                data-testid={`${testId}-constant`}
              />
            </FormField>
          )}
          {action === "mask" && maskChoice === "regex" && (
            <>
              <FormField size="medium">
                <TextInput
                  label={t("columnRules.patternLabel")}
                  value={pattern}
                  onChange={(e) => setPattern(e.currentTarget.value)}
                  data-testid={`${testId}-pattern`}
                />
              </FormField>
              <FormField size="medium">
                <TextInput
                  label={t("columnRules.replaceLabel")}
                  value={replace}
                  onChange={(e) => setReplace(e.currentTarget.value)}
                  data-testid={`${testId}-replace`}
                />
              </FormField>
            </>
          )}
          <FormFooter
            action={
              <Button
                disabled={roleId === null || saving || regexIncomplete}
                onClick={() => void add()}
                data-testid={`${testId}-add`}
              >
                {t("columnRules.add")}
              </Button>
            }
          />
        </FormLayout>
      )}
    </Stack>
  );
}
