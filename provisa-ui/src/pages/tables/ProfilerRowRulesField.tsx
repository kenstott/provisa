// Copyright (c) 2026 Kenneth Stott
// Canary: d3b97787-354f-4b5b-9a29-89d21358fd5e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import type { ProfilerRowRule } from "../../api/profiler";
import { FormField } from "../../components/form/FormLayout";

/** REQ-1934: the row rule each role gets on a profiler's result table, set as it is registered. */
export function ProfilerRowRulesField({
  rules,
  onChange,
}: {
  rules: ProfilerRowRule[];
  onChange: (rules: ProfilerRowRule[]) => void;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="full">
      <fieldset data-testid="register-table-profiler-row-rules">
        <legend>{t("registerTableForm.profilerRowRulesLabel")}</legend>
        {rules.length === 0 && <span>{t("registerTableForm.profilerRowRulesNone")}</span>}
        {rules.map((rule, i) => (
          <label key={rule.roleId}>
            {rule.roleId}
            <input
              value={rule.filter}
              onChange={(e) =>
                onChange(rules.map((r, j) => (j === i ? { ...r, filter: e.target.value } : r)))
              }
              data-testid={`register-table-profiler-row-rule-${rule.roleId}`}
            />
            {/* REQ-2081: what the rule is for; saved with it. */}
            <input
              aria-label={t("securityPage.ruleDescription")}
              value={rule.description}
              onChange={(e) =>
                onChange(rules.map((r, j) => (j === i ? { ...r, description: e.target.value } : r)))
              }
              data-testid={`register-table-profiler-row-rule-description-${rule.roleId}`}
            />
          </label>
        ))}
      </fieldset>
    </FormField>
  );
}
