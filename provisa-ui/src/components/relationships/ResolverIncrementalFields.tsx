// Copyright (c) 2026 Kenneth Stott
// Canary: d0cf23b4-a534-4f48-8e38-1d1c0724371a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2249

import { useTranslation } from "react-i18next";
import { Checkbox, NumberInput } from "@mantine/core";
import { useCodeResolverIncremental } from "../../hooks/useCodeResolverIncremental";
import { FormChecks, FormField } from "../form/FormLayout";
import { incrementalReason } from "./incrementalOption";
import type { RelForm } from "./relationship-types";

/**
 * A code resolver set's incremental setting: its landed matches kept by the resolver's @change
 * function, and how often they are rebuilt whole, which that requires. The server says whether
 * the resolver's source has such a function for this set; where it has none the setting cannot
 * be turned on, and its (?) gives the reason. Shown once the set's id and resolver are
 * given: the answer depends on both.
 */
export function ResolverIncrementalFields({
  form,
  setForm,
}: {
  form: RelForm;
  setForm: (f: RelForm) => void;
}) {
  const { t } = useTranslation();
  const option = useCodeResolverIncremental(form.resolver, form.id);
  if (option === null) return null;
  const reason = incrementalReason(option, { table: form.id, function: form.resolver });
  return (
    <>
      <FormChecks>
        {/* Where it cannot be turned on, its (?) says why in place of what it does. */}
        <FormField size="medium" help={reason ?? t("addRelationshipForm.incrementalHelp")}>
          <Checkbox
            label={t("addRelationshipForm.incrementalLabel")}
            checked={form.incremental}
            // A setting already on stays reachable, so it can be turned off.
            disabled={!option.available && !form.incremental}
            onChange={(e) => setForm({ ...form, incremental: e.currentTarget.checked })}
            data-testid="rel-form-incremental"
          />
        </FormField>
      </FormChecks>
      {form.incremental && (
        <FormField size="short" help={t("addRelationshipForm.rebuildEveryHelp")}>
          <NumberInput
            label={t("addRelationshipForm.rebuildEveryLabel")}
            required
            min={1}
            allowDecimal={false}
            value={form.rebuildEvery === "" ? "" : Number(form.rebuildEvery)}
            onChange={(v) => setForm({ ...form, rebuildEvery: v === "" ? "" : String(v) })}
            data-testid="rel-form-rebuild-every"
          />
        </FormField>
      )}
    </>
  );
}
