// Copyright (c) 2026 Kenneth Stott
// Canary: 12e9fb96-200a-4c5a-9d14-6a959e884005
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: the data_residency right on a role — a checkbox, and the region values its grant
// covers (the org's regions and "No region"). Shown only when the platform declares regions: the
// right does not exist otherwise.

import { useTranslation } from "react-i18next";
import { Checkbox } from "@mantine/core";
import { FormChecks, FormField } from "../form/FormLayout";
import { MultiSelect } from "../MultiSelect";
import { Tip } from "../Tip";

/** The grant value that stands for "no region" (server: security/residency.NO_REGION). */
export const NO_REGION = "no_region";

export function ResidencyGrant({
  held,
  values,
  regions,
  parents,
  onHeld,
  onValues,
}: {
  held: boolean;
  values: string[];
  /** The org's regions; none = the platform declares none, and nothing is shown. */
  regions: string[];
  /** What the role's parents hold (pages/security/roleParents); null for a role with none. */
  parents: { capabilities: ReadonlySet<string>; regions: ReadonlySet<string> } | null;
  onHeld: (held: boolean) => void;
  onValues: (values: string[]) => void;
}) {
  const { t } = useTranslation();
  if (regions.length === 0) return null;
  const offered = parents === null || parents.capabilities.has("data_residency");
  const covered = (value: string) => parents === null || parents.regions.has(value);
  return (
    <>
      {/* The tick decides whether the field after it is shown, so it comes first. */}
      <FormChecks data-testid="residency-grant">
        <FormField size="short">
          <Tip
            text={offered ? t("capabilityHelp.data_residency") : t("capabilityHelp.notHeldByParent")}
          >
            <span>
              <Checkbox
                label={t("residencyGrant.right")}
                checked={held}
                disabled={!offered}
                onChange={(e) => onHeld(e.currentTarget.checked)}
                data-testid="residency-grant-checkbox"
                size="sm"
              />
            </span>
          </Tip>
        </FormField>
      </FormChecks>
      {held && (
        <FormField size="long">
          <MultiSelect
            label={t("residencyGrant.values")}
            placeholder={t("residencyGrant.valuesPlaceholder")}
            options={[
              ...regions.map((region) => ({ id: region, label: region })),
              { id: NO_REGION, label: t("residencyGrant.noRegion") },
            ].filter((option) => covered(option.id))}
            value={values}
            onChange={onValues}
          />
        </FormField>
      )}
    </>
  );
}
