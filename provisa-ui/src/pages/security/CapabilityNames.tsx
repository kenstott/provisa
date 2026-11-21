// Copyright (c) 2026 Kenneth Stott
// Canary: 1c6f83b2-40ea-4d97-8f25-b7d9e3a60c48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Capabilities named in a list: each with what it allows as its hint. data_residency is an
// attributed capability: its regions are its value, shown with it, as an attributed tag's is.

import { useTranslation } from "react-i18next";
import { Tip } from "../../components/Tip";
import { NO_REGION } from "../../components/admin/ResidencyGrant";

const RESIDENCY = "data_residency";

export function CapabilityNames({
  capabilities,
  regions = [],
}: {
  capabilities: readonly string[];
  regions?: readonly string[];
}) {
  const { t } = useTranslation();
  const region = (value: string) => (value === NO_REGION ? t("residencyGrant.noRegion") : value);
  return (
    <>
      {capabilities.map((capability, index) => (
        <span key={capability}>
          {index > 0 && ", "}
          <Tip text={t(`capabilityHelp.${capability}`)}>
            <span data-testid={`capability-name-${capability}`}>
              {capability === RESIDENCY && regions.length > 0
                ? `${capability}: ${regions.map(region).join(", ")}`
                : capability}
            </span>
          </Tip>
        </span>
      ))}
    </>
  );
}
