// Copyright (c) 2026 Kenneth Stott
// Canary: 6356bf2b-53e3-442d-ab0e-c7b9fcbba469
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The "Region" drop-down (REQ-1921): one of the org's regions, or No region. Shown only when the
// platform declares regions. On a table or a view it says where the data lives; on a source it is
// only the region the form starts that source's new tables in.

import { useTranslation } from "react-i18next";
import { Select } from "@mantine/core";
import { FieldLabel } from "../../pages/tables/FieldLabel";

/** The option value standing for "no region" (a region id is a short lowercase name). */
const NO_REGION = "__none__";

export function RegionSelect({
  value,
  onChange,
  regions,
  scope,
  testId,
}: {
  /** The region chosen: null = no region. */
  value: string | null;
  onChange: (value: string | null) => void;
  /** The org's regions; none = the platform declares none, and nothing is shown. */
  regions: string[];
  /** Which form this is in. */
  scope: "table" | "view" | "source";
  testId: string;
}) {
  const { t } = useTranslation();
  if (regions.length === 0) return null;
  const help = { table: "helpTable", view: "helpView", source: "helpSource" }[scope];
  return (
    <Select
      label={<FieldLabel text={t("regionSelect.label")} help={t(`regionSelect.${help}`)} />}
      data={[
        ...regions.map((region) => ({ value: region, label: region })),
        { value: NO_REGION, label: t("regionSelect.none") },
      ]}
      value={value ?? NO_REGION}
      onChange={(option) => {
        if (option !== null) onChange(option === NO_REGION ? null : option);
      }}
      comboboxProps={{ withinPortal: true }}
      allowDeselect={false}
      data-testid={testId}
    />
  );
}
