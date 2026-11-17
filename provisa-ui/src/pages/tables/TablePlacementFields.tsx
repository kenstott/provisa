// Copyright (c) 2026 Kenneth Stott
// Canary: 499a62df-150f-4f46-8b6a-d0795b755b6b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: where a table's (or view's) data lives -- its region, saved on its own
// (setTableRegion).

import { RegionSelect } from "../../components/admin/RegionSelect";
import { useRegionChoices } from "../../hooks/useRegionQueries";
import type { RegisteredTable } from "../../types/admin";
import { FormField } from "../../components/form/FormLayout";

type PlacementProps = {
  table: RegisteredTable;
  onChange: (table: RegisteredTable) => void;
};

/** The table's region: a field among the form's fields. */
export function TableRegionField({
  table,
  isView,
  onChange,
}: PlacementProps & { isView: boolean }) {
  const regionChoices = useRegionChoices();
  return (
    <FormField size="medium">
      <RegionSelect
        value={table.region}
        onChange={(region) => onChange({ ...table, region })}
        regions={regionChoices.regions}
        scope={isView ? "view" : "table"}
        testId="table-region-select"
      />
    </FormField>
  );
}
