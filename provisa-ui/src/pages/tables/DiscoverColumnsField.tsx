// Copyright (c) 2026 Kenneth Stott
// Canary: 89e817c7-b784-48cf-ac46-cffbb4c38c40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-252: columns read from the live source. On Register Table they are the table's columns
// when it is registered; in the table's editor its next save adds the columns the source has
// that the table lacks, as registration does (the server merges them: explicit columns win).

import { Checkbox } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";

export function DiscoverColumnsField({
  checked,
  onChange,
  help,
}: {
  checked: boolean;
  onChange: (discover: boolean) => void;
  /** What ticking it does where it is shown: at registration, or at the editor's next save. */
  help: string;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="short" help={help}>
      <Checkbox
        checked={checked}
        onChange={(e) => onChange(e.currentTarget.checked)}
        data-testid="discover-columns-checkbox"
        label={t("registerTableForm.discoverLabel")}
      />
    </FormField>
  );
}
