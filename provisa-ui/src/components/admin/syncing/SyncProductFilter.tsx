// Copyright (c) 2026 Kenneth Stott
// Canary: 9a1f6c38-2e7d-4b50-8c94-d3b7e0a5f216
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: the data product filter above Context Sync's list of entries: every product a recorded
// publish carried, and the ones the address names.

import { useTranslation } from "react-i18next";
import { MultiSelect } from "../../MultiSelect";

export function SyncProductFilter({
  products,
  value,
  onChange,
}: {
  products: string[];
  value: string[];
  onChange: (ids: string[]) => void;
}) {
  const { t } = useTranslation();
  return (
    <div data-testid="syncing-product-filter" style={{ minWidth: 200, maxWidth: 360 }}>
      <MultiSelect
        options={products.map((id) => ({ id, label: id }))}
        value={value}
        onChange={onChange}
        ariaLabel={t("syncingTab.productFilterLabel")}
        placeholder={t("syncingTab.productFilterPlaceholder")}
      />
    </div>
  );
}
