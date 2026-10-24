// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634 STATUS: a data product's status, as its list and its panel show it.

import { useTranslation } from "react-i18next";
import { StatusBadge, type LifecycleStatus } from "../../components/lifecycle/StatusBadge";

export function ProductStatusBadge({
  status,
  testId,
}: {
  status: LifecycleStatus;
  testId?: string;
}) {
  const { t } = useTranslation();
  return (
    <StatusBadge status={status} label={t(`dataProductsTab.status_${status}`)} testId={testId} />
  );
}
