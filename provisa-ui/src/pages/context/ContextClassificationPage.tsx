// Copyright (c) 2026 Kenneth Stott
// Canary: 06c86f89-820b-46d2-b78e-9f667ba9fd99
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { ClassificationKindsTab } from "../../components/admin/ClassificationKindsTab";

/** REQ-2259: the Classification page of the Context group -- the kinds and where each was
 * detected, read-only. Kinds are switched in Admin; decisions are made in the table editor. ClassificationKindsTab draws
 * the page's header and list. */
export function ContextClassificationPage() {
  const { t } = useTranslation();
  return (
    <div className="page page-sticky-head">
      <ClassificationKindsTab readOnly title={t("navBar.itemClassificationKinds")} />
    </div>
  );
}
