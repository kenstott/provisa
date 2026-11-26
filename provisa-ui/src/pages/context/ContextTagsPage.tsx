// Copyright (c) 2026 Kenneth Stott
// Canary: afb94239-581b-44d5-a909-2e10ba533ff0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { TagsTab } from "../../components/admin/TagsTab";

/** REQ-2259: the Tags page of the Context group -- each tag, where it is applied and what depends
 * on it, read-only. Tags are edited in Admin. TagsTab draws the page's header, tabs and list. */
export function ContextTagsPage() {
  const { t } = useTranslation();
  return (
    <div className="page page-sticky-head">
      <TagsTab readOnly title={t("navBar.itemTags")} />
    </div>
  );
}
