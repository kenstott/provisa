// Copyright (c) 2026 Kenneth Stott
// Canary: 56ddc17a-eefb-47e0-9ac1-4ec207f3cdc4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import type { Scan } from "../../api/termFinding";

/** What a running scan is doing, in words. */
export function useScanProgress() {
  const { t } = useTranslation();
  return (scan: Scan) =>
    scan.step === "merging"
      ? t("findTerms.progressMerging")
      : scan.step === "reading"
        ? t("findTerms.progressReading", { batch: scan.batch, batches: scan.batches })
        : t("findTerms.scanning");
}
