// Copyright (c) 2026 Kenneth Stott
// Canary: 9b1fa1af-49d7-4255-ad08-cc2acf37d177
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import type { ResultRedirect } from "../types/resultRedirect";

/** The same, as text, for a surface that cannot show a link (a sample it reads, an error line). */
export function resultRedirectText(
  t: TFunction,
  language: string,
  redirect: ResultRedirect,
): string {
  return t("resultRedirect.landedAt", {
    count: redirect.rowCount,
    rows: redirect.rowCount.toLocaleString(language),
    url: redirect.url,
  });
}
