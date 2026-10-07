// Copyright (c) 2026 Kenneth Stott
// Canary: 40310425-57ba-49f2-abef-822f153f81af
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The column mask types the table editor offers. A fake is not one: it is the column's own
// declaration, read in a Test (fake) environment by every role (REQ-1942).
export function maskOptions(t: (key: string) => string) {
  return [
    { value: "", label: t("tableEditForm.maskNone") },
    { value: "regex", label: t("tableEditForm.maskRegex") },
    { value: "constant", label: t("tableEditForm.maskConstant") },
    { value: "truncate", label: t("tableEditForm.maskTruncate") },
  ];
}
