// Copyright (c) 2026 Kenneth Stott
// Canary: b4d29e61-0c87-4a3f-9e15-7f2ad83c6b09
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: an alignment read as a sentence in its direction, and the key its basis is worded by.

import type { Alignment, ImportItem } from "../../../api/ontologyImport";

/** The key a basis is worded by: its words joined by underscores. */
export function basisKey(basis: string): string {
  return basis.replace(/ /g, "_");
}

/** The name of what the item is aligned with, as the sentence says it. */
function targetName(a: Alignment, t: (k: string) => string): string {
  if (a.term) return "outside_domains" in a.term ? t("ontologyImport.outsideDomains") : a.term.name;
  return a.other_item?.name ?? "—";
}

/** The alignment as a sentence, read in its direction: "account is a kind of contract". */
export function directionPhrase(
  item: Pick<ImportItem, "name">,
  a: Alignment,
  t: (k: string, o?: Record<string, unknown>) => string,
): string {
  const other = targetName(a, t);
  const [from, to] = a.item_is_source ? [item.name, other] : [other, item.name];
  return t(`ontologyImport.phrase_${a.rel_type}`, { from, to });
}
