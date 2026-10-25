// Copyright (c) 2026 Kenneth Stott
// Canary: 9cee0b48-00cb-4599-b7ae-e698ccb96fe3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A column's ordered mask list: each entry names the readers it is for (grant entries, composites
// included, "*" for everyone) and the mask they read the column through, or none. A reader reads
// the column through the first entry it holds; a reader holding none reads it unmasked.

import type { TFunction } from "i18next";
import { grantLabel } from "./grants";

export const EVERYONE = "*";

export type MaskKind = "regex" | "constant" | "truncate";

export interface MaskSpec {
  type: MaskKind;
  pattern?: string | null;
  replace?: string | null;
  value?: string | number | null;
  precision?: string | null;
}

export interface MaskEntry {
  to: string[];
  mask: MaskSpec | null;
}

/** A new list: one entry, for everyone, masking to NULL -- so a reader the list does not name is
 *  masked, not left unmasked. */
export function newMaskList(): MaskEntry[] {
  return [{ to: [EVERYONE], mask: { type: "constant", value: null } }];
}

/** ``list`` with a new, empty entry before its final entry for everyone (else at its end). */
export function withNewEntry(list: MaskEntry[]): MaskEntry[] {
  const entry: MaskEntry = { to: [], mask: null };
  const last = list[list.length - 1];
  if (last && last.to.includes(EVERYONE)) return [...list.slice(0, -1), entry, last];
  return [...list, entry];
}

/** ``list`` with entry ``i`` moved by ``by`` places. */
export function moved(list: MaskEntry[], i: number, by: -1 | 1): MaskEntry[] {
  const j = i + by;
  if (j < 0 || j >= list.length) return list;
  const out = [...list];
  [out[i], out[j]] = [out[j], out[i]];
  return out;
}

/** A constant's value as typed: blank is NULL, a number stays a number. */
export function maskValueOf(text: string): string | number | null {
  if (text.trim() === "") return null;
  const n = Number(text);
  return Number.isFinite(n) && /^-?\d+(\.\d+)?$/.test(text.trim()) ? n : text;
}

/** Who an entry is for, as a reader reads it. */
export function maskToLabel(to: string[], t: TFunction): string {
  if (to.length === 0) return t("maskList.nobody");
  return to.map((e) => (e === EVERYONE ? t("maskList.everyone") : grantLabel(e, t))).join(", ");
}

/** What an entry does, as a reader reads it. */
export function maskSpecLabel(mask: MaskSpec | null, t: TFunction): string {
  if (mask === null) return t("maskList.unmasked");
  if (mask.type === "regex")
    return t("maskList.regexLabel", { pattern: mask.pattern ?? "", replace: mask.replace ?? "" });
  if (mask.type === "truncate")
    return t("maskList.truncateLabel", { precision: mask.precision ?? "" });
  return mask.value === null || mask.value === undefined
    ? t("maskList.constantNull")
    : t("maskList.constantLabel", { value: String(mask.value) });
}

/** ``template`` with each ``{name}`` replaced by its number. */
export function filledTemplate(template: string, values: Record<string, number>): string {
  return template.replace(/\{(\w+)\}/g, (whole, name: string) =>
    name in values ? String(values[name]) : whole,
  );
}
