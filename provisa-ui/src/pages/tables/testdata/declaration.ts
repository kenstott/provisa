// Copyright (c) 2026 Kenneth Stott
// Canary: 262584b1-d325-436c-8c26-10d4ae64d3b6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1494: a fake's declaration as the picker builds it -- the kind or method's name and the
// arguments given, written as the server reads it: kind(positional, ..., name=value, ...).

import type { FakeCatalog, FakeFit } from "../../../api/fakes";

export type ArgValues = Record<string, string>;

export interface Choice {
  // A Provisa kind or a fake method.
  type: "kind" | "method";
  name: string;
}

// The declaration for a choice and its argument values; an argument left blank is not written.
export function compose(catalog: FakeCatalog, choice: Choice, values: ArgValues): string {
  const given = (n: string) => (values[n] ?? "").trim();
  if (choice.type === "method") {
    const method = catalog.methods.find((m) => m.name === choice.name);
    const named = (method?.params ?? [])
      .filter((p) => given(p.name))
      .map((p) => `${p.name}=${given(p.name)}`);
    return `${choice.name}(${named.join(", ")})`;
  }
  const kind = catalog.kinds.find((k) => k.name === choice.name);
  const args = (kind?.args ?? []).filter((a) => given(a.name));
  const written = kind?.positional
    ? args.map((a) => given(a.name))
    : args.map((a) => `${a.name}=${given(a.name)}`);
  return `${choice.name}(${written.join(", ")})`;
}

// The kind or method a declaration names, or null for an empty or unreadable one.
export function chosen(
  catalog: FakeCatalog,
  declaration: string | null | undefined,
): Choice | null {
  const name = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(/.exec(declaration ?? "")?.[1];
  if (!name) return null;
  if (catalog.kinds.some((k) => k.name === name)) return { type: "kind", name };
  if (catalog.methods.some((m) => m.name === name)) return { type: "method", name };
  return null;
}

// A declaration's arguments at its top level, split at the commas outside parentheses and quotes.
function topLevel(inner: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let quote = "";
  let start = 0;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (quote) {
      if (ch === quote) quote = "";
    } else if (ch === "'" || ch === '"') quote = ch;
    else if (ch === "(") depth++;
    else if (ch === ")") depth--;
    else if (ch === "," && depth === 0) {
      parts.push(inner.slice(start, i).trim());
      start = i + 1;
    }
  }
  parts.push(inner.slice(start).trim());
  return parts.filter((p) => p !== "");
}

// The argument values a declaration gives, by name: the inverse of compose. A positional kind's
// arguments are named by their place; a named one by the name written before it.
export function parseArgs(catalog: FakeCatalog, declaration: string | null | undefined): ArgValues {
  const choice = chosen(catalog, declaration);
  const text = (declaration ?? "").trim();
  if (choice === null || !text.endsWith(")")) return {};
  const inner = text.slice(text.indexOf("(") + 1, -1);
  const kind =
    choice.type === "kind" ? catalog.kinds.find((k) => k.name === choice.name) : undefined;
  const values: ArgValues = {};
  topLevel(inner).forEach((part, i) => {
    const named = /^([A-Za-z_][A-Za-z0-9_]*)\s*=(?!=)\s*(.*)$/s.exec(part);
    if (named && !kind?.positional) values[named[1]] = named[2].trim();
    else if (kind?.positional && kind.args[i]) values[kind.args[i].name] = part;
  });
  return values;
}

// Why a column's fake cannot be stable, as the server's check would refuse it, or null when it can
// be (or the declaration is one only the server can read).
export type StableBlock =
  | { reason: "noFake" }
  | { reason: "never" | "noArguments"; name: string }
  | { reason: "needs"; name: string; args: string[] };

export function stableBlock(
  catalog: FakeCatalog,
  declaration: string | null | undefined,
): StableBlock | null {
  if (!(declaration ?? "").trim()) return { reason: "noFake" };
  const choice = chosen(catalog, declaration);
  if (choice === null) return null;
  const entry =
    choice.type === "kind"
      ? catalog.kinds.find((k) => k.name === choice.name)
      : catalog.methods.find((m) => m.name === choice.name);
  if (!entry) return null;
  if (!entry.stable) return { reason: "never", name: choice.name };
  const given = parseArgs(catalog, declaration);
  if (choice.type === "method")
    return Object.keys(given).length ? { reason: "noArguments", name: choice.name } : null;
  const missing = entry.stableNeeds.filter((a) => !given[a]);
  return missing.length ? { reason: "needs", name: choice.name, args: missing } : null;
}

// The words of a StableBlock, for the Stable control's tip.
export function stableReasonKey(block: StableBlock): [string, Record<string, string>] {
  switch (block.reason) {
    case "noFake":
      return ["testData.stableNoFake", {}];
    case "never":
      return ["testData.stableNever", { name: block.name }];
    case "noArguments":
      return ["testData.stableNoArguments", { name: block.name }];
    case "needs":
      return ["testData.stableNeeds", { name: block.name, args: block.args.join(", ") }];
  }
}

// Whether a kind or method fills a column of `family` and belongs in the list: a fake's list
// leaves out the kinds only a synthetic rule may declare.
export function offeredFor(entry: FakeFit & { ruleOnly?: boolean }, family: string, rule: boolean) {
  return entry.families.includes(family) && (rule || !entry.ruleOnly);
}
