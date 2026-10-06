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

import type { FakeCatalog } from "../../../api/fakes";

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
