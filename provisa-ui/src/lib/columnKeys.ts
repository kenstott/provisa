// Copyright (c) 2026 Kenneth Stott
// Canary: 5b1e7d93-2c4a-4f86-a0d3-9e61c8b47f25
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A result may name two columns the same (`SELECT a.id, b.id`). The grid keys each row's values
// by its column's key, so each column gets a key of its own, by position: the first column of a
// name keeps the name, and each later one is the name plus a separator no column name holds and
// its position. A header, a download and a copy show the column's name (columnLabel), repeats as
// they are.

const SEP = "\u0000";

/** One key per column, in order: unique, and the name itself where the name is. */
export function columnKeys(names: readonly string[]): string[] {
  const seen = new Set<string>();
  return names.map((name, i) => {
    if (!seen.has(name)) {
      seen.add(name);
      return name;
    }
    return `${name}${SEP}${i}`;
  });
}

/** The column's name, as the result named it. */
export function columnLabel(key: string): string {
  const at = key.indexOf(SEP);
  return at === -1 ? key : key.slice(0, at);
}

/** A positional row as an object keyed by its columns' keys. */
export function rowByKeys(
  keys: readonly string[],
  row: readonly unknown[],
): Record<string, unknown> {
  return Object.fromEntries(keys.map((k, i) => [k, row[i]]));
}
