// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1f3a9e-4c7d-4e25-8d0a-93f5e2b7c418
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** The languages an Explore editor holds, by their comment syntax. `json` has no comments: a
 *  request body is empty only when it is blank. */
export type EditorLanguage = "graphql" | "cypher" | "sparql" | "sql" | "python" | "json";

interface Syntax {
  /** Starts a comment that ends at the line's end. */
  line: string[];
  /** Starts a comment that ends at its closer. */
  block: [string, string][];
  /** Characters that separate nothing and end nothing: not a statement. */
  inert: string;
}

const SYNTAX: Record<EditorLanguage, Syntax> = {
  graphql: { line: ["#"], block: [], inert: "," },
  cypher: { line: ["//"], block: [["/*", "*/"]], inert: ";" },
  sparql: { line: ["#"], block: [], inert: "" },
  sql: { line: ["--"], block: [["/*", "*/"]], inert: ";" },
  python: { line: ["#"], block: [], inert: "" },
  json: { line: [], block: [], inert: "" },
};

/**
 * Whether ``text`` holds no statement to run: it is blank, or holds only comments (and, where the
 * language allows them, empty statements). The scan ends at the first character that is code, so
 * a comment marker inside a string or IRI never matters: the string already is code.
 */
export function hasNoStatement(text: string, language: EditorLanguage): boolean {
  const { line, block, inert } = SYNTAX[language];
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (/\s/.test(ch) || inert.includes(ch)) {
      i += 1;
      continue;
    }
    if (line.some((m) => text.startsWith(m, i))) {
      const end = text.indexOf("\n", i);
      i = end < 0 ? text.length : end + 1;
      continue;
    }
    const open = block.find(([o]) => text.startsWith(o, i));
    if (open) {
      const end = text.indexOf(open[1], i + open[0].length);
      i = end < 0 ? text.length : end + open[1].length;
      continue;
    }
    return false;
  }
  return true;
}
