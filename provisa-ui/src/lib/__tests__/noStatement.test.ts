// Copyright (c) 2026 Kenneth Stott
// Canary: 0c95e7b2-6a3d-4814-9f1e-27d8b4a5c630
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { hasNoStatement } from "../noStatement";

describe("hasNoStatement", () => {
  it.each(["graphql", "cypher", "sparql", "sql", "python", "json"] as const)(
    "blank text in %s",
    (lang) => {
      expect(hasNoStatement("", lang)).toBe(true);
      expect(hasNoStatement("  \n\t ", lang)).toBe(true);
    },
  );

  it("comment-only text in each language's own syntax", () => {
    expect(hasNoStatement("# Welcome\n# to GraphiQL\n", "graphql")).toBe(true);
    expect(hasNoStatement("// a\n/* b\nc */\n", "cypher")).toBe(true);
    expect(hasNoStatement("# note", "sparql")).toBe(true);
    expect(hasNoStatement("-- a\n/* b */;\n", "sql")).toBe(true);
    expect(hasNoStatement("# nothing", "python")).toBe(true);
  });

  it("an unterminated block comment holds no statement", () => {
    expect(hasNoStatement("/* never closed\nSELECT 1", "sql")).toBe(true);
  });

  it("a real statement is one, with or without comments around it", () => {
    expect(hasNoStatement("# c\n{ orders { id } }", "graphql")).toBe(false);
    expect(hasNoStatement("// c\nMATCH (n) RETURN n", "cypher")).toBe(false);
    expect(hasNoStatement("# c\nSELECT * WHERE { ?s ?p ?o }", "sparql")).toBe(false);
    expect(hasNoStatement("-- c\nSELECT 1", "sql")).toBe(false);
    expect(hasNoStatement("/* c */ SELECT 1 -- d", "sql")).toBe(false);
    expect(hasNoStatement("x = 1", "python")).toBe(false);
    expect(hasNoStatement("{}", "json")).toBe(false);
  });

  it("another language's comment marker is code", () => {
    expect(hasNoStatement("-- c", "graphql")).toBe(false);
    expect(hasNoStatement("# c", "sql")).toBe(false);
    expect(hasNoStatement("// c", "json")).toBe(false);
  });

  it("a lone separator is no statement where the language allows empty ones", () => {
    expect(hasNoStatement(" ; ;", "sql")).toBe(true);
    expect(hasNoStatement(";", "sparql")).toBe(false);
  });
});
