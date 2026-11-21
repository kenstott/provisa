// Copyright (c) 2026 Kenneth Stott
// Canary: 3f4bb1e4-515c-427a-8f2f-e56da53862f7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-194: naming.ts gives exactly the names provisa/compiler/naming.py gives. The vectors are
// naming.py's own answers (scripts/gen_naming_vectors.py writes them; tests/unit/
// test_naming_vectors.py holds the file to naming.py), so the two cannot drift apart.

import { describe, expect, it } from "vitest";
import { applyConvention, toCamelCase, toPascalCase, toSnakeCase } from "../naming";
import vectors from "../__fixtures__/naming-vectors.json";

const NAMES = vectors.names.map((v) => [v.input, v] as const);

describe("naming.ts answers as naming.py does", () => {
  it.each(NAMES)("toSnakeCase(%j)", (input, v) => {
    expect(toSnakeCase(input)).toBe(v.snake);
  });

  it.each(NAMES)("toCamelCase(%j)", (input, v) => {
    expect(toCamelCase(input)).toBe(v.camel);
  });

  it.each(NAMES)("toPascalCase(%j)", (input, v) => {
    expect(toPascalCase(input)).toBe(v.pascal);
  });

  it.each(NAMES)("applyConvention(%j, every convention naming.py accepts)", (input, v) => {
    const got = Object.fromEntries(
      Object.keys(v.convention).map((c) => [c, applyConvention(input, c)]),
    );
    expect(got).toEqual(v.convention);
  });
});
