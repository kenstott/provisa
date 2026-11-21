// Copyright (c) 2026 Kenneth Stott
// Canary: 0b6f7c1e-3d52-4a8e-9a47-6c1d2e5f8b13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A selection answers under the camelCase key the naming authority (src/naming.ts) gives the
// column, so the key a caller reads is the one the authority computes -- not a local rule.

import { describe, expect, it } from "vitest";
import { selectionSet, type MetaReader } from "../metaGraph";
import { toCamelCase } from "../../naming";

const reader = (schemaSpelling: (f: string) => string): MetaReader =>
  ({
    schema: { column: (_t: string, f: string) => schemaSpelling(f) },
    roots: new Map(),
  }) as unknown as MetaReader;

describe("selectionSet row keys", () => {
  it.each(["order_id", "created_at", "orderID", "Order_ID", "col_1x"])(
    "answers %s under the authority's camelCase key",
    (column) => {
      const out = selectionSet(reader((f) => f), "T", { fields: [column] });
      const key = toCamelCase(column);
      expect(out.replace(/[{}\s]+/g, " ").trim()).toBe(key === column ? column : `${key}: ${column}`);
    },
  );
});
