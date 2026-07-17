// Copyright (c) 2026 Kenneth Stott
// Canary: ab1c3670-385b-4459-92f6-dea610cef8bf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-846: schema-discovery normalizes a discovered native column type to a canonical IR name so it
// lands on a valid IR dropdown value; the steward's assignment is engine-independent.

import { describe, it, expect } from "vitest";
import { toIrType } from "../irTypes";

describe("toIrType (native → IR)", () => {
  it("maps common engine spellings to canonical IR names", () => {
    expect(toIrType("VARCHAR")).toBe("text");
    expect(toIrType("varchar(255)")).toBe("text"); // strips the length qualifier
    expect(toIrType("INT8")).toBe("bigint");
    expect(toIrType("tinyint")).toBe("smallint");
    expect(toIrType("double precision")).toBe("double");
    expect(toIrType("DECIMAL(10,2)")).toBe("numeric");
    expect(toIrType("timestamp with time zone")).toBe("timestamp");
    expect(toIrType("JSONB")).toBe("text");
    expect(toIrType("varbinary")).toBe("bytea");
  });

  it("passes canonical IR names through unchanged (idempotent)", () => {
    for (const t of ["text", "integer", "bigint", "boolean", "timestamp", "uuid"]) {
      expect(toIrType(t)).toBe(t);
    }
  });

  it("lowercases an unmapped spelling rather than dropping it", () => {
    expect(toIrType("GEOMETRY")).toBe("geometry");
  });
});
