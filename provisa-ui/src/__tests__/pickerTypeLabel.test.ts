// Copyright (c) 2026 Kenneth Stott
// Canary: 9c3e7a51-2d84-4f6b-b1a0-5e8d2c4f7a13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The type picker names the hosted services that are a source type, so a reader looking for their
// provider (Neon, RDS, ...) finds PostgreSQL; the source's own type label stays the plain name.

import { describe, expect, it } from "vitest";
import { SOURCE_TYPES } from "../pages/sources/constants";
import { pickerTypeLabel, sourceTypeLabel } from "../pages/sources/sourceHelpers";

const byValue = (v: string) => SOURCE_TYPES.find((s) => s.value === v)!;

describe("pickerTypeLabel", () => {
  it("lists the managed PostgreSQL services after the name", () => {
    expect(pickerTypeLabel(byValue("postgresql"))).toBe(
      "PostgreSQL (RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon)",
    );
  });

  it("leaves a type without managed services as its name", () => {
    expect(pickerTypeLabel(byValue("mysql"))).toBe("MySQL");
  });

  it("keeps the plain name where a source's type is shown", () => {
    expect(sourceTypeLabel("postgresql", null)).toBe("PostgreSQL");
  });
});
