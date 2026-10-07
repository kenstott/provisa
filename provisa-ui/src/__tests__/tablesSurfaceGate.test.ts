// Copyright (c) 2026 Kenneth Stott
// Canary: 2a9c4e71-8b3d-4f05-96e2-c1d7b8a4e360
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1944: the Tables surface opens to a table editor or to a holder of any hiding right, and a
// holder of the hiding rights governs the domains of the roles carrying them -- decided by the
// rights a role holds, never by its name.

import { readFileSync } from "fs";
import { resolve } from "path";
import { describe, it, expect } from "vitest";
import {
  HIDING_RIGHTS,
  TABLES_SURFACE,
  hidingDomains,
  isDemonstrated,
  meetsRequirement,
} from "../lib/capabilities";
import type { Capability } from "../types/auth";

describe("the Tables surface gate (REQ-1944)", () => {
  it.each([
    "table_registration",
    "masking_config",
    "column_grant",
    "access_config",
    "sensitive_data",
  ] as Capability[])("opens to a holder of %s", (right) => {
    expect(meetsRequirement([right], TABLES_SURFACE)).toBe(true);
  });

  it.each([
    "usage",
    "query_development",
    "glossary_rw",
    "data_product_rw",
    "view_governance",
  ] as Capability[])("stays closed to a holder of %s alone", (right) => {
    expect(meetsRequirement([right], TABLES_SURFACE)).toBe(false);
  });

  it("is demonstrated when any one of its rights is", () => {
    expect(isDemonstrated(["sensitive_data"], TABLES_SURFACE)).toBe(true);
    expect(isDemonstrated(["usage"], TABLES_SURFACE)).toBe(false);
  });

  it("names the four hiding rights and not the table editor's", () => {
    expect([...HIDING_RIGHTS].sort()).toEqual(
      ["access_config", "column_grant", "masking_config", "sensitive_data"].sort(),
    );
  });
});

describe("hidingDomains (REQ-1944)", () => {
  it("is the domains of the roles carrying a hiding right, not of every role held", () => {
    const governed = hidingDomains([
      { capabilities: ["masking_config"], domain_access: ["sales"] },
      { capabilities: ["usage"], domain_access: ["finance"] },
    ]);
    expect(governed).toEqual(new Set(["sales"]));
  });

  it("is every domain when one such role reaches all of them", () => {
    expect(hidingDomains([{ capabilities: ["sensitive_data"], domain_access: ["*"] }])).toBeNull();
  });

  it("is nothing for a caller holding no hiding right", () => {
    expect(hidingDomains([{ capabilities: ["usage"], domain_access: ["*"] }])).toEqual(new Set());
  });
});

describe("the /tables route and its nav link carry the surface gate (REQ-1944)", () => {
  const SRC = resolve(__dirname, "..");

  it("the route opens to the hiding rights as well as table_registration", () => {
    const app = readFileSync(resolve(SRC, "App.tsx"), "utf-8");
    const at = app.indexOf('path="/tables"');
    expect(at).toBeGreaterThan(-1);
    const gate = app.slice(at, at + 400);
    expect(gate).toContain('capability="table_registration"');
    expect(gate).toContain("anyOf={HIDING_RIGHTS}");
  });

  it("the nav link is gated the same way", () => {
    const nav = readFileSync(resolve(SRC, "components/NavBar.tsx"), "utf-8");
    const at = nav.indexOf('to="/tables"');
    expect(at).toBeGreaterThan(-1);
    expect(nav.slice(Math.max(0, at - 200), at)).toContain(
      'capability="table_registration" anyOf={HIDING_RIGHTS}',
    );
  });
});
