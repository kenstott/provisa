// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e4a90-2b5d-4f38-a6e1-5d9c03b78f12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1327 (amended 2026-11-14): the page of views, metrics, commands and roles opens on the right
// the server asks, in the nav, the route table and the tour alike.

import { describe, it, expect } from "vitest";
import { readFileSync } from "fs";
import { resolve } from "path";
import { NAV_GROUPS } from "../components/navGroups";
import { meetsRequirement } from "../lib/capabilities";

const SRC = resolve(__dirname, "..");

function navItem(to: string) {
  const item = NAV_GROUPS.flatMap((g) => g.items).find((i) => i.to === to);
  expect(item, to).toBeDefined();
  return item!;
}

/** The gate of a route in App.tsx: the CapabilityGate props that follow its `path`. */
function routeGate(path: string): string {
  const source = readFileSync(resolve(SRC, "App.tsx"), "utf-8");
  const at = source.indexOf(`path="${path}"`);
  expect(at, path).toBeGreaterThan(-1);
  const gate = source.indexOf("<CapabilityGate", at);
  return source.slice(gate, source.indexOf(">", gate));
}

describe("the model pages open on the right the server asks", () => {
  it("opens Views and Metrics on create_view, never table_registration", () => {
    for (const to of ["/views", "/metrics"]) {
      const req = navItem(to);
      expect(meetsRequirement(["create_view"], req), to).toBe(true);
      expect(meetsRequirement(["table_registration"], req), to).toBe(false);
      expect(routeGate(to), to).toContain('capability="create_view"');
    }
  });

  it("opens Commands to an author and to a holder of command_execution", () => {
    const req = navItem("/commands");
    expect(meetsRequirement(["table_registration"], req)).toBe(true);
    expect(meetsRequirement(["command_execution"], req)).toBe(true);
    expect(meetsRequirement(["create_view"], req)).toBe(false);
    expect(routeGate("/commands")).toContain('orCapability="command_execution"');
  });

  it("opens Roles on access_config or user_management", () => {
    const req = navItem("/security/roles");
    expect(meetsRequirement(["access_config"], req)).toBe(true);
    expect(meetsRequirement(["user_management"], req)).toBe(true);
    expect(meetsRequirement(["table_registration"], req)).toBe(false);
    expect(routeGate("/security/roles")).toContain('orCapability="user_management"');
  });

  it("keeps the tour's Views and Metrics steps on create_view", () => {
    const source = readFileSync(resolve(SRC, "tour/tourSteps.ts"), "utf-8");
    for (const route of ["/views", "/metrics"]) {
      expect(source).toContain(`route: "${route}",\n    capability: "create_view"`);
    }
  });
});
