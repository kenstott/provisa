// Copyright (c) 2026 Kenneth Stott
// Canary: 1486f697-ce6f-4581-bb90-0ea4622829b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The guided tour is shown to whoever just created an org, and that person is an org_admin. A step
// whose route is gated on a right org_admin does not hold does not fail loudly: the route renders
// "You do not have permission to view this page", the step's anchor never mounts, and the runner
// sits on its waiting overlay until the anchor wait expires — a tour that stops dead partway with
// no error. /lineage was gated on `admin`, the PLATFORM wildcard (rights.py
// PLATFORM_BYPASS_CAPABILITIES), which no org role carries, while the endpoint behind it requires
// nothing at all.
//
// Both tables are plain literals, so they are read from the source rather than by mounting the
// router — what is asserted is the table itself, which is the thing that was wrong. The capability
// set is read from schema.sql's org_admin seed for the same reason: a right dropped from that seed
// has to surface here rather than in a tour nobody can finish.

import { describe, it, expect } from "vitest";
import { readFileSync } from "fs";
import { resolve } from "path";
import { TOUR_STEPS, stepRoute, tourItinerary } from "../tour/tourSteps";

const SRC = resolve(__dirname, "..");
const SCHEMA_SQL = resolve(SRC, "../../provisa/core/schema.sql");

function orgAdminCapabilities(): Set<string> {
  const sql = readFileSync(SCHEMA_SQL, "utf-8");
  const seed = /'org_admin',\s*'(\[[\s\S]*?\])'::jsonb/.exec(sql);
  if (!seed) throw new Error("schema.sql: org_admin role seed not found");
  return new Set(JSON.parse(seed[1]) as string[]);
}

/** Every path the tour navigates to, without its query string. */
function tourRoutes(): string[] {
  const source = readFileSync(resolve(SRC, "tour/tourSteps.ts"), "utf-8");
  const routes = new Set<string>();
  for (const m of source.matchAll(/route:\s*[`"]([^`"$]*)/g)) {
    const path = m[1].split("?")[0];
    if (path.startsWith("/")) routes.add(path);
  }
  return [...routes];
}

/** path → the capability its <Route> gate names, for the JSX route table in App.tsx. */
function routeCapabilities(): Record<string, string> {
  const source = readFileSync(resolve(SRC, "App.tsx"), "utf-8");
  const out: Record<string, string> = {};
  // A route is `path="/x"` followed, before the next `path=`, by the gate that wraps its element.
  const blocks = source.split(/path=/).slice(1);
  for (const block of blocks) {
    const path = /^"([^"]+)"/.exec(block);
    if (!path) continue;
    const cap = /capability=\{?"([a-z_]+)"/.exec(block);
    if (cap) out[path[1]] = cap[1];
  }
  // The Admin surfaces are a `[path, capability]` table rather than JSX routes.
  for (const m of source.matchAll(/\["(\/admin\/[^"]+)",\s*"([a-z_]+)"\]/g)) out[m[1]] = m[2];
  return out;
}

describe("guided tour route gates", () => {
  const caps = orgAdminCapabilities();
  const gates = routeCapabilities();

  it("reads the org_admin seed", () => {
    expect(caps.has("table_registration")).toBe(true);
    // The platform wildcard is what org_admin deliberately does NOT hold (REQ-1297).
    expect(caps.has("admin")).toBe(false);
  });

  it.each(tourRoutes())("an org_admin may reach %s", (route) => {
    const capability = gates[route];
    // A route with no gate is reachable by anyone signed in; nothing to assert.
    if (capability === undefined) return;
    expect(caps.has(capability)).toBe(true);
  });

  // What the runner drops a step on is the step's own `capability`, so that declaration — not the
  // route table above — is what decides whether a viewer is shown a page. If it disagrees with the
  // gate, the tour either walks into NotAuthorized and hangs, or hides a page the viewer could see.
  // REQ-1590: a step may name a STRICTER right than its route's gate when what it points at is not
  // on the page for everyone the route admits. /admin/glossary opens to a reader, but the glossary
  // step highlights the AI-generation buttons, which only a curator is shown. The right named must
  // still carry the viewer through the gate, which is what this map records — and the seeds grant
  // glossary_rw only alongside glossary_read.
  const IMPLIED_GATE: Record<string, string> = { glossary_rw: "glossary_read" };

  it.each(TOUR_STEPS.filter((s) => s.route).map((s) => [s.key, s.route!, s.capability] as const))(
    "step %s declares the gate its route carries",
    (_key, route, capability) => {
      const opens = IMPLIED_GATE[capability!] ?? capability;
      expect(opens).toBe(gates[route.split("?")[0]]);
    },
  );

  it.each(Object.entries(IMPLIED_GATE))("%s is seeded alongside %s", (strict, gate) => {
    expect(caps.has(strict)).toBe(true);
    expect(caps.has(gate)).toBe(true);
  });
});

describe("tour itinerary", () => {
  const all = (): number[] => tourItinerary(() => true);

  it("is every step when the viewer holds everything", () => {
    expect(all()).toEqual(TOUR_STEPS.map((_, i) => i));
  });

  it("is empty when the viewer holds nothing", () => {
    // Every step in the tour today sits under some gate, so a viewer with no rights has no tour —
    // which is what withholds the launcher and the welcome modal rather than offering an empty one.
    expect(tourItinerary(() => false)).toEqual([]);
  });

  it("drops a routeless step along with the route it continues on", () => {
    // The Views chapter: /views is the route owner, and the steps after it that omit `route` are
    // continuations of that page — several via a clickBefore the owner's step opened. Withholding
    // table_registration must take the whole chapter, not just the step that names the route.
    const kept = tourItinerary((c) => c !== "table_registration");
    for (const i of kept) {
      const route = stepRoute(i);
      if (route === undefined) continue;
      const owner = TOUR_STEPS.slice(0, i + 1).findLast((s) => s.route);
      expect(owner?.capability).not.toBe("table_registration");
    }
    // And it really did remove some — a filter that dropped nothing would pass the loop above.
    expect(kept.length).toBeLessThan(TOUR_STEPS.length);
  });

  it("keeps the steps of the chapters that survive", () => {
    const kept = tourItinerary((c) => c !== "table_registration");
    const keys = kept.map((i) => TOUR_STEPS[i].key);
    expect(keys).toContain("step0"); // /sources — source_registration, untouched
    expect(keys).not.toContain("step20"); // /views
    expect(keys).not.toContain("step22"); // /lineage
  });

  it("refuses a routed step that names no capability", () => {
    // The runner cannot decide whether to show a page whose gate is undeclared, and guessing is
    // how the lineage step came to hang. Surfacing it as an error is the point.
    const routed = TOUR_STEPS.find((s) => s.route)!;
    const saved = routed.capability;
    routed.capability = undefined;
    try {
      expect(() => tourItinerary(() => true)).toThrow(/no capability/);
    } finally {
      routed.capability = saved;
    }
  });
});
