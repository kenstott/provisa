// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The tour's per-step warm-ups: the REST reads its start-up prefetch does not cover.
 *
 * A step whose page holds its loading state on its own round-trip (TablesPage and /views on
 * /admin/settings, the DAG step on the lineage analysis) only starts that request once Next has
 * been clicked and the route has mounted — on a loaded machine that is the whole delay the visitor
 * feels. The runner fires the warm-up one step early; these tests pin both halves: every declared
 * warm-up resolves to a real action, and the page's own call adopts the warm request instead of
 * opening a second one.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { TOUR_STEPS, LINEAGE_DEMO_SQL } from "../tour/tourSteps";
import { PREFETCH_ACTIONS } from "../tour/useTour";
import { prefetchSettings, fetchSettings } from "../api/admin";
import { prefetchLineageGraph, fetchLineageGraph } from "../api/lineage";

const json = (body: unknown) => ({ ok: true, status: 200, json: async () => body }) as Response;

describe("tour step prefetch", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json({ nodes: [], edges: [], outputs: [] })),
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("resolves every declared warm-up to a registered action", () => {
    const declared = TOUR_STEPS.map((s) => s.prefetch).filter((n): n is string => Boolean(n));
    expect(declared.length).toBeGreaterThan(0);
    for (const name of declared) {
      expect(PREFETCH_ACTIONS[name], `unregistered prefetch "${name}"`).toBeTypeOf("function");
    }
  });

  it("warms the pages whose loading state waits on a REST read", () => {
    // The steps that made an advance feel dead: the quality-contract table and the lineage DAG.
    const quality = TOUR_STEPS.find((s) => s.key === "stepQualityTable");
    const lineage = TOUR_STEPS.find((s) => s.key === "step22");
    expect(quality?.prefetch).toBe("settings");
    expect(lineage?.prefetch).toBe("lineageDemo");
  });

  it("lets the settings page adopt the prefetched read instead of repeating it", async () => {
    prefetchSettings();
    // A second warm-up before the page mounts (the step warms its own entry as well as being
    // warmed by its predecessor) must not double the request.
    prefetchSettings();
    await fetchSettings();
    expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);

    // The entry is consumed once: a later read — an admin save, a manual refresh — re-queries.
    await fetchSettings();
    expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
  });

  it("lets the lineage page adopt the analysis started a step earlier", async () => {
    prefetchLineageGraph(LINEAGE_DEMO_SQL);
    await fetchLineageGraph(LINEAGE_DEMO_SQL);
    expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);

    // A different statement shares nothing with the warmed one.
    await fetchLineageGraph("SELECT 1");
    expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
  });
});
