// Copyright (c) 2026 Kenneth Stott
// Canary: 12249dd0-b36d-4d35-a1ee-586832db74de
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1362: the tour waits for its destinations' DATA, not only their code.
 *
 * Preloading the route chunks removes fetch-and-parse time; it does nothing about the queries each
 * page gates its render on. Three properties keep a step from landing on a "Loading…" screen with
 * none of its anchors present: the prefetch covers every query the destination pages read, a query
 * the visitor's role cannot see does not keep the tour from starting, and a page holding a warm
 * cache does not re-blank itself while revalidating.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import type { DocumentNode } from "graphql";
import { TOUR_STEPS } from "../tour/tourSteps";

const executed: string[] = [];
let failing: string | null = null;

function operationName(doc: DocumentNode): string {
  for (const def of doc.definitions) {
    if (def.kind === "OperationDefinition" && def.name) return def.name.value;
  }
  throw new Error("query document has no operation name");
}

let queryResult: { data: unknown; loading: boolean } = { data: undefined, loading: false };

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory leaks into other files.
vi.mock("@apollo/client/react", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@apollo/client/react")>()),
  useLazyQuery: (doc: DocumentNode) => {
    const name = operationName(doc);
    return [
      () => {
        executed.push(name);
        return name === failing
          ? Promise.reject(new Error("forbidden"))
          : Promise.resolve({ data: {} });
      },
    ];
  },
  useQuery: () => ({ ...queryResult, error: undefined, refetch: vi.fn() }),
}));

const { useTourPrefetch, useSources, useRelationships, useRoles } =
  await import("../hooks/useAdminQueries");

describe("REQ-1362 tour data preload", () => {
  beforeEach(() => {
    executed.length = 0;
    failing = null;
  });

  it("runs every query the tour's destination pages read", async () => {
    const { result } = renderHook(() => useTourPrefetch());
    await act(() => result.current());

    // /relationships, /security/*, /sources, /tables, /views and /admin between them read exactly
    // these; a page whose query is missing here paints its own loading state on arrival.
    expect(new Set(executed)).toEqual(
      new Set([
        "SourcesQuery",
        "DomainsQuery",
        "TablesQuery",
        "RelationshipsQuery",
        "AllRelationshipsQuery",
        "RLSRulesQuery",
        "RolesQuery",
      ]),
    );
  });

  it("still resolves when a query the visitor cannot read is rejected", async () => {
    // Roles and RLS rules need admin capabilities. A visitor without them must still get a tour.
    failing = "RolesQuery";
    const { result } = renderHook(() => useTourPrefetch());
    await expect(act(() => result.current())).resolves.toBeUndefined();
    expect(executed).toHaveLength(7);
  });
});

describe("REQ-1362 warm-cache loading semantics", () => {
  const hooks: [string, () => { loading: boolean }][] = [
    ["useSources", useSources],
    ["useRelationships", useRelationships],
    ["useRoles", useRoles],
  ];

  it("reports loading only while there is genuinely nothing to paint", () => {
    queryResult = { data: undefined, loading: true };
    for (const [name, hook] of hooks) {
      expect(renderHook(() => hook()).result.current.loading, name).toBe(true);
    }
  });

  it("does not re-blank a page whose cache is warm", () => {
    // cache-and-network reports loading: true on every revalidating mount. Gating the render on
    // that flag is what made the prefetched data useless — the page blanked itself anyway.
    queryResult = { data: { sources: [], relationships: [], roles: [] }, loading: true };
    for (const [name, hook] of hooks) {
      expect(renderHook(() => hook()).result.current.loading, name).toBe(false);
    }
  });
});

describe("REQ-1362 step readiness gating", () => {
  // Pages that replace their whole body with a loading state until their queries land. A step
  // arriving here must wait for page content, or its popover points at an anchor-less screen.
  const GATED_ROUTES = [
    "/sources",
    "/tables",
    "/views",
    "/relationships",
    "/security/roles",
    "/security/rls",
    "/admin/overview",
  ];

  // Selectors living in the always-mounted shell: they resolve the instant the route changes,
  // before the destination has painted anything of its own.
  const isShellSelector = (sel: string) =>
    sel.startsWith(".subnav") || sel.startsWith(".navbar") || sel.startsWith('[data-tour="nav-');

  const inheritedRoute = (index: number): string | undefined => {
    for (let i = index; i >= 0; i--) {
      const route = TOUR_STEPS[i].route;
      if (route) return route;
    }
    return undefined;
  };

  it("gates every shell-anchored step on a gated page", () => {
    const shellSteps = TOUR_STEPS.map((step, i) => ({ step, i })).filter(
      ({ step, i }) => isShellSelector(step.element) && GATED_ROUTES.includes(inheritedRoute(i)!),
    );
    expect(shellSteps.length).toBeGreaterThan(0);

    for (const { step } of shellSteps) {
      expect(step.readySelector ?? step.clickBefore, `${step.key} (${step.element})`).toBeTruthy();
    }
  });

  it("points every readySelector at page content rather than the shell", () => {
    const gated = TOUR_STEPS.filter((s) => s.readySelector);
    expect(gated.length).toBeGreaterThan(0);
    for (const step of gated) {
      expect(isShellSelector(step.readySelector!), `${step.key}`).toBe(false);
    }
  });
});
