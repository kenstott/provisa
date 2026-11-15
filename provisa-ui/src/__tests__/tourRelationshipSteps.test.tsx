// Copyright (c) 2026 Kenneth Stott
// Canary: 57b21ac2-942c-475f-b745-d62f716c3135
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2006: a demo's relationship steps walked on the real Relationships page. The cloudpolicy
// track shows ATTACHED_TO, then CITES on the same page: the second step inherited the first's
// search, its row filtered away, and the stuck notice came up over the first step's popover and row.

import { beforeEach, afterEach, describe, it, expect, vi } from "vitest";
import { act, fireEvent, screen } from "@testing-library/react";
import { Route, Routes } from "react-router-dom";
import { render } from "../test-utils/render";
import { RelationshipsPage } from "../pages/RelationshipsPage";
import { NamedDemoContext, type NamedDemo } from "../context/NamedDemoContext";
import type { DemoTourStepDef } from "../tour/demoTour";
import type { OrgHas } from "../tour/tourPrerequisites";
import i18n from "../i18n";

const org = vi.hoisted(() => ({ current: null as unknown }));
vi.mock("../hooks/useOrgHas", () => {
  const read = () => Promise.resolve(org.current);
  return { useOrgHas: () => read };
});

let chunksResolve: () => void = () => {};
vi.mock("../pageChunks", () => ({
  prefetchAllPageChunks: () =>
    new Promise<void>((resolve) => {
      chunksResolve = resolve;
    }),
}));

const rel = (id: string, source: string, target: string, alias: string) => ({
  id,
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: source,
  sourceDomainId: "cloudpolicy",
  targetTableName: target,
  sourceColumn: "x",
  targetColumn: "id",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias,
  graphqlAlias: null,
  computedCypherAlias: null,
  autoSuggested: false,
  disableCypher: false,
  viaTableName: null,
});

// One value per hook across renders: a fresh array each render reruns the page's effects forever.
const RELS = {
  relationships: [
    rel("disk-to-machine", "storage_resources", "compute_resources", "ATTACHED_TO"),
    rel("incident-to-disk", "incident", "storage_resources", "ABOUT"),
    rel("incident-to-control", "incident", "policy_sections", "CITES"),
  ],
  loading: false,
  refetch: vi.fn(),
};
const TABLES = { tables: [], loading: false };
const DOMAINS = { domains: [] };
const UPSERT = { upsertRelationship: vi.fn(async () => ({ success: true, message: "" })) };
const DELETE = { deleteRelationship: vi.fn() };
const AUTH = {
  loading: false,
  capabilities: ["create_relationship", "source_registration"],
  actingRoles: [],
};
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };

vi.mock("../hooks/useAdminQueries", () => ({
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => DOMAINS,
  useUpsertRelationship: () => UPSERT,
  useDeleteRelationship: () => DELETE,
  useDeleteRelationships: () => vi.fn(),
  useTourPrefetch: () => () => Promise.resolve(),
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchCandidates: vi.fn().mockResolvedValue([]),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
}));
vi.mock("../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../components/TagControl", () => ({ TagControl: () => null }));

const { TourProvider, useTour } = await import("../tour/useTour");

const ORG: OrgHas = {
  sources: new Set(["inventory"]),
  tables: new Set(["inventory.storage_resources"]),
  dataProducts: 0,
  pollyConfigured: true,
};

const OPENS_PAGE: DemoTourStepDef = {
  key: "cloudpolicyModel",
  route: "/relationships",
  capability: "create_relationship",
  relationship: "ATTACHED_TO",
};

function demo(tour: DemoTourStepDef[]): NamedDemo {
  return {
    name: "cloudpolicy",
    exampleQueries: [],
    tour,
    landing: null,
    notebook: [],
    orgFiles: [],
  };
}

function Launcher() {
  const { startTour } = useTour();
  return (
    <button type="button" onClick={() => startTour()}>
      launch
    </button>
  );
}

async function walk(tour: DemoTourStepDef[]) {
  render(
    <NamedDemoContext.Provider value={demo(tour)}>
      <TourProvider>
        <Launcher />
        {/* As in the app: the page mounts on the step's own route, its search in the URL. */}
        <Routes>
          <Route path="/relationships" element={<RelationshipsPage />} />
          <Route path="*" element={null} />
        </Routes>
      </TourProvider>
    </NamedDemoContext.Provider>,
  );
  fireEvent.click(screen.getByText("launch"));
  await act(async () => chunksResolve());
}

const lit = () =>
  document.querySelector(".driver-active-element")?.getAttribute("data-tour-relationship");
const stuck = () => screen.queryByText(i18n.t("tour.status.stuck"));
const press = (selector: string) =>
  fireEvent.click(document.querySelector<HTMLElement>(selector)!);
const past = (ms: number) =>
  act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });

describe("a demo's relationship steps on the Relationships page", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    org.current = ORG;
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => vi.useRealTimers());

  it("walks one relationship after another on the page, and back, never stuck", async () => {
    await walk([OPENS_PAGE, { key: "cloudpolicyCites", relationship: "CITES" }]);
    await vi.waitFor(() => expect(lit()).toBe("ATTACHED_TO"));

    press(".driver-popover-next-btn");
    await past(62000);
    expect(stuck()).toBeNull();
    expect(lit()).toBe("CITES");

    press(".driver-popover-prev-btn");
    await past(62000);
    expect(stuck()).toBeNull();
    expect(lit()).toBe("ATTACHED_TO");
  });

  it("is stuck on a step whose relationship the org does not hold, opening the page", async () => {
    await walk([{ ...OPENS_PAGE, relationship: "NOT_DECLARED" }]);
    await past(62000);
    expect(stuck()).not.toBeNull();
  });

  it("is stuck on a later step whose relationship the org does not hold", async () => {
    await walk([OPENS_PAGE, { key: "cloudpolicyCites", relationship: "NOT_DECLARED" }]);
    await vi.waitFor(() => expect(lit()).toBe("ATTACHED_TO"));

    press(".driver-popover-next-btn");
    await past(62000);
    expect(stuck()).not.toBeNull();
  });
});
