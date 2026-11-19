// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2253: term finding and table search ask for the domains selected in the navbar filter, so
// the server reads, sends to the model and proposes nothing outside them. Every domain checked
// asks for no narrowing (null).

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";

const api = vi.hoisted(() => ({
  fetchScannableDocuments: vi.fn(),
  fetchScans: vi.fn(),
  fetchScan: vi.fn(),
  startScan: vi.fn(),
}));
vi.mock("../api/termFinding", () => api);
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => TABLES,
}));
const TABLES = vi.hoisted(() => ({ tables: [], loading: false, refetch: () => undefined }));
const filter = vi.hoisted(() => ({
  domains: ["sales", "hr"],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set<string>(["sales"]),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: true,
}));
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => filter }));

import { FindTermsDialog } from "../components/admin/FindTermsDialog";

const SCAN = {
  id: 7,
  document_id: 4,
  document_title: "Client policy",
  document_source: "https://policies.example.com/client.html",
  document_removed: false,
  source_table: null,
  domains: ["sales"],
  status: "done",
  error: null,
  summary: null,
  started_by: "ada",
  started_at: "2026-10-29T09:00:00Z",
  finished_at: "2026-10-29T09:01:00Z",
  model: "anthropic:claude-haiku-4-5",
  step: null,
  batch: null,
  batches: null,
  budget_limit: null,
  counts: {},
};

function open() {
  return render(
    <FindTermsDialog
      terms={[]}
      domainOptions={["sales", "hr"]}
      domainsEnabled
      defaultDomains={["sales"]}
      onClose={() => undefined}
      onGlossaryChanged={() => undefined}
    />,
  );
}

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  filter.checkedDomains = new Set(["sales"]);
  api.fetchScannableDocuments.mockResolvedValue([]);
  api.fetchScans.mockResolvedValue([SCAN]);
  api.fetchScan.mockResolvedValue({ scan: SCAN, drafts: [] });
  api.startScan.mockResolvedValue({ scan: { ...SCAN, id: 8 } });
});

describe("Find terms asks for the selected domains", () => {
  it("offers only the selection's documents and scans", async () => {
    open();
    await waitFor(() =>
      expect(api.fetchScannableDocuments).toHaveBeenCalledWith(new Set(["sales"])),
    );
    expect(api.fetchScans).toHaveBeenCalledWith(new Set(["sales"]));
  });

  it("starts a scan within the selection", async () => {
    open();
    await screen.findByTestId("find-terms-scan-7");
    fireEvent.click(screen.getByRole("radio", { name: "A URL" }));
    fireEvent.change(screen.getByTestId("find-terms-url"), {
      target: { value: "https://x.example.com/records.html" },
    });
    fireEvent.click(screen.getByTestId("find-terms-start"));
    await waitFor(() =>
      expect(api.startScan).toHaveBeenCalledWith(
        { url: "https://x.example.com/records.html" },
        ["sales"],
        new Set(["sales"]),
      ),
    );
  });

  it("scans again within the selection", async () => {
    open();
    fireEvent.click(await screen.findByTestId("find-terms-rescan-7"));
    await waitFor(() =>
      expect(api.startScan).toHaveBeenCalledWith({ document_id: 4 }, ["sales"], new Set(["sales"])),
    );
  });

  it("asks for no narrowing when every domain is checked", async () => {
    filter.checkedDomains = new Set(["sales", "hr"]);
    open();
    await waitFor(() => expect(api.fetchScannableDocuments).toHaveBeenCalledWith(null));
    expect(api.fetchScans).toHaveBeenCalledWith(null);
  });
});

describe("the requests carry the selection as repeated domains= parameters", () => {
  let urls: string[];
  beforeEach(() => {
    urls = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        urls.push(url);
        return new Response(
          JSON.stringify({ documents: [], scans: [], scan: SCAN, candidates: [] }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }),
    );
  });
  afterEach(() => vi.unstubAllGlobals());

  const query = (url: string) => new URL(url, "http://x").searchParams;

  it("term finding's reads and scans", async () => {
    const real = await vi.importActual<typeof import("../api/termFinding")>("../api/termFinding");
    const selected = new Set(["sales", ""]);
    await real.fetchScannableDocuments(selected);
    await real.fetchScans(selected, 4);
    await real.startScan({ document_id: 4 }, ["sales"], selected);
    await real.fetchScans(null);
    expect(query(urls[0]).getAll("domains")).toEqual(["sales", ""]);
    expect(query(urls[1]).getAll("domains")).toEqual(["sales", ""]);
    expect(query(urls[1]).get("document_id")).toBe("4");
    expect(query(urls[2]).getAll("domains")).toEqual(["sales", ""]);
    expect(urls[3]).not.toContain("?");
  });

  it("table search", async () => {
    const { searchSourceTables } = await import("../api/admin");
    await searchSourceTables("wh", "customers", "public", new Set(["sales"]));
    await searchSourceTables("wh", "customers", "public", null);
    expect(query(urls[0]).getAll("domains")).toEqual(["sales"]);
    expect(query(urls[0]).get("q")).toBe("customers");
    expect(query(urls[1]).getAll("domains")).toEqual([]);
  });
});
