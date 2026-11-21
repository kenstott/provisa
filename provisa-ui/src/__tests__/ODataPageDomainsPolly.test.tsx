// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0c7a3e-91b4-4f26-8e1d-2b6f4c8a9e17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The OData explorer follows the navbar's domain selection (each entity set names its table's domain
// in $metadata), and Polly drives it: a read handed as state.odataUrl opens its set with its options
// set, runs when autoRun asks, and read_screen reports the read, how it went and its warnings.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import { useNavigate } from "react-router-dom";
import i18n from "../i18n";

const filter = vi.hoisted(() => ({
  checked: new Set<string>(),
  ensureDomainChecked: vi.fn(),
}));

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ actingRoles: "analyst" }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    checkedDomains: filter.checked,
    ensureDomainChecked: filter.ensureDomainChecked,
  }),
}));

import { ODataPage } from "../pages/ODataPage";
import { parseMetadata, setsInDomains } from "../pages/odata/metadata";
import { HandoffRefused, parseQueryUrl, EMPTY_QUERY } from "../pages/odata/queryUrl";
import { readScreen } from "../mcp/screenState";

const t = i18n.getFixedT("en");

// The CSDL as provisa/api/odata/csdl.py writes it: each entity set names its table's domain.
const CSDL =
  '<?xml version="1.0" encoding="utf-8"?>' +
  '<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0"><edmx:DataServices>' +
  '<Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="Provisa">' +
  '<Term Name="Domain" Type="Edm.String" AppliesTo="EntitySet"/>' +
  '<EntityType Name="Orders"><Key><PropertyRef Name="id"/></Key>' +
  '<Property Name="id" Type="Edm.Int64" Nullable="false"/>' +
  '<Property Name="amount" Type="Edm.Decimal"/>' +
  '<NavigationProperty Name="customer" Type="Provisa.Customers"/>' +
  "</EntityType>" +
  '<EntityType Name="Customers"><Key><PropertyRef Name="id"/></Key>' +
  '<Property Name="id" Type="Edm.Int32" Nullable="false"/></EntityType>' +
  '<EntityType Name="OpRuns"><Property Name="at" Type="Edm.DateTimeOffset"/></EntityType>' +
  '<EntityContainer Name="Default">' +
  '<EntitySet Name="orders" EntityType="Provisa.Orders">' +
  '<NavigationPropertyBinding Path="customer" Target="customers"/>' +
  '<Annotation Term="Provisa.Domain" String="sales"/></EntitySet>' +
  '<EntitySet Name="customers" EntityType="Provisa.Customers">' +
  '<Annotation Term="Provisa.Domain" String="sales"/></EntitySet>' +
  '<EntitySet Name="op_runs" EntityType="Provisa.OpRuns">' +
  '<Annotation Term="Provisa.Domain" String="ops"/></EntitySet>' +
  "</EntityContainer></Schema></edmx:DataServices></edmx:Edmx>";

const ROWS_CUT = {
  code: "statement.rows_cut",
  params: { limit: 2, kind: "role" },
  message: "The answer was cut at 2 rows.",
};

let reads: Record<string, unknown>;
const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
  const url = String(input);
  if (url === "/data/odata/$metadata") {
    return new Response(CSDL, { status: 200, headers: { "Content-Type": "application/xml" } });
  }
  if (!(url in reads)) throw new Error(`unexpected request ${url}`);
  return new Response(JSON.stringify(reads[url]), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
});

beforeEach(() => {
  reads = {};
  filter.checked = new Set();
  filter.ensureDomainChecked.mockClear();
  fetchMock.mockClear();
  vi.stubGlobal("fetch", fetchMock);
  HTMLElement.prototype.checkVisibility = () => true;
  window.history.pushState({}, "", "/odata");
});
afterEach(() => {
  vi.unstubAllGlobals();
});

async function inputs() {
  const read = await readScreen(1000);
  if (typeof read === "string") throw new Error(read);
  return read.inputs;
}

function Polly({ label, state }: { label: string; state: object }) {
  const navigate = useNavigate();
  return <button onClick={() => navigate("/odata", { state })}>{label}</button>;
}

describe("the entity sets' domains", () => {
  it("are read from each set's Provisa.Domain annotation", () => {
    expect(parseMetadata(CSDL).map((s) => [s.name, s.domain])).toEqual([
      ["orders", "sales"],
      ["customers", "sales"],
      ["op_runs", "ops"],
    ]);
  });

  it("are null for a set that names none", () => {
    const bare = CSDL.replace('<Annotation Term="Provisa.Domain" String="ops"/>', "");
    expect(parseMetadata(bare).find((s) => s.name === "op_runs")?.domain).toBeNull();
  });

  it("narrow the sets to the selection; an empty selection and an unnamed domain hide nothing", () => {
    const sets = parseMetadata(CSDL);
    expect(setsInDomains(sets, new Set(["sales"])).map((s) => s.name)).toEqual([
      "orders",
      "customers",
    ]);
    expect(setsInDomains(sets, new Set())).toHaveLength(3);
    const unnamed = sets.map((s) => ({ ...s, domain: null }));
    expect(setsInDomains(unnamed, new Set(["sales"]))).toHaveLength(3);
  });
});

describe("the page under a domain selection", () => {
  it("lists only the sets of the selected domains", async () => {
    filter.checked = new Set(["sales"]);
    render(<ODataPage />, { initialEntries: ["/odata"] });
    const list = await screen.findByTestId("odata-sets");
    expect(await within(list).findByTestId("odata-set-row-orders")).toBeInTheDocument();
    expect(within(list).getByTestId("odata-set-row-customers")).toBeInTheDocument();
    expect(within(list).queryByTestId("odata-set-row-op_runs")).not.toBeInTheDocument();
  });

  it("lists the ops sets once ops is selected", async () => {
    filter.checked = new Set(["ops"]);
    render(<ODataPage />, { initialEntries: ["/odata"] });
    expect(await screen.findByTestId("odata-set-row-op_runs")).toBeInTheDocument();
    expect(screen.queryByTestId("odata-set-row-orders")).not.toBeInTheDocument();
  });
});

describe("parseQueryUrl", () => {
  it("reads the set and every option the builder holds", () => {
    const read = parseQueryUrl(
      "/data/odata/orders?$filter=amount%20gt%205&$select=id,amount&$expand=customer" +
        "&$orderby=amount desc,id&$top=10&$skip=20&$count=true",
      "/data/odata",
    );
    expect(read).toEqual({
      set: "orders",
      query: {
        filter: "amount gt 5",
        select: ["id", "amount"],
        expand: ["customer"],
        orderBy: [
          { property: "amount", descending: true },
          { property: "id", descending: false },
        ],
        top: 10,
        skip: 20,
        count: true,
      },
    });
  });

  it("reads an absolute URL and a bare set", () => {
    expect(parseQueryUrl(`${window.location.origin}/data/odata/orders`, "/data/odata")).toEqual({
      set: "orders",
      query: EMPTY_QUERY,
    });
  });

  it.each([
    ["/data/rest/orders", "odataPage.handoffNotService"],
    ["/data/odata/orders(1)", "odataPage.handoffPath"],
    ["/data/odata/orders/customer", "odataPage.handoffPath"],
    ["/data/odata/$metadata", "odataPage.handoffPath"],
    ["/data/odata/orders?$top=many", "odataPage.handoffNumber"],
    ["/data/odata/orders?$expand=customer($select=id)", "odataPage.handoffOption"],
    ["/data/odata/orders?$search=x", "odataPage.handoffOption"],
    ["/data/odata/orders?$count=yes", "odataPage.handoffOption"],
    ["/data/odata/orders?$orderby=amount sideways", "odataPage.handoffOption"],
  ])("refuses %s", (url, key) => {
    expect(() => parseQueryUrl(url, "/data/odata")).toThrow(HandoffRefused);
    try {
      parseQueryUrl(url, "/data/odata");
    } catch (e) {
      expect((e as HandoffRefused).key).toBe(key);
    }
  });
});

describe("Polly on the OData page", () => {
  it("opens a handed read with its options set, runs it, and reads it back", async () => {
    const handed = "/data/odata/orders?$filter=amount gt 5&$select=id,amount&$top=2&$count=true";
    reads["/data/odata/orders?$filter=amount%20gt%205&$select=id,amount&$top=2&$count=true"] = {
      value: [
        { id: 1, amount: 9 },
        { id: 2, amount: 7 },
      ],
      "@odata.count": 5,
      "@odata.nextLink": "http://backend/data/odata/orders?$skip=2",
      "@Provisa.warnings": [ROWS_CUT],
    };
    render(
      <>
        <Polly label="go" state={{ odataUrl: handed, autoRun: true }} />
        <ODataPage />
      </>,
      { initialEntries: ["/odata"] },
    );
    await screen.findByTestId("odata-set-row-orders");
    act(() => screen.getByText("go").click());

    const panel = await screen.findByTestId("odata-set-orders");
    expect(within(panel).getByTestId("odata-filter")).toHaveValue("amount gt 5");
    expect(within(panel).getByTestId("odata-top")).toHaveValue("2");
    expect(within(panel).getByTestId("odata-count")).toBeChecked();
    expect(await within(panel).findByTestId("odata-warnings")).toBeInTheDocument();

    await waitFor(async () => expect(await inputs()).toMatchObject({ status: "answered" }));
    expect(await inputs()).toMatchObject({
      entitySets: ["orders", "customers", "op_runs"],
      set: "orders",
      filter: "amount gt 5",
      select: ["id", "amount"],
      top: 2,
      count: true,
      status: "answered",
      error: null,
      rows: 2,
      total: 5,
      nextPage: true,
      warnings: [{ code: "statement.rows_cut", message: ROWS_CUT.message }],
      handoffError: null,
    });
  });

  it("sets the options without running when autoRun is not asked", async () => {
    render(
      <>
        <Polly label="go" state={{ odataUrl: "/data/odata/customers?$top=3" }} />
        <ODataPage />
      </>,
      { initialEntries: ["/odata"] },
    );
    await screen.findByTestId("odata-set-row-customers");
    act(() => screen.getByText("go").click());
    const panel = await screen.findByTestId("odata-set-customers");
    expect(within(panel).getByTestId("odata-top")).toHaveValue("3");
    expect(await inputs()).toMatchObject({ set: "customers", top: 3, status: "idle" });
    expect(fetchMock.mock.calls.map((c) => String(c[0]))).toEqual(["/data/odata/$metadata"]);
  });

  it("applies each hand-off while the page is open", async () => {
    reads["/data/odata/customers"] = { value: [{ id: 1 }] };
    reads["/data/odata/orders?$top=1"] = { value: [{ id: 4, amount: 1 }] };
    render(
      <>
        <Polly label="one" state={{ odataUrl: "/data/odata/customers", autoRun: true }} />
        <Polly label="two" state={{ odataUrl: "/data/odata/orders?$top=1", autoRun: true }} />
        <ODataPage />
      </>,
      { initialEntries: ["/odata"] },
    );
    await screen.findByTestId("odata-set-row-orders");
    act(() => screen.getByText("one").click());
    await screen.findByTestId("odata-set-customers");
    act(() => screen.getByText("two").click());
    await screen.findByTestId("odata-set-orders");
    await waitFor(() =>
      expect(fetchMock.mock.calls.map((c) => String(c[0]))).toContain("/data/odata/orders?$top=1"),
    );
  });

  it("shows a set of a domain the selection hides", async () => {
    filter.checked = new Set(["sales"]);
    render(
      <>
        <Polly label="go" state={{ odataUrl: "/data/odata/op_runs" }} />
        <ODataPage />
      </>,
      { initialEntries: ["/odata"] },
    );
    await screen.findByTestId("odata-set-row-orders");
    act(() => screen.getByText("go").click());
    await waitFor(() => expect(filter.ensureDomainChecked).toHaveBeenCalledWith("ops"));
  });

  it.each([
    ["/data/odata/nothing", () => t("odataPage.handoffNoSet", { set: "nothing" })],
    [
      "/data/odata/orders?$select=colour",
      () => t("odataPage.handoffMember", { set: "orders", option: "$select", name: "colour" }),
    ],
    [
      "/data/odata/orders?$apply=groupby((id))",
      () => t("odataPage.handoffOption", { option: "$apply", value: "groupby((id))" }),
    ],
  ])("says why it cannot set %s, on the page and to read_screen", async (url, message) => {
    render(
      <>
        <Polly label="go" state={{ odataUrl: url, autoRun: true }} />
        <ODataPage />
      </>,
      { initialEntries: ["/odata"] },
    );
    await screen.findByTestId("odata-set-row-orders");
    act(() => screen.getByText("go").click());
    expect(await screen.findByTestId("odata-handoff-error")).toHaveTextContent(message());
    expect(await inputs()).toMatchObject({ handoffError: message() });
    expect(screen.queryByTestId("odata-set-orders")).not.toBeInTheDocument();
  });

  it("is reported while no set is open", async () => {
    render(<ODataPage />, { initialEntries: ["/odata"] });
    await screen.findByTestId("odata-set-row-orders");
    fireEvent.click(screen.getByTestId("odata-set-row-orders"));
    await screen.findByTestId("odata-set-orders");
    expect(await inputs()).toMatchObject({ set: "orders", status: "idle" });
  });
});
