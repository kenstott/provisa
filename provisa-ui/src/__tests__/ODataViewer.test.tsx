// Copyright (c) 2026 Kenneth Stott
// Canary: 994138c5-114b-4c05-ae9f-252886da5cf3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2193 (amended 2026-11-02): the OData explorer's viewer. Its entity sets are read from the
// role's $metadata; its query builder writes the URL of a read, runs it as the acting role and shows
// the rows, a page at a time through @odata.nextLink. REQ-1949: a read cut at the role's row limit
// says so, from the X-Provisa-Warnings header or the @Provisa.warnings annotation.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import i18n from "../i18n";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ actingRoles: "analyst" }),
}));

import { ODataPage } from "../pages/ODataPage";
import { parseMetadata } from "../pages/odata/metadata";
import {
  buildQueryUrl,
  encodeOptionValue,
  EMPTY_QUERY,
  queryString,
} from "../pages/odata/queryUrl";
import { columnsOf, readWarnings, servicePath } from "../api/odata";
import { gridRow } from "../pages/odata/answer";

const t = i18n.getFixedT("en");
const ORIGIN = window.location.origin;
const ROOT = `${ORIGIN}/data/odata`;

// The CSDL as provisa/api/odata/csdl.py writes it.
const CSDL =
  '<?xml version="1.0" encoding="utf-8"?>' +
  '<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0"><edmx:DataServices>' +
  '<Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="Provisa">' +
  '<EntityType Name="Customers"><Key><PropertyRef Name="id"/></Key>' +
  '<Property Name="id" Type="Edm.Int32" Nullable="false"/>' +
  '<Property Name="name" Type="Edm.String"/>' +
  '<Property Name="balance" Type="Edm.Decimal" Precision="10" Scale="2"/>' +
  '<NavigationProperty Name="orders" Type="Collection(Provisa.Orders)">' +
  '<ReferentialConstraint Property="id" ReferencedProperty="customer_id"/></NavigationProperty>' +
  "</EntityType>" +
  '<EntityType Name="Orders"><Key><PropertyRef Name="id"/></Key>' +
  '<Property Name="id" Type="Edm.Int64" Nullable="false"/>' +
  '<Property Name="customer_id" Type="Edm.Int32"/>' +
  '<NavigationProperty Name="customer" Type="Provisa.Customers">' +
  '<ReferentialConstraint Property="customer_id" ReferencedProperty="id"/></NavigationProperty>' +
  "</EntityType>" +
  '<EntityType Name="Events"><Property Name="at" Type="Edm.DateTimeOffset"/></EntityType>' +
  '<EntityContainer Name="Default">' +
  '<EntitySet Name="Customers" EntityType="Provisa.Customers">' +
  '<NavigationPropertyBinding Path="orders" Target="Orders"/></EntitySet>' +
  '<EntitySet Name="Orders" EntityType="Provisa.Orders">' +
  '<NavigationPropertyBinding Path="customer" Target="Customers"/></EntitySet>' +
  '<EntitySet Name="Events" EntityType="Provisa.Events"/>' +
  "</EntityContainer></Schema></edmx:DataServices></edmx:Edmx>";

const ROWS_CUT = {
  code: "statement.rows_cut",
  params: { limit: 2, kind: "role" },
  message: "The answer was cut at 2 rows.",
};

describe("parseMetadata", () => {
  const sets = parseMetadata(CSDL);

  it("reads every entity set in the container's order, with its entity type", () => {
    expect(sets.map((s) => [s.name, s.typeName])).toEqual([
      ["Customers", "Customers"],
      ["Orders", "Orders"],
      ["Events", "Events"],
    ]);
  });

  it("reads a key, and a set with none", () => {
    expect(sets[0].key).toEqual(["id"]);
    expect(sets[2].key).toEqual([]);
  });

  it("reads each property with its type, nullability, precision and scale", () => {
    expect(sets[0].properties).toEqual([
      { name: "id", type: "Edm.Int32", nullable: false, precision: null, scale: null },
      { name: "name", type: "Edm.String", nullable: true, precision: null, scale: null },
      { name: "balance", type: "Edm.Decimal", nullable: true, precision: 10, scale: "2" },
    ]);
  });

  it("reads navigation properties: collection or single, bound set, constraints", () => {
    expect(sets[0].navigations).toEqual([
      {
        name: "orders",
        targetType: "Orders",
        targetSet: "Orders",
        collection: true,
        constraints: [{ property: "id", referenced: "customer_id" }],
      },
    ]);
    expect(sets[1].navigations[0]).toMatchObject({
      name: "customer",
      targetType: "Customers",
      targetSet: "Customers",
      collection: false,
    });
  });

  it("refuses a document that is not XML", () => {
    expect(() => parseMetadata("<edmx:Edmx")).toThrow(/not well-formed/);
  });

  it("refuses a set whose entity type is not declared", () => {
    const broken = CSDL.replace('EntityType="Provisa.Events"', 'EntityType="Provisa.Missing"');
    expect(() => parseMetadata(broken)).toThrow(/Provisa.Missing is not declared/);
  });
});

describe("the query URL", () => {
  it("is the entity set alone when no option is set", () => {
    expect(buildQueryUrl(ROOT, "Customers", EMPTY_QUERY)).toBe(`${ROOT}/Customers`);
  });

  it("writes every option in one order: filter, select, expand, orderby, top, skip, count", () => {
    const url = buildQueryUrl(ROOT, "Customers", {
      filter: "id gt 3",
      select: ["id", "name"],
      expand: ["orders"],
      orderBy: [
        { property: "name", descending: false },
        { property: "id", descending: true },
      ],
      top: 10,
      skip: 20,
      count: true,
    });
    expect(url).toBe(
      `${ROOT}/Customers?$filter=id%20gt%203&$select=id,name&$expand=orders` +
        "&$orderby=name,id%20desc&$top=10&$skip=20&$count=true",
    );
  });

  it("escapes what would break the query string, keeping OData's own characters readable", () => {
    expect(encodeOptionValue("name eq 'A&B' or tag eq '#1+2=3'")).toBe(
      "name%20eq%20'A%26B'%20or%20tag%20eq%20'%231%2B2%3D3'",
    );
    expect(encodeOptionValue("startswith(name,'x/y') and at lt 2026-01-01T00:00:00Z")).toBe(
      "startswith(name,'x/y')%20and%20at%20lt%202026-01-01T00:00:00Z",
    );
  });

  it("reads back as the options written", () => {
    const filter = "name eq 'A&B+C' and price ge 1.5";
    const params = new URLSearchParams(queryString({ ...EMPTY_QUERY, filter }));
    expect(params.get("$filter")).toBe(filter);
  });

  it("leaves out a blank filter, a skip of 0 and an unset top, but keeps $top=0", () => {
    expect(queryString({ ...EMPTY_QUERY, filter: "  ", skip: 0, top: 0 })).toBe("$top=0");
  });

  it("encodes the entity set name", () => {
    expect(buildQueryUrl(ROOT, "a b", EMPTY_QUERY)).toBe(`${ROOT}/a%20b`);
  });
});

describe("reading an answer", () => {
  it("reads a nextLink from this page's own origin", () => {
    expect(servicePath("http://backend:8001/data/odata/Customers?$skip=2")).toBe(
      "/data/odata/Customers?$skip=2",
    );
    expect(() => servicePath("http://elsewhere/other/Customers")).toThrow(/not an address/);
  });

  it("takes the columns the rows carry, annotations left out", () => {
    expect(
      columnsOf([
        { "@odata.etag": "x", id: 1, orders: [], "orders@odata.nextLink": "n" },
        { id: 2, name: "b" },
      ]),
    ).toEqual(["id", "orders", "name"]);
  });

  it("shows an expanded entity as its JSON", () => {
    expect(gridRow({ id: 1, customer: { id: 7 }, orders: [{ id: 2 }], name: null })).toEqual({
      id: 1,
      customer: '{"id":7}',
      orders: '[{"id":2}]',
      name: null,
    });
  });

  it("keeps a warning said in both the header and the body once", () => {
    expect(readWarnings({ "@Provisa.warnings": [ROWS_CUT] }, [ROWS_CUT])).toEqual([ROWS_CUT]);
    expect(readWarnings({}, [ROWS_CUT])).toEqual([ROWS_CUT]);
    expect(readWarnings({ "@Provisa.warnings": [ROWS_CUT] }, [])).toEqual([ROWS_CUT]);
  });
});

// ---- the page, against a mocked service ----

type Answer = { body: unknown; status?: number; headers?: Record<string, string> };

function json({ body, status = 200, headers = {} }: Answer): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

let reads: Record<string, Answer>;
const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
  const url = String(input);
  if (url === "/data/odata/$metadata") {
    return new Response(CSDL, { status: 200, headers: { "Content-Type": "application/xml" } });
  }
  const answer = reads[url];
  if (!answer) throw new Error(`unexpected request ${url}`);
  return json(answer);
});

beforeEach(() => {
  reads = {};
  fetchMock.mockClear();
  vi.stubGlobal("fetch", fetchMock);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

async function openCustomers() {
  render(<ODataPage />);
  fireEvent.click(await screen.findByTestId("odata-set-row-Customers"));
  return screen.findByTestId("odata-set-Customers");
}

function headersOf(call: unknown[]): Record<string, string> {
  return (call[1] as RequestInit).headers as Record<string, string>;
}

describe("the OData viewer", () => {
  it("lists the role's entity sets from $metadata, read as the acting role", async () => {
    render(<ODataPage />);
    const list = await screen.findByTestId("odata-sets");
    expect(within(list).getByTestId("odata-set-row-Customers")).toBeInTheDocument();
    expect(within(list).getByTestId("odata-set-row-Orders")).toBeInTheDocument();
    expect(within(list).getByTestId("odata-set-row-Events")).toBeInTheDocument();
    expect(headersOf(fetchMock.mock.calls[0])["X-Provisa-Role"]).toBe("analyst");
  });

  it("says why when $metadata is refused", async () => {
    fetchMock.mockImplementationOnce(async () =>
      json({
        body: { error: { code: "no_surface", message: "role 'analyst' is served no data" } },
        status: 403,
      }),
    );
    render(<ODataPage />);
    expect(await screen.findByTestId("odata-metadata-error")).toHaveTextContent(
      "role 'analyst' is served no data",
    );
  });

  it("shows a set's key, typed properties and navigation properties", async () => {
    const panel = await openCustomers();
    expect(within(panel).getByTestId("odata-entity-key")).toHaveTextContent("id");
    expect(within(panel).getByTestId("odata-property-balance")).toHaveTextContent(
      "Edm.Decimal(10, 2)",
    );
    expect(within(panel).getByTestId("odata-navigation-orders")).toHaveTextContent(
      t("odataPage.navigationMany", { target: "Orders" }),
    );
  });

  it("writes the URL as the options are set, and copies it", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    const panel = await openCustomers();
    fireEvent.change(within(panel).getByTestId("odata-filter"), {
      target: { value: "name eq 'Rex'" },
    });
    fireEvent.change(within(panel).getByTestId("odata-top"), { target: { value: "5" } });
    fireEvent.click(within(panel).getByTestId("odata-count"));
    const expected = `${ROOT}/Customers?$filter=name%20eq%20'Rex'&$top=5&$count=true`;
    expect(within(panel).getByTestId("odata-url")).toHaveTextContent(expected);
    const copies = within(panel).getAllByRole("button", { name: t("odataPage.copyUrl") });
    fireEvent.click(copies[copies.length - 1]);
    expect(writeText).toHaveBeenCalledWith(expected);
  });

  it("runs the read, pages forward by @odata.nextLink and back", async () => {
    reads["/data/odata/Customers"] = {
      body: {
        "@odata.context": `${ROOT}/$metadata#Customers`,
        value: [
          { id: 1, name: "Ann" },
          { id: 2, name: "Bob" },
        ],
        // Absolute, from the host the service saw: read from this page's origin all the same.
        "@odata.nextLink": "http://backend:8001/data/odata/Customers?$skip=2",
      },
    };
    reads["/data/odata/Customers?$skip=2"] = { body: { value: [{ id: 3, name: "Cy" }] } };
    const panel = await openCustomers();
    fireEvent.click(within(panel).getByTestId("odata-run"));

    expect(await within(panel).findByTestId("odata-range")).toHaveTextContent(
      t("odataPage.rowRange", { start: 1, end: 2 }),
    );
    expect(within(panel).getByText("Ann")).toBeInTheDocument();
    expect(within(panel).getByTestId("odata-previous")).toBeDisabled();
    expect(headersOf(fetchMock.mock.calls[1])["X-Provisa-Role"]).toBe("analyst");

    fireEvent.click(within(panel).getByTestId("odata-next"));
    await waitFor(() =>
      expect(within(panel).getByTestId("odata-range")).toHaveTextContent(
        t("odataPage.rowRange", { start: 3, end: 3 }),
      ),
    );
    expect(within(panel).getByText("Cy")).toBeInTheDocument();
    expect(within(panel).getByTestId("odata-next")).toBeDisabled();

    fireEvent.click(within(panel).getByTestId("odata-previous"));
    await waitFor(() =>
      expect(within(panel).getByTestId("odata-range")).toHaveTextContent(
        t("odataPage.rowRange", { start: 1, end: 2 }),
      ),
    );
    expect(within(panel).getByText("Bob")).toBeInTheDocument();
  });

  it("gives the total with the range when the read counts", async () => {
    reads["/data/odata/Customers?$count=true"] = {
      body: { "@odata.count": 40, value: [{ id: 1 }] },
    };
    const panel = await openCustomers();
    fireEvent.click(within(panel).getByTestId("odata-count"));
    fireEvent.click(within(panel).getByTestId("odata-run"));
    expect(await within(panel).findByTestId("odata-range")).toHaveTextContent(
      t("odataPage.rowRangeOfTotal", { start: 1, end: 1, total: 40 }),
    );
  });

  it("shows the row-limit warning the header carries", async () => {
    reads["/data/odata/Customers"] = {
      body: { value: [{ id: 1 }, { id: 2 }] },
      headers: { "X-Provisa-Warnings": JSON.stringify([ROWS_CUT]) },
    };
    const panel = await openCustomers();
    fireEvent.click(within(panel).getByTestId("odata-run"));
    const warning = await within(panel).findByTestId("odata-warnings");
    expect(warning).toHaveTextContent(t("odataPage.rowsCutTitle"));
    expect(within(warning).getByTestId("odata-warning-statement.rows_cut")).toHaveTextContent(
      i18n.t("serverErrors.statement.rows_cut", ROWS_CUT.params),
    );
  });

  it("shows the row-limit warning the body annotates", async () => {
    reads["/data/odata/Customers"] = {
      body: { "@Provisa.warnings": [ROWS_CUT], value: [{ id: 1 }, { id: 2 }] },
    };
    const panel = await openCustomers();
    fireEvent.click(within(panel).getByTestId("odata-run"));
    expect(await within(panel).findByTestId("odata-warnings")).toHaveTextContent(
      t("odataPage.rowsCutTitle"),
    );
  });

  it("shows no warning on an answer that fits the limit", async () => {
    reads["/data/odata/Customers"] = { body: { value: [{ id: 1 }] } };
    const panel = await openCustomers();
    fireEvent.click(within(panel).getByTestId("odata-run"));
    await within(panel).findByTestId("odata-results");
    expect(within(panel).queryByTestId("odata-warnings")).not.toBeInTheDocument();
  });

  it("says why a read is refused, in the service's own error", async () => {
    reads["/data/odata/Customers?$filter=nope"] = {
      status: 400,
      body: { error: { code: "invalid_query_option", message: "$filter: no property nope" } },
    };
    const panel = await openCustomers();
    fireEvent.change(within(panel).getByTestId("odata-filter"), { target: { value: "nope" } });
    fireEvent.click(within(panel).getByTestId("odata-run"));
    expect(await within(panel).findByTestId("odata-error")).toHaveTextContent(
      "$filter: no property nope",
    );
  });
});
