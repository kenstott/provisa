// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0e6f3a-81c7-4b29-96fd-e24a7b1c3d08
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Polly's navigate reads the screen back before it answers: a page handed state has taken it only
// when the screen shows it, and a page that dropped it is a failure, never "Navigated".

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { readBack } from "../mcp/readBack";
import { useScreenEditor, useScreenInputs, type ScreenInputs } from "../mcp/screenState";

function Inputs({ route, inputs }: { route: string; inputs: ScreenInputs }) {
  useScreenInputs(route, inputs);
  return null;
}

function Editor({ route, text }: { route: string; text: string }) {
  useScreenEditor(route, { language: "sql", text, tab: null });
  return null;
}

/** The page on `route`, publishing `inputs`. */
function open(route: string, inputs: ScreenInputs) {
  window.history.pushState({}, "", route);
  return render(<Inputs route={route} inputs={inputs} />);
}

const WAIT = 150;

beforeEach(() => {
  HTMLElement.prototype.checkVisibility = () => true;
});
afterEach(() => cleanup());

describe("read-back of an editor", () => {
  it("reads as shown once the editor holds what was handed, laid out as it likes", async () => {
    window.history.pushState({}, "", "/sparql");
    render(<Editor route="/sparql" text={"SELECT ?s\nWHERE { ?s ?p ?o }"} />);
    const out = await readBack("/sparql", { query: "SELECT ?s WHERE { ?s ?p ?o }" }, WAIT);
    expect(out.shown).toBe(true);
  });

  it("waits for the page to take the state", async () => {
    window.history.pushState({}, "", "/sql");
    const { rerender } = render(<Editor route="/sql" text="SELECT 0" />);
    const reading = readBack("/sql", { sql: "SELECT 1" }, 2000);
    await act(async () => {
      await new Promise((r) => setTimeout(r, 150));
      rerender(<Editor route="/sql" text="SELECT 1" />);
    });
    expect((await reading).shown).toBe(true);
  });

  it("names what the editor shows when it never takes the state", async () => {
    window.history.pushState({}, "", "/sql");
    render(<Editor route="/sql" text="SELECT 0" />);
    const out = await readBack("/sql", { sql: "SELECT 1" }, WAIT);
    expect(out).toMatchObject({
      shown: false,
      difference: 'the editor shows "SELECT 0", not the sql handed to it',
    });
  });

  it("says so when the page never loads", async () => {
    window.history.pushState({}, "", "/query");
    const out = await readBack("/query", { query: "{ a }" }, WAIT);
    expect(out).toMatchObject({ shown: false, screen: null });
  });

  it("has nothing to read back on a route with no check", async () => {
    window.history.pushState({}, "", "/somewhere-else");
    expect(await readBack("/sources", { view: "x" }, WAIT)).toEqual({ shown: true, screen: null });
  });
});

describe("read-back of the gRPC page", () => {
  const body = { filter: { id: 1 }, limit: 5, offset: 0, read_mask: { paths: ["id", "name"] } };
  const answered = {
    method: "QueryOrders",
    body: JSON.stringify(body, null, 2),
    status: "answered",
    error: null,
  };

  it("reads as shown when the method and body are on screen, whatever the key order", async () => {
    open("/grpc", answered);
    const handed = { read_mask: body.read_mask, offset: 0, limit: 5, filter: body.filter };
    const out = await readBack("/grpc", { grpcMethod: "QueryOrders()", grpcBody: handed }, WAIT);
    expect(out.shown).toBe(true);
  });

  it("refuses a method the page did not select: it falls to the first method", async () => {
    open("/grpc", { ...answered, method: "QueryUsers" });
    const out = await readBack("/grpc", { grpcMethod: "QueryOrders()" }, WAIT);
    expect(out).toMatchObject({
      shown: false,
      difference: "the page has QueryUsers selected, not QueryOrders",
    });
  });

  it("refuses a body the page does not show", async () => {
    open("/grpc", answered);
    const handed = { ...body, limit: 6 };
    const out = await readBack("/grpc", { grpcMethod: "QueryOrders", grpcBody: handed }, WAIT);
    expect(out.shown).toBe(false);
  });

  it("with autoRun, reads as shown only once the page has answered", async () => {
    open("/grpc", { ...answered, status: "running" });
    const out = await readBack("/grpc", { grpcMethod: "QueryOrders", autoRun: true }, WAIT);
    expect(out).toMatchObject({ shown: false, difference: "the page has not answered yet" });
  });

  it("with autoRun, reports the error the page shows", async () => {
    open("/grpc", { ...answered, status: "failed", error: "Invalid read_mask path 'x'" });
    const out = await readBack("/grpc", { grpcMethod: "QueryOrders", autoRun: true }, WAIT);
    expect(out).toMatchObject({
      shown: false,
      difference: "the page shows this error: Invalid read_mask path 'x'",
    });
  });
});

describe("read-back of the JSON:API page", () => {
  const url =
    "/data/jsonapi/shop/orders?role=admin&filter%5Bstatus%5D=open&sort=-total&page%5Bsize%5D=5&fields%5Borders%5D=id%2Ctotal";
  const inputs = { url, fields: ["id", "total"], status: "idle", error: null };

  it("reads as shown when the request carries every handed parameter", async () => {
    open("/jsonapi", inputs);
    const jsonapiUrl =
      "/data/jsonapi/shop/orders?fields[orders]=total,id&filter[status]=open&sort=-total&page[size]=5";
    expect((await readBack("/jsonapi", { jsonapiUrl }, WAIT)).shown).toBe(true);
  });

  it("names a parameter the request lost", async () => {
    open("/jsonapi", inputs);
    const jsonapiUrl = "/data/jsonapi/shop/orders?filter[region]=EU";
    const out = await readBack("/jsonapi", { jsonapiUrl }, WAIT);
    expect(out).toMatchObject({
      shown: false,
      difference: "the page's request does not carry filter[region]=EU",
    });
  });

  it("refuses another table", async () => {
    open("/jsonapi", inputs);
    const out = await readBack("/jsonapi", { jsonapiUrl: "/data/jsonapi/shop/users" }, WAIT);
    expect(out.shown).toBe(false);
  });

  it("counts every column selected as the fields handed: the URL then names none", async () => {
    open("/jsonapi", { ...inputs, url: "/data/jsonapi/shop/orders?role=admin" });
    const jsonapiUrl = "/data/jsonapi/shop/orders?fields[orders]=id,total";
    expect((await readBack("/jsonapi", { jsonapiUrl }, WAIT)).shown).toBe(true);
  });
});

describe("read-back of the OpenAPI page", () => {
  const inputs = {
    operation: "GET /shop/orders",
    parameters: { limit: "5", fields: "id,total", filter: '[{"status":{"eq":"open"}}]' },
    body: null,
    status: "answered",
    error: null,
  };
  const openApiUrl =
    'GET /data/rest/shop/orders?limit=5&fields=id,total&filter=[{"status":{"eq":"open"}}]';

  it("reads as shown when the operation carries every handed parameter", async () => {
    open("/openapi", inputs);
    const out = await readBack("/openapi", { openApiUrl: openApiUrl.replaceAll('"', "%22") }, WAIT);
    expect(out.shown).toBe(true);
  });

  it("refuses when another operation is open", async () => {
    open("/openapi", { ...inputs, operation: "GET /shop/users" });
    const out = await readBack("/openapi", { openApiUrl: "GET /data/rest/shop/orders" }, WAIT);
    expect(out.shown).toBe(false);
  });

  it("refuses when no operation is open", async () => {
    open("/openapi", { status: "idle", error: null });
    const out = await readBack("/openapi", { openApiUrl: "GET /data/rest/shop/orders" }, WAIT);
    expect(out.shown).toBe(false);
  });

  it("reports why the page could not carry the call out", async () => {
    open("/openapi", { status: "failed", error: "GET /x has no parameter region" });
    const out = await readBack("/openapi", { openApiUrl: "GET /x?region=EU" }, WAIT);
    expect(out).toMatchObject({
      shown: false,
      difference: "the page shows this error: GET /x has no parameter region",
    });
  });

  it("refuses a body the page does not show", async () => {
    open("/openapi", { ...inputs, operation: "POST /shop/commands/ship", body: '{"id":1}' });
    const out = await readBack(
      "/openapi",
      { openApiUrl: "POST /data/rest/shop/commands/ship", openApiBody: { id: 2 } },
      WAIT,
    );
    expect(out.shown).toBe(false);
  });
});

describe("navigate", () => {
  it("answers with the failure, never Navigated, when the page did not take its state", async () => {
    vi.resetModules();
    vi.doMock("../mcp/readBack", () => ({
      readBack: async () => ({ shown: false, difference: "no such method", screen: null }),
    }));
    const { executeClientTool } = await import("../mcp/clientTools");
    const navigate = vi.fn();
    const ctx = {
      navigate,
      confirm: vi.fn(),
      runMutation: vi.fn(),
      presentChoice: vi.fn(),
      runAdminMutation: vi.fn(),
      chooseFile: vi.fn(),
    };
    const out = await executeClientTool(
      "navigate",
      { route: "/grpc", state: { grpcMethod: "QueryNope()" } },
      ctx,
    );
    expect(out).toEqual({
      success: false,
      message: "navigate: /grpc did not take what it was handed: no such method.",
    });
    expect(navigate).toHaveBeenCalledOnce();
    vi.doUnmock("../mcp/readBack");
  });
});

describe("read-back of a page that keeps its state in its URL", () => {
  it("reads as shown when the page found what the link names", async () => {
    open("/tables", { view: "erd", table: 7, domains: ["shop"], objects: [], collapsed: [] });
    window.history.pushState({}, "", "/tables?view=erd&table=7&domains=shop");
    const out = await readBack("/tables?view=erd&table=7&domains=shop", {}, WAIT);
    expect(out.shown).toBe(true);
  });

  it("names a table the page did not open", async () => {
    open("/tables", { view: "erd", table: null });
    const out = await readBack("/tables?view=erd&table=7", {}, WAIT);
    expect(out).toMatchObject({ shown: false, difference: "the page shows table none, not 7" });
  });

  it("names domains that did not collapse", async () => {
    open("/tables", { view: "erd", collapsed: ["shop"] });
    const out = await readBack("/tables?view=erd&collapsed=all", {}, WAIT);
    expect(out).toMatchObject({ shown: false, difference: "the page shows collapsed shop, not all" });
  });

  it("reads a data product the list does not hold as not opened", async () => {
    open("/data-products", { product: null });
    const out = await readBack("/data-products?product=nope", {}, WAIT);
    expect(out).toMatchObject({ shown: false, difference: "the page shows product none, not nope" });
  });

  it("reads the ontology graph's controls back", async () => {
    const graph = { flat: true, layout: "layered" };
    open("/admin/ontology", { panel: "graph", term: null, graph });
    const state = { panel: "graph", graph: { flat: true } };
    expect((await readBack("/admin/ontology", state, WAIT)).shown).toBe(true);
    const out = await readBack("/admin/ontology", { graph: { flat: false } }, WAIT);
    expect(out).toMatchObject({ shown: false, difference: "the graph shows flat true, not false" });
  });

  it("reads a term the glossary does not hold as not opened", async () => {
    open("/admin/ontology", { panel: "terms", term: 9, termName: null });
    const out = await readBack("/admin/ontology", { term: 9 }, WAIT);
    expect(out).toMatchObject({ shown: false, difference: "the glossary has no term 9" });
  });

  it("merges what several components publish for one route", async () => {
    window.history.pushState({}, "", "/tables");
    render(
      <>
        <Inputs route="/tables" inputs={{ view: "erd" }} />
        <Inputs route="/tables" inputs={{ collapsed: "all" }} />
      </>,
    );
    const out = await readBack("/tables?view=erd&collapsed=all", {}, WAIT);
    expect(out.shown).toBe(true);
    expect(out.screen?.inputs).toEqual({ view: "erd", collapsed: "all" });
  });
});
