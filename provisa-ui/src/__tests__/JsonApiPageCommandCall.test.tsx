// Copyright (c) 2026 Kenneth Stott
// Canary: 9d41c7e2-5a08-4b3f-86e1-0c72f9a4d3b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): a command's call handed to the JSON:API Explorer by its Query it
// line is selected with its arguments filled: a read runs at once as a GET with each argument a
// query parameter, a write waits for Run and is sent as a POST with the arguments as the body's meta.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "org_admin" }, actingRoles: "org_admin" }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [] }),
  useTables: () => ({ tables: [] }),
  useAllRelationships: () => ({ relationships: [] }),
}));

import { JsonApiPage } from "../pages/JsonApiPage";
import { readScreen } from "../mcp/screenState";

interface Sent {
  url: string;
  method: string;
  body: unknown;
  role: string | null;
}
const sent: Sent[] = [];

function page(state: Record<string, unknown>) {
  render(<JsonApiPage />, { initialEntries: [{ pathname: "/jsonapi", state }] });
}

describe("JsonApiPage — a command's call handed over", () => {
  beforeEach(() => {
    HTMLElement.prototype.checkVisibility = () => true;
    window.history.pushState({}, "", "/jsonapi");
    sent.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        if (String(url).startsWith("/data/jsonapi/")) {
          const headers = new Headers(init?.headers);
          sent.push({
            url: String(url),
            method: init?.method ?? "GET",
            body: init?.body ? JSON.parse(String(init.body)) : null,
            role: headers.get("x-provisa-role"),
          });
        }
        return new Response(JSON.stringify({ meta: { rows: [{ id: 1, score: 2.5 }] } }), {
          status: 200,
          headers: { "content-type": "application/vnd.api+json" },
        });
      }),
    );
  });

  it("runs a read at once as a GET with each argument a query parameter", async () => {
    page({
      jsonapiCommand: "enrich",
      jsonapiDomain: "derived",
      jsonapiWrite: false,
      jsonapiArgs: { label: "it's", factor: 1.5 },
      autoRun: true,
    });
    await waitFor(() => expect(sent).toHaveLength(1));
    expect(sent[0].method).toBe("GET");
    const [path, query] = sent[0].url.split("?");
    expect(path).toBe("/data/jsonapi/derived/commands/enrich");
    expect(Object.fromEntries(new URLSearchParams(query))).toEqual({
      label: "it's",
      factor: "1.5",
    });
    expect(sent[0].role).toBe("org_admin");
    await waitFor(() =>
      expect(screen.getByTestId("jsonapi-command-rows")).toHaveTextContent("2.5"),
    );
    expect(screen.getByTestId("jsonapi-command-arg-label")).toHaveValue("it's");

    const read = await readScreen(1000);
    if (typeof read === "string") throw new Error(read);
    expect(read.inputs?.command).toMatchObject({
      name: "enrich",
      method: "GET",
      arguments: { label: "it's", factor: "1.5" },
      status: "answered",
    });
  });

  it("selects a write's call and leaves it for Run, then POSTs the arguments as meta", async () => {
    page({
      jsonapiCommand: "refund_order",
      jsonapiDomain: "sales",
      jsonapiWrite: true,
      jsonapiArgs: { order_id: 7, reason: "late" },
      autoRun: false,
    });
    await waitFor(() =>
      expect(screen.getByTestId("jsonapi-command-url")).toHaveTextContent(
        "/data/jsonapi/sales/commands/refund_order",
      ),
    );
    expect(screen.getByText("POST")).toBeInTheDocument();
    expect(sent).toEqual([]);

    fireEvent.change(screen.getByTestId("jsonapi-command-arg-order_id"), {
      target: { value: "8" },
    });
    fireEvent.click(screen.getByTestId("jsonapi-run-button"));
    await waitFor(() => expect(sent).toHaveLength(1));
    expect(sent[0]).toMatchObject({
      url: "/data/jsonapi/sales/commands/refund_order",
      method: "POST",
      body: { meta: { order_id: 8, reason: "late" } },
    });
  });

  it("refuses a write whose JSON argument is not JSON, sending nothing", async () => {
    page({
      jsonapiCommand: "refund_order",
      jsonapiDomain: "sales",
      jsonapiWrite: true,
      jsonapiArgs: { order_id: 7 },
      autoRun: false,
    });
    const field = await screen.findByTestId("jsonapi-command-arg-order_id");
    fireEvent.change(field, { target: { value: "{" } });
    fireEvent.click(screen.getByTestId("jsonapi-run-button"));
    expect(await screen.findByText("order_id is not JSON")).toBeInTheDocument();
    expect(sent).toEqual([]);
  });

  it("returns to the table explorer", async () => {
    page({ jsonapiCommand: "enrich", jsonapiDomain: "derived", jsonapiArgs: {}, autoRun: false });
    fireEvent.click(await screen.findByTestId("jsonapi-command-close"));
    expect(await screen.findByTestId("jsonapi-table-select")).toBeInTheDocument();
  });
});
