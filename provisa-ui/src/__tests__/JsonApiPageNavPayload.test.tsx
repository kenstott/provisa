// Copyright (c) 2026 Kenneth Stott
// Canary: 81132377-e700-4c29-9d0c-dd65dd49b204
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A JSON:API URL handed to the explorer with autoRun (NL "Open in JSON:API", Polly) is applied and
// run, whether the page was just opened or was already open.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { render } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "org_admin" }, selectedRoles: [{ id: "org_admin" }] }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useDomains: () => ({ domains: [{ id: "pet-store", description: "Pet store" }] }),
  useTables: () => ({
    tables: [
      {
        id: "t1",
        domainId: "pet-store",
        tableName: "inquiries",
        columns: [{ columnName: "id" }, { columnName: "status" }, { columnName: "age" }],
      },
      { id: "t2", domainId: "pet-store", tableName: "users", columns: [{ columnName: "id" }] },
    ],
  }),
  useAllRelationships: () => ({ relationships: [] }),
}));

import { JsonApiPage } from "../pages/JsonApiPage";
import { readScreen } from "../mcp/screenState";

const runs: string[] = [];

function Polly({ url }: { url: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/jsonapi", { state: { jsonapiUrl: url, autoRun: true } })}>
      {url}
    </button>
  );
}

describe("JsonApiPage — a hand-off while the page is open", () => {
  beforeEach(() => {
    localStorage.clear();
    runs.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        if (String(url).startsWith("/data/jsonapi/")) runs.push(String(url));
        return new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
      }),
    );
  });

  it("applies and runs each handed URL", async () => {
    render(
      <>
        <Polly url="/data/jsonapi/pet-store/users" />
        <Polly url="/data/jsonapi/pet-store/inquiries" />
        <JsonApiPage />
      </>,
      { initialEntries: ["/jsonapi"] },
    );
    act(() => screen.getByText("/data/jsonapi/pet-store/users").click());
    await waitFor(() =>
      expect(runs.some((u) => u.startsWith("/data/jsonapi/pet-store/users"))).toBe(true),
    );

    act(() => screen.getByText("/data/jsonapi/pet-store/inquiries").click());
    await waitFor(() =>
      expect(runs.some((u) => u.startsWith("/data/jsonapi/pet-store/inquiries"))).toBe(true),
    );
  });
});

describe("JsonApiPage — what a handed URL carries", () => {
  const params = (u: string) => new URLSearchParams(u.split("?")[1]);

  beforeEach(() => {
    runs.length = 0;
    HTMLElement.prototype.checkVisibility = () => true;
    window.history.pushState({}, "", "/jsonapi");
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        if (String(url).startsWith("/data/jsonapi/pet-store/")) runs.push(String(url));
        return new Response(JSON.stringify({ data: [{ id: "1", type: "inquiries" }] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );
  });

  async function hand(...urls: string[]) {
    render(
      <>
        {urls.map((u) => (
          <Polly key={u} url={u} />
        ))}
        <JsonApiPage />
      </>,
      { initialEntries: ["/jsonapi"] },
    );
    await act(async () => screen.getByText(urls[0]).click());
    await waitFor(() => expect(runs.length).toBe(1));
  }

  it("runs its fields, filter and sort, and shows them in the pickers", async () => {
    await hand(
      "/data/jsonapi/pet-store/inquiries?fields[inquiries]=id,status&filter[age][gt]=3&sort=-age&page[size]=5",
    );
    const sent = params(runs[0]);
    expect(sent.get("fields[inquiries]")).toBe("id,status");
    expect(sent.get("filter[age][gt]")).toBe("3");
    expect(sent.get("sort")).toBe("-age");
    expect(sent.get("page[size]")).toBe("5");
    expect(screen.getByTestId("jsonapi-filter-value-input")).toHaveValue("3");

    const read = await readScreen(1000);
    if (typeof read === "string") throw new Error(read);
    expect(read.inputs).toMatchObject({
      fields: ["id", "status"],
      filter: { field: "age", op: "gt", value: "3" },
      sort: { field: "age", dir: "desc" },
      status: "answered",
      rows: 1,
    });
  });

  it("keeps what no picker holds in the request, and shows it", async () => {
    await hand(
      "/data/jsonapi/pet-store/inquiries?filter[age]=3&filter[status]=open&page[number]=2",
    );
    const sent = params(runs[0]);
    expect(sent.get("filter[age]")).toBe("3");
    expect(sent.get("filter[status]")).toBe("open");
    expect(sent.get("page[number]")).toBe("2");
    const shown = screen.getByTestId("jsonapi-handed-params");
    expect(shown).toHaveTextContent("filter[status]=open");
    expect(shown).toHaveTextContent("page[number]=2");
  });

  it("leaves nothing of the last request in the next one for the same table", async () => {
    const plain = "/data/jsonapi/pet-store/inquiries";
    await hand(`${plain}?filter[age]=3&sort=age&aggregate=count`, plain);
    await act(async () => screen.getByText(plain).click());
    await waitFor(() => expect(runs.length).toBe(2));
    const sent = params(runs[1]);
    expect([...sent.keys()].filter((k) => k !== "role").sort()).toEqual([
      "page[size]",
      "page[total]",
    ]);
  });
});
