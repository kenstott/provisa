// Copyright (c) 2026 Kenneth Stott
// Canary: 9d2c7a51-6e38-4b0f-8c14-e5a3f7b9d026
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The gRPC explorer acts as the ACTIVE roles (REQ-1620): under "Role: All" every request names the
// whole set — in the path of the routes that take a role there, and in X-Provisa-Role — and a
// refused request shows the server's own reason, not one fixed sentence.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";

const auth = vi.hoisted(() => ({ selectedRoles: [{ id: "analyst" }, { id: "org_admin" }] }));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: auth.selectedRoles[0], selectedRoles: auth.selectedRoles }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>(["pets", "ops"]) }),
}));

import "../i18n";
import { GrpcPage } from "../pages/GrpcPage";

// CodeMirror measures text with Range.getClientRects, which jsdom does not implement.
Range.prototype.getClientRects = () => [] as unknown as DOMRectList;
Range.prototype.getBoundingClientRect = () => new DOMRect();

const PROTO = `syntax = "proto3";
package provisa.v1;
service ProvisaService {
  rpc QueryOrders(OrdersRequest) returns (stream Orders);
  rpc QueryOrdersGroupBy(OrdersGroupByRequest) returns (stream OrdersGroupBy);
}
message OrdersRequest { int32 limit = 1; }
message Orders { int32 id = 1; }
message OrdersGroupByRequest { repeated string by = 1; }
message OrdersGroupBy { int32 count = 1; }
`;

interface Call {
  url: string;
  role: string | null;
  body: Record<string, unknown> | null;
}
const calls: Call[] = [];
let protoResponse: () => Response;

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function renderPage() {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/grpc"]}>
        <GrpcPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

beforeEach(() => {
  calls.length = 0;
  auth.selectedRoles = [{ id: "analyst" }, { id: "org_admin" }];
  protoResponse = () => new Response(PROTO, { status: 200 });
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const u = String(url);
      const headers = new Headers(init?.headers);
      calls.push({
        url: u,
        role: headers.get("x-provisa-role"),
        body: init?.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : null,
      });
      if (u.startsWith("/data/proto/")) return protoResponse();
      return json(200, []);
    }),
  );
});

const SET = encodeURIComponent("analyst,org_admin");

describe("GrpcPage — several active roles", () => {
  it("names the whole set for the proto, the commands and a query", async () => {
    renderPage();
    await waitFor(() => expect(screen.getByTestId("grpc-send-btn")).not.toBeDisabled());
    const urls = calls.map((c) => c.url);
    expect(urls).toContain(`/data/proto/${SET}?domains=${encodeURIComponent("pets,ops")}`);
    expect(urls).toContain(`/data/grpc-commands/${SET}`);
    expect(urls.some((u) => /\/analyst([?/]|$)/.test(u))).toBe(false);

    screen.getByTestId("grpc-send-btn").click();
    await waitFor(() => expect(calls.some((c) => c.url === "/data/grpc/Orders")).toBe(true));
    const run = calls.find((c) => c.url === "/data/grpc/Orders")!;
    expect(run.role).toBe("analyst,org_admin");
    // The acting role travels only in the header: the server refuses a body role that differs.
    expect(run.body).not.toHaveProperty("role_id");
    expect(run.body).not.toHaveProperty("role");
  });

  it("names one role when one is active", async () => {
    auth.selectedRoles = [{ id: "analyst" }];
    renderPage();
    await waitFor(() => expect(screen.getByTestId("grpc-send-btn")).not.toBeDisabled());
    expect(calls.map((c) => c.url)).toContain(
      `/data/proto/analyst?domains=${encodeURIComponent("pets,ops")}`,
    );
  });
});

describe("GrpcPage — a refused proto request says why", () => {
  it.each([
    [403, "auth.role_not_assigned", { role_id: "org_admin" }, /org_admin.*not assigned/i],
    [403, "data.domain_not_accessible", { role_id: "analyst", domain: "ops" }, /analyst.*ops/],
    [404, "data.no_proto_for_role", { role_id: "analyst" }, /analyst.*no proto/i],
    [503, "data.schema_cache_not_ready", {}, /not ready/i],
  ])("%i %s", async (status, code, params, shown) => {
    protoResponse = () => json(status, { detail: "the English detail", code, params });
    renderPage();
    const error = await screen.findByTestId("grpc-proto-error");
    expect(error.textContent).toMatch(shown);
    expect(error.textContent).not.toMatch(/schema not yet built/);
  });

  it("shows the server's detail for a refusal with no catalog code", async () => {
    protoResponse = () => json(403, { detail: "'meta:a+b' is not a role" });
    renderPage();
    expect((await screen.findByTestId("grpc-proto-error")).textContent).toBe(
      "'meta:a+b' is not a role",
    );
  });

  it("shows the status of a refusal that is not a JSON error", async () => {
    protoResponse = () => new Response("<html>bad gateway</html>", { status: 502 });
    renderPage();
    expect((await screen.findByTestId("grpc-proto-error")).textContent).toMatch(/502/);
  });
});
