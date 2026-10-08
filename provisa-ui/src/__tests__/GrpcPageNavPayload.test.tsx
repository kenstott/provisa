// Copyright (c) 2026 Kenneth Stott
// Canary: 6b9e3937-160c-45df-bd5c-531cbb8ab7aa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A gRPC method handed to the gRPC explorer with autoRun (NL "Open in gRPC", Polly) is selected and
// run, whether the page was just opened or was already open.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";

vi.mock("../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "org_admin" }, selectedRoles: [{ id: "org_admin" }] }) }));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));

import { GrpcPage } from "../pages/GrpcPage";

// CodeMirror measures text with Range.getClientRects, which jsdom does not implement.
Range.prototype.getClientRects = () => [] as unknown as DOMRectList;
Range.prototype.getBoundingClientRect = () => new DOMRect();

const PROTO = `syntax = "proto3";
package provisa.v1;
service ProvisaService {
  rpc QueryOrders(OrdersRequest) returns (stream Orders);
  rpc QueryCustomers(CustomersRequest) returns (stream Customers);
}
message OrdersRequest { int32 limit = 1; }
message Orders { int32 id = 1; }
message CustomersRequest { int32 limit = 1; }
message Customers { int32 id = 1; }
`;

const runs: string[] = [];

function Polly() {
  const navigate = useNavigate();
  return (
    <button
      onClick={() => navigate("/grpc", { state: { grpcMethod: "QueryCustomers", autoRun: true } })}
    >
      polly
    </button>
  );
}

describe("GrpcPage — a hand-off while the page is open", () => {
  beforeEach(() => {
    runs.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        const u = String(url);
        if (u.startsWith("/data/proto/")) return new Response(PROTO, { status: 200 });
        if (u.startsWith("/data/grpc/")) runs.push(u);
        return new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
      }),
    );
  });

  it("selects and runs the handed method", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/grpc"]}>
          <Polly />
          <GrpcPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    await waitFor(() => expect(screen.getAllByText(/Orders/).length).toBeGreaterThan(0));
    const before = runs.length;

    act(() => screen.getByText("polly").click());
    await waitFor(() =>
      expect(runs.slice(before).some((u) => u === "/data/grpc/Customers")).toBe(true),
    );
  });
});
