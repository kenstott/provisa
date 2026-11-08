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

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "org_admin" },
    selectedRoles: [{ id: "org_admin" }],
    actingRoles: "org_admin",
  }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));

import { GrpcPage } from "../pages/GrpcPage";
import { readScreen } from "../mcp/screenState";

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
    // REQ-2010: the response is in, marked for a demo tour step that waits on it.
    await waitFor(() =>
      expect(document.querySelector(".grpc-panel-right[data-answered]")).not.toBeNull(),
    );
  });
});

describe("GrpcPage — a body handed with the method", () => {
  const sent: unknown[] = [];

  function Hand({ label, state }: { label: string; state: Record<string, unknown> }) {
    const navigate = useNavigate();
    return <button onClick={() => navigate("/grpc", { state })}>{label}</button>;
  }

  beforeEach(() => {
    sent.length = 0;
    HTMLElement.prototype.checkVisibility = () => true;
    window.history.pushState({}, "", "/grpc");
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const u = String(url);
        if (u.startsWith("/data/proto/")) return new Response(PROTO, { status: 200 });
        if (u.startsWith("/data/grpc/")) sent.push(JSON.parse(String(init?.body)));
        return new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
      }),
    );
  });

  const first = { read_mask: { paths: ["id"] }, filter: { id: 1 }, limit: 5 };
  const second = { read_mask: { paths: ["id"] }, filter: { id: 2 }, limit: 5 };

  function page() {
    return render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/grpc"]}>
          <Hand
            label="first"
            state={{ grpcMethod: "QueryCustomers()", grpcBody: first, autoRun: true }}
          />
          <Hand
            label="second"
            state={{ grpcMethod: "QueryCustomers()", grpcBody: second, autoRun: true }}
          />
          <GrpcPage />
        </MemoryRouter>
      </MantineProvider>,
    );
  }

  it("runs the body it was handed and publishes it to read_screen", async () => {
    page();
    await waitFor(() => expect(screen.getAllByText(/Orders/).length).toBeGreaterThan(0));
    act(() => screen.getByText("first").click());
    await waitFor(() => expect(sent).toEqual([first]));

    // The fake answers after it records the body, so the page may still be running at this point.
    await waitFor(async () => {
      const read = await readScreen(1000);
      if (typeof read === "string") throw new Error(read);
      expect(read.inputs).toMatchObject({ method: "QueryCustomers", status: "answered" });
      expect(JSON.parse(String(read.inputs?.body))).toEqual(first);
    });
  });

  it("runs a second body handed for the method already selected, not the one on screen", async () => {
    page();
    await waitFor(() => expect(screen.getAllByText(/Orders/).length).toBeGreaterThan(0));
    act(() => screen.getByText("first").click());
    await waitFor(() => expect(sent).toEqual([first]));
    act(() => screen.getByText("second").click());
    await waitFor(() => expect(sent).toEqual([first, second]));
  });

  it("publishes the first method when the handed one is not in the proto, for read-back", async () => {
    render(
      <MantineProvider>
        <MemoryRouter
          initialEntries={[
            { pathname: "/grpc", state: { grpcMethod: "QueryNope()", autoRun: true } },
          ]}
        >
          <GrpcPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    await waitFor(async () => {
      const read = await readScreen(1000);
      if (typeof read === "string") throw new Error(read);
      expect(read.inputs?.method).toBe("QueryOrders");
    });
    expect(sent).toEqual([]);
  });
});
