// Copyright (c) 2026 Kenneth Stott
// Canary: e1b49a07-3c58-4d2f-96a0-84f7c25d1e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A gRPC request body that is blank says there is no request to send, in the response pane,
// before anything is sent. A real body still sends.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "analyst" },
    selectedRoles: [{ id: "analyst" }],
    actingRoles: "analyst",
  }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));
// The body editor stood in for by a textarea (CodeMirror does not measure in jsdom).
vi.mock("../pages/grpc/GrpcCodeView", () => ({
  GrpcCodeView: ({
    value,
    onChange,
    "data-testid": id,
  }: {
    value: string;
    onChange?: (v: string) => void;
    "data-testid"?: string;
  }) =>
    onChange ? (
      <textarea data-testid={id} value={value} onChange={(e) => onChange(e.target.value)} />
    ) : (
      <pre>{value}</pre>
    ),
}));

import "../i18n";
import { GrpcPage } from "../pages/GrpcPage";

const PROTO = `syntax = "proto3";
package provisa.v1;
service ProvisaService {
  rpc QueryOrders(OrdersRequest) returns (stream Orders);
}
message OrdersRequest { int32 limit = 1; }
message Orders { int32 id = 1; }
`;

const urls: string[] = [];

beforeEach(() => {
  urls.length = 0;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      const u = String(url);
      urls.push(u);
      if (u.startsWith("/data/proto/")) return new Response(PROTO, { status: 200 });
      return new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
    }),
  );
});

async function ready() {
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/grpc"]}>
        <GrpcPage />
      </MemoryRouter>
    </MantineProvider>,
  );
  await waitFor(() => expect(screen.getByTestId("grpc-send-btn")).not.toBeDisabled());
  return screen.getByTestId("grpc-body-editor");
}

describe("gRPC explorer with no request", () => {
  it.each([["blank", "  \n"]])("%s body: says so and sends nothing", async (_n, text) => {
    const editor = await ready();
    fireEvent.change(editor, { target: { value: text } });
    fireEvent.click(screen.getByTestId("grpc-send-btn"));
    expect(await screen.findByTestId("grpc-no-query")).toHaveTextContent(
      "There is no query to run.",
    );
    expect(urls.filter((u) => u.startsWith("/data/grpc/"))).toHaveLength(0);
  });

  it("a real body still sends", async () => {
    await ready();
    fireEvent.click(screen.getByTestId("grpc-send-btn"));
    await waitFor(() => expect(urls).toContain("/data/grpc/Orders"));
    expect(screen.queryByTestId("grpc-no-query")).toBeNull();
  });
});
