// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2172: a handed question marks the explorer answered once its answer is in; a run_sql call's
// badge opens its explain -- the statement the agent sent, and the one that ran for the role; and a
// hand-off opens the Connect Claude Desktop panel.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { render } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ actingRoles: "support_west" }),
}));

import { McpExplorePage } from "../pages/McpExplorePage";

const SENT = "SELECT c.name, c.email FROM \"shop\".\"customers\" AS c";
const RAN = "SELECT c.name, REGEXP_REPLACE(c.email, '^[^@]+', '***') FROM customers c WHERE region = 'West'";

const explained: { sql: string; role: string | null }[] = [];

function sse(events: object[]): Response {
  const body = events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
  return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

function Handoff({ label, state }: { label: string; state: object }) {
  const navigate = useNavigate();
  return <button onClick={() => navigate("/explore", { state })}>{label}</button>;
}

describe("McpExplorePage -- what the agent's calls ran", () => {
  beforeEach(() => {
    explained.length = 0;
    sessionStorage.clear();
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const path = String(url);
        if (path.endsWith("/admin/mcp/chat")) {
          return sse([
            { type: "tool_use", name: "list_tables", input: { schema: "shop" } },
            { type: "tool_result", name: "list_tables", is_error: false },
            { type: "tool_use", name: "run_sql", input: { sql: SENT } },
            { type: "tool_result", name: "run_sql", is_error: false },
            { type: "text", text: "Two West customers ordered." },
          ]);
        }
        if (path.endsWith("/data/sql/explain")) {
          const headers = new Headers(init?.headers);
          explained.push({
            sql: (JSON.parse(String(init?.body)) as { sql: string }).sql,
            role: headers.get("X-Provisa-Role"),
          });
          return new Response(JSON.stringify({ sql: RAN, plan: [], mermaid: "" }), { status: 200 });
        }
        if (path.endsWith("/admin/mcp-server")) {
          return new Response(JSON.stringify({ enabled: true, url: "http://localhost:8101/mcp" }), {
            status: 200,
          });
        }
        return new Response("", { status: 404 });
      }),
    );
  });

  it("marks the handed question answered, and opens the last run_sql call's explain as the role", async () => {
    render(
      <>
        <Handoff label="ask" state={{ mcpQuestion: "Which West customers ordered?" }} />
        <McpExplorePage />
      </>,
      { initialEntries: ["/explore"] },
    );
    const page = screen.getByTestId("mcp-explore");
    expect(page).not.toHaveAttribute("data-answered");
    act(() => screen.getByText("ask").click());
    await waitFor(() => expect(page).toHaveAttribute("data-answered"));

    // Only the run_sql call explains; it is the turn's last.
    const last = document.querySelector<HTMLElement>("[data-mcp-explain-last]");
    expect(last?.textContent).toBe("run_sql");
    expect(document.querySelectorAll("[data-mcp-explain-last]")).toHaveLength(1);

    act(() => last!.click());
    expect(await screen.findByTestId("mcp-explain-sent")).toHaveTextContent(SENT);
    expect(await screen.findByTestId("mcp-explain-ran")).toHaveTextContent("region = 'West'");
    expect(explained).toEqual([{ sql: SENT, role: "support_west" }]);
  });

  it("opens the Connect Claude Desktop panel when a hand-off asks for it", async () => {
    render(
      <>
        <Handoff label="connect" state={{ mcpConnect: true }} />
        <McpExplorePage />
      </>,
      { initialEntries: ["/explore"] },
    );
    await screen.findByText("Connect Claude Desktop");
    act(() => screen.getByText("connect").click());
    await waitFor(() => expect(screen.getByTestId("mcp-connect-config")).toBeVisible());
  });
});
