// Copyright (c) 2026 Kenneth Stott
// Canary: 545073dc-133e-4391-943e-8b54a44e7e88
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A question handed to MCP Chat (NL "MCP Chat ›", Polly) is sent once, whether the page was just
// opened or was already open.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { render } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "org_admin" } }) }));

import { McpExplorePage } from "../pages/McpExplorePage";

const asked: string[] = [];

function Ask({ question }: { question: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/mcp", { state: { mcpQuestion: question } })}>
      {question}
    </button>
  );
}

describe("McpExplorePage — a question handed over while the page is open", () => {
  beforeEach(() => {
    asked.length = 0;
    sessionStorage.clear();
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        if (String(url).endsWith("/admin/mcp/chat")) asked.push(String(init?.body));
        return new Response("", { status: 200 });
      }),
    );
  });

  it("sends each handed question", async () => {
    render(
      <>
        <Ask question="how many orders?" />
        <Ask question="top customers?" />
        <McpExplorePage />
      </>,
      { initialEntries: ["/mcp"] },
    );
    act(() => screen.getByText("how many orders?").click());
    await waitFor(() => expect(asked.some((b) => b.includes("how many orders?"))).toBe(true));
    await waitFor(() => expect(screen.getByText("top customers?")).toBeEnabled());

    act(() => screen.getByText("top customers?").click());
    await waitFor(() => expect(asked.some((b) => b.includes("top customers?"))).toBe(true));
  });
});
