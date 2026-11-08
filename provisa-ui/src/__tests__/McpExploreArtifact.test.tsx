// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8a62d1-9c47-4e0b-b5a3-71d2e4c6a908
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A file the model delivered (the server's `artifact` stream event) shows its download in the
// Explore → MCP chat, and survives a reload of the page like the rest of that chat.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import { render } from "../test-utils/render";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "org_admin" }, selectedRoles: [{ id: "org_admin" }] }),
}));

import { McpExplorePage } from "../pages/McpExplorePage";

const sse = (ev: object) => `data: ${JSON.stringify(ev)}\n\n`;
const STREAM =
  sse({ type: "text", text: "Your CSV is ready." }) +
  sse({
    type: "artifact",
    kind: "file",
    redirect_url: "/results/orders.csv",
    row_count: 42,
    expires_in: 600,
    content_type: "text/csv",
  });

describe("McpExplorePage — a delivered file", () => {
  beforeEach(() => {
    sessionStorage.clear();
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) =>
        String(url).endsWith("/admin/mcp/chat")
          ? new Response(STREAM, { status: 200 })
          : new Response("", { status: 200 }),
      ),
    );
  });

  it("renders the download on the assistant message, and again after a remount", async () => {
    const first = render(<McpExplorePage />, { initialEntries: ["/mcp"] });
    await waitFor(() => expect(document.querySelector("[contenteditable]")).not.toBeNull());
    const input = document.querySelector("[contenteditable]") as HTMLElement;
    act(() => {
      input.textContent = "give me a csv";
      input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    });
    await act(async () => {
      (document.querySelector(".cs-button--send") as HTMLElement).click();
    });
    await waitFor(() => expect(screen.getByTestId("result-redirected")).toBeInTheDocument());
    expect(screen.getByText("Your CSV is ready.")).toBeInTheDocument();
    first.unmount();

    render(<McpExplorePage />, { initialEntries: ["/mcp"] });
    await waitFor(() => expect(screen.getByTestId("result-redirected")).toBeInTheDocument());
  });
});
