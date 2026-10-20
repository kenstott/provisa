// Copyright (c) 2026 Kenneth Stott
// Canary: 1dce8028-bc13-447e-b00e-2a7b3c546a91
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Tour anchors (test-utils/tourAnchor): the steps on surfaces of their own -- Polly's panel, which
// the runner opens through its launcher's handler (pollyOpen) for an org whose model is
// configured; the natural-language page's question box; the approvals page's pending tab.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";
import { render } from "../test-utils/render";
import { stepRoute, tourStep, walkTourStep } from "../test-utils/tourAnchor";

vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => AUTH,
}));
const AUTH = {
  role: { id: "org_admin" },
  selectedRoles: [{ id: "org_admin" }],
  capabilities: ["source_registration", "create_relationship", "query_development", "org_settings"],
  domainAccess: ["*"],
  activeOrgId: "acme",
};
vi.mock("../api/mcpChat", () => ({
  fetchMcpChatStatus: vi.fn(async () => ({ configured: true, reason: "" })),
}));

import { ChatPanel } from "../components/ChatPanel";
import { NlPage } from "../pages/NlPage";
import { RequestsPage } from "../pages/RequestsPage";

describe("the steps on surfaces of their own", () => {
  it("opens Polly for stepPolly", async () => {
    const step = tourStep("stepPolly");
    expect(step.pollyOpen).toBe(true);
    render(<ChatPanel />, { initialEntries: [stepRoute("stepPolly")] });
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    expect(await walkTourStep("stepPolly")).toBe(screen.getByTestId("chat-panel"));
  });
  it("finds step13's anchor", async () => {
    render(<NlPage />, { initialEntries: [stepRoute("step13")] });
    expect(await walkTourStep("step13")).toBeInTheDocument();
  });
  it("finds stepRequests's anchor", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify([]), { headers: { "Content-Type": "application/json" } }),
      ),
    );
    render(<RequestsPage />, { initialEntries: [stepRoute("stepRequests")] });
    expect(await walkTourStep("stepRequests")).toBeInTheDocument();
    vi.unstubAllGlobals();
  });
});
