// Copyright (c) 2026 Kenneth Stott
// Canary: ddaa9c48-d378-4104-86ec-1700e69f8699
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2253: the MCP chat works only within the navbar's selected domains. Both chat callers (the
// chat panel's hook and the Explore page) send useViewDomains() as a list; no narrowing sends
// none, so the server keeps the role's whole surface.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, renderHook, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { render } from "../test-utils/render";

let selected: ReadonlySet<string> | null = null;
vi.mock("../lib/viewDomains", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../lib/viewDomains")>()),
  useViewDomains: () => selected,
}));

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ actingRoles: "analyst" }),
}));

import { useMcpChat } from "../hooks/useMcpChat";
import { McpExplorePage } from "../pages/McpExplorePage";

function sse(events: object[]): Response {
  const body = events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
  return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  HTMLElement.prototype.checkVisibility = () => true;
  sessionStorage.clear();
  fetchMock = vi.fn(async (url: string) => {
    if (String(url).endsWith("/admin/mcp/chat")) {
      return sse([{ type: "text", text: "ok" }, { type: "done" }]);
    }
    if (String(url).endsWith("/admin/mcp-server")) {
      return new Response(JSON.stringify({ enabled: false }), { status: 200 });
    }
    return new Response("", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  selected = null;
});

function chatBody(): Record<string, unknown> {
  const call = fetchMock.mock.calls.find(([url]) => String(url).endsWith("/admin/mcp/chat"));
  if (!call) throw new Error("no chat request sent");
  return JSON.parse((call[1] as RequestInit).body as string);
}

const ctx = {
  navigate: vi.fn(),
  confirm: vi.fn(),
  runMutation: vi.fn(),
  presentChoice: vi.fn(),
  runAdminMutation: vi.fn(),
  chooseFile: vi.fn(),
  changed: vi.fn(),
};

async function sendFromHook(): Promise<void> {
  const { result } = renderHook(() => useMcpChat("analyst", ctx));
  await act(async () => {
    await result.current.send("which orders?");
  });
}

describe("the chat panel's chat (useMcpChat)", () => {
  it("sends the selected domains, the no-domain domain included", async () => {
    selected = new Set(["sales", ""]);
    await sendFromHook();
    expect(chatBody().domains).toEqual(["sales", ""]);
  });

  it("sends no domains when nothing narrows the view", async () => {
    selected = null;
    await sendFromHook();
    expect("domains" in chatBody()).toBe(false);
  });
});

function Handoff() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/explore", { state: { mcpQuestion: "which orders?" } })}>
      ask
    </button>
  );
}

async function sendFromExplore(): Promise<void> {
  window.history.pushState({}, "", "/explore");
  render(
    <>
      <Handoff />
      <McpExplorePage />
    </>,
    { initialEntries: ["/explore"] },
  );
  act(() => screen.getByText("ask").click());
  await waitFor(() =>
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith("/admin/mcp/chat"))).toBe(
      true,
    ),
  );
}

describe("the Explore page's chat", () => {
  it("sends the selected domains", async () => {
    selected = new Set(["sales"]);
    await sendFromExplore();
    expect(chatBody().domains).toEqual(["sales"]);
  });

  it("sends no domains when nothing narrows the view", async () => {
    selected = null;
    await sendFromExplore();
    expect("domains" in chatBody()).toBe(false);
  });
});
