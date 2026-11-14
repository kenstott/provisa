// Copyright (c) 2026 Kenneth Stott
// Canary: 274cb01d-1cd6-4ca7-a778-71e89e014c5b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Polly reads the MCP explorer (/explore): the conversation, the tool calls of the last answer (a
// run_sql call with its statement in full), whether a turn is running and the draft, published to
// read_screen while the page is mounted and gone once it is not.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { render } from "../test-utils/render";
import { readScreen } from "../mcp/screenState";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";
import { setStored } from "../lib/browserStorage";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ actingRoles: "support_west" }),
}));

import { McpExplorePage, SCREEN_MESSAGES, SCREEN_TEXT_CHARS } from "../pages/McpExplorePage";

const QUESTION = "Which West customers ordered?";
const ANSWER = "Two West customers ordered.";
const SENT = 'SELECT c.name, c.email FROM "shop"."customers" AS c WHERE c.region = \'West\'';

function sse(events: object[]): string {
  return events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
}

const TURN = [
  { type: "tool_use", name: "list_tables", input: { schema: "shop" } },
  { type: "tool_result", name: "list_tables", is_error: false },
  { type: "tool_use", name: "run_sql", input: { sql: SENT } },
  { type: "tool_result", name: "run_sql", is_error: false },
  { type: "tool_use", name: "describe_table", input: { table: "orders" } },
  { type: "tool_result", name: "describe_table", is_error: true },
  { type: "text", text: ANSWER },
];

/** The chat's answer: the events of TURN, or held open after `holdAfter` events until released. */
let release: (() => void) | null = null;
function chatAnswer(holdAfter: number | null): Response {
  const encoder = new TextEncoder();
  const events = TURN;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const first = holdAfter === null ? events : events.slice(0, holdAfter);
      controller.enqueue(encoder.encode(sse(first)));
      if (holdAfter === null) {
        controller.close();
        return;
      }
      release = () => {
        controller.enqueue(encoder.encode(sse(events.slice(holdAfter))));
        controller.close();
      };
    },
  });
  return new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

let holdAfter: number | null = null;

function Handoff({ question }: { question: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/explore", { state: { mcpQuestion: question } })}>ask</button>
  );
}

async function inputs() {
  const read = await readScreen(500);
  if (typeof read === "string") throw new Error(read);
  return read.inputs;
}

const ctx: ClientToolContext = {
  navigate: vi.fn(),
  confirm: vi.fn(),
  runMutation: vi.fn(),
  presentChoice: vi.fn(),
  runAdminMutation: vi.fn(),
  chooseFile: vi.fn(),
};

function page(question = QUESTION) {
  return render(
    <>
      <Handoff question={question} />
      <McpExplorePage />
    </>,
    { initialEntries: ["/explore"] },
  );
}

beforeEach(() => {
  HTMLElement.prototype.checkVisibility = () => true;
  sessionStorage.clear();
  window.history.pushState({}, "", "/explore");
  holdAfter = null;
  release = null;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      const path = String(url);
      if (path.endsWith("/admin/mcp/chat")) return chatAnswer(holdAfter);
      if (path.endsWith("/admin/mcp-server")) {
        return new Response(JSON.stringify({ enabled: false }), { status: 200 });
      }
      return new Response("", { status: 404 });
    }),
  );
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  window.history.pushState({}, "", "/");
});

describe("McpExplorePage -- what read_screen reports", () => {
  it("publishes the question, the answer and the tool calls, a run_sql call with its statement", async () => {
    page();
    expect(await inputs()).toEqual({
      status: "idle",
      error: "",
      draft: "",
      tools: [],
      messageCount: 0,
      messages: [],
    });

    act(() => screen.getByText("ask").click());
    await waitFor(() => expect(screen.getByTestId("mcp-explore")).toHaveAttribute("data-answered"));

    expect(await inputs()).toEqual({
      status: "answered",
      error: "",
      draft: "",
      tools: [
        { name: "list_tables", status: "done", sql: null },
        { name: "run_sql", status: "done", sql: SENT },
        { name: "describe_table", status: "error", sql: null },
      ],
      messageCount: 2,
      messages: [
        { role: "user", text: QUESTION, cut: false },
        { role: "assistant", text: ANSWER, cut: false },
      ],
    });
  });

  it("says a turn is running, and which of its calls are", async () => {
    holdAfter = 3; // list_tables done, run_sql sent and not yet answered
    page();
    act(() => screen.getByText("ask").click());
    await waitFor(async () => expect((await inputs())?.status).toBe("running"));
    expect((await inputs())?.tools).toEqual([
      { name: "list_tables", status: "done", sql: null },
      { name: "run_sql", status: "running", sql: SENT },
    ]);

    act(() => release!());
    await waitFor(async () => expect((await inputs())?.status).toBe("answered"));
  });

  it("publishes the draft in the input", async () => {
    setStored("mcp.hist.support_west", JSON.stringify(["How many orders shipped?"]), {
      session: true,
    });
    page();
    const input = await screen.findByTestId("mcp-chat-input");
    const editable = input.querySelector<HTMLElement>("[contenteditable]") ?? input;
    fireEvent.keyDown(editable, { key: "ArrowUp" });
    await waitFor(async () => expect((await inputs())?.draft).toBe("How many orders shipped?"));
  });

  it("keeps the last messages, each cut at its limit and saying so", async () => {
    const long = "x".repeat(SCREEN_TEXT_CHARS + 50);
    const saved = Array.from({ length: SCREEN_MESSAGES + 2 }, (_, i) => ({
      role: i % 2 === 0 ? "user" : "assistant",
      text: i === SCREEN_MESSAGES + 1 ? long : `message ${i}`,
    }));
    setStored("mcp.chat.support_west", JSON.stringify(saved), { session: true });
    page();
    await waitFor(async () => expect((await inputs())?.messageCount).toBe(SCREEN_MESSAGES + 2));
    const { messages } = (await inputs()) as { messages: { text: string; cut: boolean }[] };
    expect(messages).toHaveLength(SCREEN_MESSAGES);
    expect(messages[0].text).toBe("message 2");
    expect(messages[SCREEN_MESSAGES - 1]).toEqual({
      role: "assistant",
      text: "x".repeat(SCREEN_TEXT_CHARS),
      cut: true,
    });
  });

  it("puts the statement ahead of the conversation, so read_screen's limit cuts the conversation first", async () => {
    const saved = Array.from({ length: SCREEN_MESSAGES }, (_, i) => ({
      role: i % 2 === 0 ? "user" : "assistant",
      text: "y".repeat(SCREEN_TEXT_CHARS),
    }));
    setStored("mcp.chat.support_west", JSON.stringify(saved), { session: true });
    page();
    act(() => screen.getByText("ask").click());
    await waitFor(() => expect(screen.getByTestId("mcp-explore")).toHaveAttribute("data-answered"));
    const result = await executeClientTool("read_screen", {}, ctx);
    expect(result.success).toBe(true);
    expect(result.message).toContain(JSON.stringify(SENT));
  });

  it("is cleared when the page unmounts", async () => {
    const { unmount } = page();
    act(() => screen.getByText("ask").click());
    await waitFor(() => expect(screen.getByTestId("mcp-explore")).toHaveAttribute("data-answered"));
    expect((await inputs())?.tools).toHaveLength(3);

    unmount();
    expect(await readScreen(50)).toBe("the page on /explore has not loaded within 0.05s");
  });
});
