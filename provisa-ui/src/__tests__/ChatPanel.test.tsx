// Copyright (c) 2026 Kenneth Stott
// Canary: 4b8e2c71-9a5d-4f36-8c10-2e7f5b3a6d90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1804: clicking the chat toggle checks whether Polly (the LLM) is configured BEFORE opening
// the panel — an unconfigured deployment gets an explanatory modal instead of an open panel that
// only reveals the problem after a message is sent.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { ApolloClient } from "@apollo/client";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { ChatPanel } from "../components/ChatPanel";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "analyst" },
    selectedRoles: [{ id: "analyst" }, { id: "org_admin" }],
  }),
}));

const fetchMcpChatStatus = vi.fn();
vi.mock("../api/mcpChat", () => ({
  fetchMcpChatStatus: () => fetchMcpChatStatus(),
}));

const navigate = vi.fn();
vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react-router-dom")>();
  return { ...actual, useNavigate: () => navigate };
});

beforeEach(() => {
  fetchMcpChatStatus.mockReset();
  navigate.mockReset();
});

describe("ChatPanel preflight", () => {
  it("shows an explanatory modal instead of opening when the LLM isn't configured", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: false, reason: "ANTHROPIC_API_KEY is not set" });
    render(<ChatPanel />);

    fireEvent.click(screen.getByTestId("chat-panel-toggle"));

    await waitFor(() =>
      expect(screen.getByText(/ANTHROPIC_API_KEY is not set/)).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("chat-panel")).not.toBeInTheDocument();
  });

  it("navigates to AI Models settings from the modal", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: false, reason: "no vendor configured" });
    render(<ChatPanel />);

    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    await waitFor(() => screen.getByText("Open AI Models settings"));
    fireEvent.click(screen.getByText("Open AI Models settings"));

    expect(navigate).toHaveBeenCalledWith("/admin/ai-models");
  });

  it("opens the panel directly when the LLM is configured", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    render(<ChatPanel />);

    fireEvent.click(screen.getByTestId("chat-panel-toggle"));

    await waitFor(() => expect(screen.getByTestId("chat-panel")).toBeInTheDocument());
    expect(screen.queryByTestId("chat-panel-unconfigured-modal")).not.toBeInTheDocument();
  });
});

function sseResponse(events: Record<string, unknown>[]): Response {
  const body = events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
  return new Response(body, { status: 200 });
}

describe("ChatPanel — suggested questions (REQ-1806)", () => {
  it("shows the enumerated suggestions before any message is sent", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    await waitFor(() => screen.getByTestId("chat-panel-suggestions"));

    expect(screen.getByText(/Why are you named Polly\?/)).toBeInTheDocument();
  });

  it("sends a suggestion when clicked and it disappears once the conversation starts", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(sseResponse([{ type: "text", text: "Poly means many!" }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    await waitFor(() => screen.getByText(/Why are you named Polly\?/));
    fireEvent.click(screen.getByText(/Why are you named Polly\?/));

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [, init] = fetchMock.mock.calls[0];
    const sentBody = JSON.parse((init as RequestInit).body as string);
    expect(sentBody.messages[0].content).toBe("Why are you named Polly?");

    await waitFor(() =>
      expect(screen.queryByTestId("chat-panel-suggestions")).not.toBeInTheDocument(),
    );
    fetchMock.mockRestore();
  });

  it("typing just the suggestion's number sends the full question, not the digit", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(sseResponse([{ type: "text", text: "..." }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    await waitFor(() => screen.getByTestId("chat-panel-suggestions"));

    const textarea = screen.getByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "1" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [, init] = fetchMock.mock.calls[0];
    const sentBody = JSON.parse((init as RequestInit).body as string);
    expect(sentBody.messages[0].content).toBe("Why are you named Polly?");
    fetchMock.mockRestore();
  });

  it("a plain-text draft that happens to be a digit is sent as-is once a conversation exists", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(sseResponse([{ type: "text", text: "ok" }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);

    // First turn: a real message, so the suggestions are gone.
    fireEvent.change(textarea, { target: { value: "hello" } });
    fireEvent.keyDown(textarea, { key: "Enter" });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    // Second turn: "1" is now just a message, not a suggestion index.
    fireEvent.change(textarea, { target: { value: "1" } });
    fireEvent.keyDown(textarea, { key: "Enter" });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));

    const [, init] = fetchMock.mock.calls[1];
    const sentBody = JSON.parse((init as RequestInit).body as string);
    expect(sentBody.messages.at(-1).content).toBe("1");
    fetchMock.mockRestore();
  });
});

describe("ChatPanel — mic (REQ-1805)", () => {
  const original = {
    SpeechRecognition: window.SpeechRecognition,
    webkitSpeechRecognition: window.webkitSpeechRecognition,
  };

  afterEach(() => {
    window.SpeechRecognition = original.SpeechRecognition;
    window.webkitSpeechRecognition = original.webkitSpeechRecognition;
  });

  it("shows no mic button when the browser has no SpeechRecognition", async () => {
    delete (window as { SpeechRecognition?: unknown }).SpeechRecognition;
    delete (window as { webkitSpeechRecognition?: unknown }).webkitSpeechRecognition;
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    await waitFor(() => screen.getByTestId("chat-panel"));

    expect(screen.queryByTestId("chat-panel-mic")).not.toBeInTheDocument();
  });

  it("shows the mic button when SpeechRecognition is available", async () => {
    class FakeRecognition {
      lang = "";
      continuous = false;
      interimResults = false;
      onresult: (() => void) | null = null;
      onerror: (() => void) | null = null;
      onend: (() => void) | null = null;
      start = vi.fn();
      stop = vi.fn();
      abort = vi.fn();
    }
    window.SpeechRecognition = FakeRecognition as unknown as typeof window.SpeechRecognition;
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    await waitFor(() => screen.getByTestId("chat-panel"));

    expect(screen.getByTestId("chat-panel-mic")).toBeInTheDocument();
  });
});

describe("ChatPanel — present_choice widget (REQ-1810)", () => {
  it("yes_no mode renders Yes/No and resumes with the boolean answer", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        sseResponse([
          {
            type: "awaiting_client_tools",
            assistant_content: [
              { type: "tool_use", id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } },
            ],
            server_tool_results: [],
            pending: [{ id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } }],
          },
          { type: "done" },
        ]),
      )
      .mockResolvedValueOnce(sseResponse([{ type: "text", text: "Done." }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "do it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Yes"));
    fireEvent.click(screen.getByText("Yes"));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const [, secondInit] = fetchMock.mock.calls[1];
    const sentBody = JSON.parse((secondInit as RequestInit).body as string);
    const toolResultMsg = sentBody.messages.at(-1);
    const resultContent = JSON.parse(toolResultMsg.content[0].content);
    expect(resultContent.selected).toBe(true);
    fetchMock.mockRestore();
  });

  it("single mode renders radio options and resumes with the picked option", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        sseResponse([
          {
            type: "awaiting_client_tools",
            assistant_content: [
              {
                type: "tool_use",
                id: "c1",
                name: "present_choice",
                input: { question: "Which source?", mode: "single", options: ["A", "B"] },
              },
            ],
            server_tool_results: [],
            pending: [
              {
                id: "c1",
                name: "present_choice",
                input: { question: "Which source?", mode: "single", options: ["A", "B"] },
              },
            ],
          },
          { type: "done" },
        ]),
      )
      .mockResolvedValueOnce(sseResponse([{ type: "text", text: "Ok." }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find a source" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByLabelText("B"));
    fireEvent.click(screen.getByLabelText("B"));
    fireEvent.click(screen.getByText("Submit"));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const [, secondInit] = fetchMock.mock.calls[1];
    const sentBody = JSON.parse((secondInit as RequestInit).body as string);
    const toolResultMsg = sentBody.messages.at(-1);
    const resultContent = JSON.parse(toolResultMsg.content[0].content);
    expect(resultContent.selected).toBe("B");
    fetchMock.mockRestore();
  });
});

describe("ChatPanel — copy button (REQ-1811)", () => {
  it("copies a message's text to the clipboard", async () => {
    Object.assign(navigator, { clipboard: { writeText: vi.fn().mockResolvedValue(undefined) } });
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      sseResponse([{ type: "text", text: "Poly means many!" }, { type: "done" }]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "why polly" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Poly means many!"));
    // Two bubbles now exist (the user's "why polly" and the assistant's reply), each with its
    // own copy button — click the assistant's, which renders last.
    const copyButtons = screen.getAllByTestId("chat-message-copy");
    fireEvent.click(copyButtons[copyButtons.length - 1]);

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith("Poly means many!");
  });
});

describe("ChatPanel — role header carries every held role (REQ-1812)", () => {
  it("sends every selected role, not just the acting one, in x-provisa-role", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(sseResponse([{ type: "text", text: "ok" }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "hi" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [, init] = fetchMock.mock.calls[0];
    expect((init as RequestInit).headers).toMatchObject({
      "x-provisa-role": "analyst,org_admin",
    });
  });
});

describe("ChatPanel — clear conversation (REQ-1813)", () => {
  it("clears messages and is disabled when there is nothing to clear", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      sseResponse([{ type: "text", text: "Poly means many!" }, { type: "done" }]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    expect(screen.getByTestId("chat-panel-clear")).toBeDisabled();

    fireEvent.change(textarea, { target: { value: "why polly" } });
    fireEvent.keyDown(textarea, { key: "Enter" });
    await waitFor(() => screen.getByText("Poly means many!"));

    expect(screen.getByTestId("chat-panel-clear")).not.toBeDisabled();
    fireEvent.click(screen.getByTestId("chat-panel-clear"));

    expect(screen.queryByText("Poly means many!")).not.toBeInTheDocument();
    expect(screen.getByTestId("chat-panel-suggestions")).toBeInTheDocument();
  });

  it("also clears the tool-call log, not just the messages", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      sseResponse([
        { type: "tool_use", name: "search_catalog", input: { query: "orders" } },
        { type: "tool_result", name: "search_catalog", is_error: false },
        { type: "text", text: "Found it." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });
    await waitFor(() => screen.getByTestId("chat-panel-tool-badge"));

    fireEvent.click(screen.getByTestId("chat-panel-clear"));

    expect(screen.queryByTestId("chat-panel-tool-badge")).not.toBeInTheDocument();
    expect(screen.queryByTestId("chat-panel-tool-log")).not.toBeInTheDocument();
  });
});

describe("ChatPanel — present_choice answer recorded in conversation (REQ-1813)", () => {
  it("appends the picked option as a chat message once resolved", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        sseResponse([
          {
            type: "awaiting_client_tools",
            assistant_content: [
              {
                type: "tool_use",
                id: "c1",
                name: "present_choice",
                input: { question: "Which source?", mode: "single", options: ["A", "B"] },
              },
            ],
            server_tool_results: [],
            pending: [
              {
                id: "c1",
                name: "present_choice",
                input: { question: "Which source?", mode: "single", options: ["A", "B"] },
              },
            ],
          },
          { type: "done" },
        ]),
      )
      .mockResolvedValueOnce(sseResponse([{ type: "text", text: "Ok." }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find a source" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByLabelText("B"));
    fireEvent.click(screen.getByLabelText("B"));
    fireEvent.click(screen.getByText("Submit"));

    await waitFor(() => screen.getByText("Ok."));
    expect(screen.getByText("B")).toBeInTheDocument();
  });
});

describe("ChatPanel — present_choice modal positioning (REQ-1812)", () => {
  it("renders the choice overlay INSIDE the docked chat panel, not as a sibling of it", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        {
          type: "awaiting_client_tools",
          assistant_content: [
            { type: "tool_use", id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } },
          ],
          server_tool_results: [],
          pending: [{ id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } }],
        },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "do it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByTestId("chat-panel-choice-modal"));
    expect(screen.getByTestId("chat-panel")).toContainElement(
      screen.getByTestId("chat-panel-choice-modal"),
    );
    fetchMock.mockRestore();
  });
});

describe("ChatPanel — present_choice dialog is draggable (REQ-1843)", () => {
  it("moves the dialog by the drag amount when dragged via its handle", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        {
          type: "awaiting_client_tools",
          assistant_content: [
            { type: "tool_use", id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } },
          ],
          server_tool_results: [],
          pending: [{ id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } }],
        },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "do it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    const handle = await screen.findByTestId("chat-panel-choice-modal-drag-handle");
    const dialog = handle.closest(".mantine-Paper-root") as HTMLElement;
    expect(dialog.style.transform).toBe("");

    fireEvent.pointerDown(handle, { clientX: 100, clientY: 100 });
    fireEvent.pointerMove(handle, { clientX: 130, clientY: 175 });
    expect(dialog.style.transform).toBe("translate(30px, 75px)");

    fireEvent.pointerUp(handle);
    fetchMock.mockRestore();
  });
});

describe("ChatPanel — present_choice 'Something else' free text (REQ-1816)", () => {
  it("single mode resolves with typed text instead of a listed option", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        sseResponse([
          {
            type: "awaiting_client_tools",
            assistant_content: [
              {
                type: "tool_use",
                id: "c1",
                name: "present_choice",
                input: { question: "Which source?", mode: "single", options: ["A", "B"] },
              },
            ],
            server_tool_results: [],
            pending: [
              {
                id: "c1",
                name: "present_choice",
                input: { question: "Which source?", mode: "single", options: ["A", "B"] },
              },
            ],
          },
          { type: "done" },
        ]),
      )
      .mockResolvedValueOnce(sseResponse([{ type: "text", text: "Ok." }, { type: "done" }]));

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find a source" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByLabelText("Type something else"));
    fireEvent.click(screen.getByLabelText("Type something else"));
    fireEvent.change(screen.getByPlaceholderText("Type your answer…"), {
      target: { value: "Neither, use C" },
    });
    fireEvent.click(screen.getByText("Submit"));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const [, secondInit] = fetchMock.mock.calls[1];
    const sentBody = JSON.parse((secondInit as RequestInit).body as string);
    const resultContent = JSON.parse(sentBody.messages.at(-1).content[0].content);
    expect(resultContent.selected).toBe("Neither, use C");
    expect(screen.getByText("Neither, use C")).toBeInTheDocument();
    fetchMock.mockRestore();
  });
});

describe("ChatPanel — Stop/Go send control (REQ-1815)", () => {
  it("shows Stop while busy; clicking it aborts the request and refocuses the prompt", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    let capturedSignal: AbortSignal | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(
      (_url, init) =>
        new Promise((_resolve, reject) => {
          capturedSignal = (init as RequestInit).signal ?? undefined;
          capturedSignal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        }) as Promise<Response>,
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "do something slow" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Stop"));
    fireEvent.click(screen.getByTestId("chat-panel-send"));

    await waitFor(() => screen.getByText("Send"));
    expect(textarea).toHaveFocus();
  });
});

describe("ChatPanel — separate message per tool-loop round (REQ-1823)", () => {
  it("does not merge text said before a tool round trip with text said after it", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        sseResponse([
          { type: "text", text: "Let me check the catalog for that." },
          {
            type: "awaiting_client_tools",
            assistant_content: [
              { type: "text", text: "Let me check the catalog for that." },
              { type: "tool_use", id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } },
            ],
            server_tool_results: [],
            pending: [{ id: "c1", name: "present_choice", input: { question: "Proceed?", mode: "yes_no" } }],
          },
          { type: "done" },
        ]),
      )
      .mockResolvedValueOnce(
        sseResponse([{ type: "text", text: "All done, it's registered now." }, { type: "done" }]),
      );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "register it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Let me check the catalog for that."));
    await waitFor(() => screen.getByText("Yes"));
    fireEvent.click(screen.getByText("Yes"));
    await waitFor(() => screen.getByText("All done, it's registered now."));

    // Two DISTINCT assistant bubbles, not one bubble containing both sentences concatenated.
    expect(
      screen.queryByText("Let me check the catalog for that.All done, it's registered now."),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Let me check the catalog for that.")).toBeInTheDocument();
    expect(screen.getByText("All done, it's registered now.")).toBeInTheDocument();
  });
});

describe("ChatPanel — server tool result triggers a data refresh (REQ-1820)", () => {
  it("refetches active queries after create_source_now completes", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const refetchSpy = vi
      .spyOn(ApolloClient.prototype, "refetchQueries")
      .mockResolvedValue([]);
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        { type: "tool_use", name: "create_source_now", input: { source: { id: "kaggle_x" } } },
        { type: "tool_result", name: "create_source_now", is_error: false },
        { type: "text", text: "Created it." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "create it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Created it."));
    expect(refetchSpy).toHaveBeenCalledWith(expect.objectContaining({ include: "active" }));
    refetchSpy.mockRestore();
  });

  it("does not refetch for tools that don't mutate the catalog", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const refetchSpy = vi
      .spyOn(ApolloClient.prototype, "refetchQueries")
      .mockResolvedValue([]);
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        { type: "tool_use", name: "search_catalog", input: { query: "orders" } },
        { type: "tool_result", name: "search_catalog", is_error: false },
        { type: "text", text: "Found it." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Found it."));
    expect(refetchSpy).not.toHaveBeenCalled();
    refetchSpy.mockRestore();
  });
});

describe("ChatPanel — glossary tool result notifies the Glossary page (REQ-1845)", () => {
  it("dispatches provisa:glossary-changed after update_glossary_term completes", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const listener = vi.fn();
    window.addEventListener("provisa:glossary-changed", listener);
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        {
          type: "tool_use",
          name: "update_glossary_term",
          input: { term_id: 1, definition: "New definition." },
        },
        { type: "tool_result", name: "update_glossary_term", is_error: false },
        { type: "text", text: "Updated it." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "update it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Updated it."));
    expect(listener).toHaveBeenCalledTimes(1);
    window.removeEventListener("provisa:glossary-changed", listener);
  });

  it("does not dispatch provisa:glossary-changed for a non-glossary tool", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    const listener = vi.fn();
    window.addEventListener("provisa:glossary-changed", listener);
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        { type: "tool_use", name: "search_catalog", input: { query: "orders" } },
        { type: "tool_result", name: "search_catalog", is_error: false },
        { type: "text", text: "Found it." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Found it."));
    expect(listener).not.toHaveBeenCalled();
    window.removeEventListener("provisa:glossary-changed", listener);
  });
});

describe("ChatPanel — tool call log (REQ-1821)", () => {
  it("shows a tooltip with the tool's params on hover", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        { type: "tool_use", name: "search_catalog", input: { query: "orders" } },
        { type: "tool_result", name: "search_catalog", is_error: false },
        { type: "text", text: "Found it." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    const badge = await screen.findByTestId("chat-panel-tool-badge");
    fireEvent.mouseEnter(badge);
    await waitFor(() => screen.getByText(/"query": "orders"/));
  });

  it("shows a check icon (not the running spinner) once the tool completes", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        { type: "tool_use", name: "search_catalog", input: { query: "orders" } },
        { type: "tool_result", name: "search_catalog", is_error: false },
        { type: "text", text: "Found it." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "find it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("Found it."));
    expect(screen.getByTestId("chat-panel-tool-done-icon")).toBeInTheDocument();
    expect(screen.queryByTestId("chat-panel-tool-spinner")).not.toBeInTheDocument();
    expect(screen.getByTestId("chat-panel-tool-badge")).toHaveAttribute("data-running", "false");
  });

  it("shows an error icon when the tool fails", async () => {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      sseResponse([
        { type: "tool_use", name: "run_sql", input: { sql: "SELECT 1" } },
        { type: "tool_result", name: "run_sql", is_error: true },
        { type: "text", text: "That query failed." },
        { type: "done" },
      ]),
    );

    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask the assistant/);
    fireEvent.change(textarea, { target: { value: "run it" } });
    fireEvent.keyDown(textarea, { key: "Enter" });

    await waitFor(() => screen.getByText("That query failed."));
    expect(screen.getByTestId("chat-panel-tool-error-icon")).toBeInTheDocument();
  });
});
