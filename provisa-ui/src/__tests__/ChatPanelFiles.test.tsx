// Copyright (c) 2026 Kenneth Stott
// Canary: 5e7a2c90-3b1d-4f68-9a42-c8d0e6b1f375
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2114 (amended 2026-11-25): Polly works with files only through the person's browser. A file
// attached in the chat is read into the browser session and sent with every turn; a file she hands
// back is a download card in her reply.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { ChatPanel } from "../components/ChatPanel";
import { MAX_ATTACHMENT_BYTES, fileToBase64 } from "../hooks/useMcpChat";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "analyst" },
    selectedRoles: [{ id: "analyst" }],
  }),
}));

const fetchMcpChatStatus = vi.fn();
vi.mock("../api/mcpChat", () => ({
  fetchMcpChatStatus: () => fetchMcpChatStatus(),
}));

function sseResponse(events: Record<string, unknown>[]): Response {
  const body = events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
  return new Response(body, { status: 200 });
}

function sentBodies(fetchMock: ReturnType<typeof vi.spyOn>) {
  return fetchMock.mock.calls
    .filter((call: unknown[]) => String(call[0]).endsWith("/admin/mcp/chat"))
    .map((call: unknown[]) => JSON.parse((call[1] as RequestInit).body as string));
}

async function openPanel() {
  render(<ChatPanel />);
  fireEvent.click(screen.getByTestId("chat-panel-toggle"));
  await waitFor(() => screen.getByTestId("chat-panel"));
}

function attachInput(): HTMLInputElement {
  // Mantine's FileButton keeps its hidden input beside the control it opens.
  const input = document.querySelector('input[type="file"]');
  expect(input).not.toBeNull();
  return input as HTMLInputElement;
}

beforeEach(() => {
  fetchMcpChatStatus.mockReset();
  fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("ChatPanel — attaching a file (REQ-2114)", () => {
  it("offers the attach control for the kinds Polly reads, with the stated limit", async () => {
    await openPanel();
    const control = screen.getByTestId("chat-panel-attach");
    expect(control.getAttribute("aria-label")).toContain("up to 10 MB");
    const accept = attachInput().accept;
    for (const ending of [".csv", ".yaml", ".zip", ".pdf", ".docx"]) {
      expect(accept).toContain(ending);
    }
  });

  it("sends the attached file's bytes with every turn, until it is removed", async () => {
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () =>
        sseResponse([{ type: "text", text: "Read it." }, { type: "done" }]),
      );
    await openPanel();

    const file = new File(["id,total\n1,9.5\n"], "orders.csv", { type: "text/csv" });
    fireEvent.change(attachInput(), { target: { files: [file] } });
    await waitFor(() =>
      expect(screen.getByTestId("chat-panel-attachment")).toHaveTextContent("orders.csv"),
    );

    fireEvent.change(screen.getByPlaceholderText(/Ask me a question/), {
      target: { value: "Summarize this CSV" },
    });
    fireEvent.click(screen.getByTestId("chat-panel-send"));
    await waitFor(() => expect(screen.getByText("Read it.")).toBeInTheDocument());

    const [first] = sentBodies(fetchMock);
    expect(first.attachments).toEqual([
      { id: "att1", name: "orders.csv", data: btoa("id,total\n1,9.5\n") },
    ]);

    fireEvent.change(screen.getByPlaceholderText(/Ask me a question/), {
      target: { value: "And the total?" },
    });
    fireEvent.click(screen.getByTestId("chat-panel-send"));
    await waitFor(() => expect(sentBodies(fetchMock)).toHaveLength(2));
    expect(sentBodies(fetchMock)[1].attachments).toHaveLength(1);

    fireEvent.click(screen.getByLabelText("Remove orders.csv"));
    await waitFor(() => expect(screen.queryByTestId("chat-panel-attachment")).toBeNull());
    fireEvent.change(screen.getByPlaceholderText(/Ask me a question/), {
      target: { value: "Thanks" },
    });
    fireEvent.click(screen.getByTestId("chat-panel-send"));
    await waitFor(() => expect(sentBodies(fetchMock)).toHaveLength(3));
    expect(sentBodies(fetchMock)[2].attachments).toEqual([]);
  });

  it("refuses a file over the limit at once, and attaches nothing", async () => {
    await openPanel();
    const file = new File(["x"], "huge.csv", { type: "text/csv" });
    Object.defineProperty(file, "size", { value: MAX_ATTACHMENT_BYTES + 1 });
    fireEvent.change(attachInput(), { target: { files: [file] } });

    await waitFor(() =>
      expect(screen.getByTestId("chat-panel-attach-error")).toHaveTextContent(
        "huge.csv is larger than 10 MB",
      ),
    );
    expect(screen.queryByTestId("chat-panel-attachment")).toBeNull();
  });

  it("shows the server's refusal of an attachment as the server states it", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ detail: "'a.csv' is 12.0 MB, larger than 10.0 MB." }), {
        status: 413,
        headers: { "content-type": "application/json" },
      }),
    );
    await openPanel();
    fireEvent.change(screen.getByPlaceholderText(/Ask me a question/), {
      target: { value: "hi" },
    });
    fireEvent.click(screen.getByTestId("chat-panel-send"));
    await waitFor(() =>
      expect(screen.getByTestId("chat-panel-error")).toHaveTextContent(
        "'a.csv' is 12.0 MB, larger than 10.0 MB.",
      ),
    );
  });

  it("reads a file's bytes as base64", async () => {
    const bytes = new Uint8Array([0, 255, 16, 32]);
    expect(await fileToBase64(new Blob([bytes]))).toBe("AP8QIA==");
  });
});

describe("ChatPanel — a file Polly hands back (REQ-2114)", () => {
  it("shows a download card that saves the file under its name", async () => {
    const data = btoa("print('hi')\n");
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      sseResponse([
        {
          type: "artifact",
          kind: "download",
          filename: "orders.py",
          content_type: "text/x-python",
          size: 12,
          data,
        },
        { type: "text", text: "Here is the code." },
        { type: "done" },
      ]),
    );
    await openPanel();
    fireEvent.change(screen.getByPlaceholderText(/Ask me a question/), {
      target: { value: "Write me code" },
    });
    fireEvent.click(screen.getByTestId("chat-panel-send"));

    await waitFor(() => screen.getByTestId("chat-download"));
    expect(screen.getByTestId("chat-download-name")).toHaveTextContent("orders.py");
    const save = screen.getByTestId("chat-download-save");
    expect(save.getAttribute("download")).toBe("orders.py");
    expect(save.getAttribute("href")).toBe(`data:text/x-python;base64,${data}`);
    expect(save).toHaveTextContent("Download");
  });
});
