// Copyright (c) 2026 Kenneth Stott
// Canary: 5e62c8e3-a00d-43e1-889c-ccacb30a5b08
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1192: a file Polly delivered is served only to the user who asked for it, so the chat
// panel's download fetches it with that user's credentials; a link the browser follows has none.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { ChatPanel } from "../components/ChatPanel";

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

beforeEach(() => {
  fetchMcpChatStatus.mockReset();
});

function sseResponse(events: Record<string, unknown>[]): Response {
  const body = events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
  return new Response(body, { status: 200 });
}

describe("ChatPanel — a file Polly delivered is fetched as the signed-in user", () => {
  const FILE_URL = "/data/redirect-file/0123456789abcdef0123456789abcdef.csv";

  async function fileLink(fetchMock: ReturnType<typeof vi.spyOn>) {
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    fetchMock.mockResolvedValueOnce(
      sseResponse([
        {
          type: "artifact",
          kind: "file",
          redirect_url: FILE_URL,
          row_count: 37,
          expires_in: 3600,
          content_type: "text/csv",
        },
        { type: "text", text: "It is ready." },
        { type: "done" },
      ]),
    );
    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask me a question/);
    fireEvent.change(textarea, { target: { value: "as a CSV" } });
    fireEvent.keyDown(textarea, { key: "Enter" });
    return (await screen.findByTestId("result-redirected")).querySelector("a")!;
  }

  afterEach(() => vi.restoreAllMocks());

  it("fetches the file through the app's fetch and saves what came back, leaving the page", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch");
    const link = await fileLink(fetchMock);
    fetchMock.mockResolvedValueOnce(new Response("id,name\n1,a\n", { status: 200 }));
    const objectUrl = vi.fn((_blob: Blob) => "blob:saved");
    URL.createObjectURL = objectUrl as typeof URL.createObjectURL;
    URL.revokeObjectURL = vi.fn();
    const clicked: HTMLAnchorElement[] = [];
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
      this: HTMLAnchorElement,
    ) {
      clicked.push(this);
    });

    // false: the browser did not follow the link, which would carry no credential.
    expect(fireEvent.click(link)).toBe(false);

    await waitFor(() => expect(objectUrl).toHaveBeenCalledTimes(1));
    expect(fetchMock).toHaveBeenLastCalledWith(FILE_URL);
    expect(await objectUrl.mock.calls[0][0].text()).toBe("id,name\n1,a\n");
    expect(clicked[0].download).toBe("0123456789abcdef0123456789abcdef.csv");
    expect(screen.queryByTestId("result-redirect-failed")).not.toBeInTheDocument();
  });

  it("says the download failed when the server refuses it", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch");
    const link = await fileLink(fetchMock);
    fetchMock.mockResolvedValueOnce(new Response("gone", { status: 404 }));
    const objectUrl = vi.fn();
    URL.createObjectURL = objectUrl as typeof URL.createObjectURL;

    fireEvent.click(link);

    const failed = await screen.findByTestId("result-redirect-failed");
    expect(failed).toHaveTextContent("404");
    expect(objectUrl).not.toHaveBeenCalled();
  });

  it("leaves a presigned object-store link to the browser", async () => {
    const presigned = "https://store.example/results/a.csv?X-Amz-Signature=abc";
    const fetchMock = vi.spyOn(globalThis, "fetch");
    fetchMcpChatStatus.mockResolvedValue({ configured: true, reason: "" });
    fetchMock.mockResolvedValueOnce(
      sseResponse([
        { type: "artifact", kind: "file", redirect_url: presigned, row_count: 37 },
        { type: "done" },
      ]),
    );
    render(<ChatPanel />);
    fireEvent.click(screen.getByTestId("chat-panel-toggle"));
    const textarea = await screen.findByPlaceholderText(/Ask me a question/);
    fireEvent.change(textarea, { target: { value: "as a CSV" } });
    fireEvent.keyDown(textarea, { key: "Enter" });
    const link = (await screen.findByTestId("result-redirected")).querySelector("a")!;
    link.addEventListener("click", (e) => e.preventDefault()); // jsdom cannot navigate

    expect(link.getAttribute("href")).toBe(presigned);
    const calls = fetchMock.mock.calls.length;
    fireEvent.click(link);
    expect(fetchMock.mock.calls.length).toBe(calls);
  });
});
