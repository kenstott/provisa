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

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { ChatPanel } from "../components/ChatPanel";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "analyst" } }),
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
