// Copyright (c) 2026 Kenneth Stott
// Canary: 8c3e5a19-6d4f-4b72-9a15-2c7e1f8b6d40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, afterEach } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { useMcpChat } from "../hooks/useMcpChat";

// REQ-1800: the frontend sends its own current route with every chat turn, so the assistant can
// answer "what page am I on" from the request body instead of a tool round-trip.

function sseResponse(events: Record<string, unknown>[]): Response {
  const body = events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
  return new Response(body, { status: 200 });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("useMcpChat current_route", () => {
  it("includes the given current_route in the POST body", async () => {
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(sseResponse([{ type: "text", text: "hi" }, { type: "done" }]));

    const { result } = renderHook(() =>
      useMcpChat("analyst", { navigate: vi.fn(), confirm: vi.fn(), runMutation: vi.fn(), presentChoice: vi.fn() }, "/admin/ai-models"),
    );

    await act(async () => {
      await result.current.send("what page am i on");
    });

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [, init] = fetchMock.mock.calls[0];
    const sentBody = JSON.parse((init as RequestInit).body as string);
    expect(sentBody.current_route).toBe("/admin/ai-models");
  });

  it("sends current_route as undefined when none is given", async () => {
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(sseResponse([{ type: "text", text: "hi" }, { type: "done" }]));

    const { result } = renderHook(() =>
      useMcpChat("analyst", { navigate: vi.fn(), confirm: vi.fn(), runMutation: vi.fn(), presentChoice: vi.fn() }),
    );

    await act(async () => {
      await result.current.send("hi");
    });

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [, init] = fetchMock.mock.calls[0];
    const sentBody = JSON.parse((init as RequestInit).body as string);
    expect(sentBody.current_route).toBeUndefined();
  });
});
