// Copyright (c) 2026 Kenneth Stott
// Canary: 5e9b2c74-1a3d-4f86-9c07-d4a6b8e0f213
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1910: the operator's debug-trace control. What matters: a window is started with a scope and
// a stated duration, an open window shows the time it has left and counts down by itself, stopping
// one names that window, and the per-request hint is permitted and revoked per role.
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { act } from "react";
import { render, screen, fireEvent, waitFor, within } from "../../../test-utils/render";
import type { DebugTraceState } from "../../../api/debugTrace";

vi.mock("../../../api/debugTrace", () => ({
  fetchDebugTrace: vi.fn(),
  startDebugWindow: vi.fn(),
  stopDebugWindow: vi.fn(),
  setDebugHintRole: vi.fn(),
}));

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  fetchOrgs: vi.fn().mockResolvedValue([
    { id: "acme", name: "Acme", created_by: null, created_at: "", isolated_engine: false },
    { id: "globex", name: "Globex", created_by: null, created_at: "", isolated_engine: false },
  ]),
}));

import {
  fetchDebugTrace,
  setDebugHintRole,
  startDebugWindow,
  stopDebugWindow,
} from "../../../api/debugTrace";
import { DebugTracePanel } from "../DebugTracePanel";

const mockFetch = vi.mocked(fetchDebugTrace);
const mockStart = vi.mocked(startDebugWindow);
const mockStop = vi.mocked(stopDebugWindow);
const mockHint = vi.mocked(setDebugHintRole);

function state(overrides: Partial<DebugTraceState> = {}): DebugTraceState {
  return {
    windows: [],
    hint_roles: [],
    max_minutes: 1440,
    hint_setting: "debug_trace_hint_roles",
    ...overrides,
  };
}

const acmeWindow = {
  id: "w1",
  scope: "role" as const,
  org_id: "acme",
  target: "analyst",
  started_at: "2026-10-01T12:00:00+00:00",
  expires_at: "2026-10-01T12:15:00+00:00",
  remaining_seconds: 125,
  created_by: "ops@example.com",
};

describe("DebugTracePanel", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockStart.mockReset();
    mockStop.mockReset();
    mockHint.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("says so when no window is open and no role is permitted the hint", async () => {
    mockFetch.mockResolvedValue(state());
    render(<DebugTracePanel />);
    expect(await screen.findByTestId("debug-trace-no-windows")).toBeTruthy();
    expect(screen.getByTestId("debug-trace-no-hint-roles")).toBeTruthy();
  });

  it("lists an open window with its scope, target and time remaining", async () => {
    mockFetch.mockResolvedValue(state({ windows: [acmeWindow] }));
    render(<DebugTracePanel />);
    const row = await screen.findByTestId("debug-trace-window-w1");
    expect(row.textContent).toContain("Role");
    expect(row.textContent).toContain("acme");
    expect(row.textContent).toContain("analyst");
    expect(within(row).getByTestId("debug-trace-remaining-w1").textContent).toBe("2:05");
  });

  it("counts the time remaining down and drops the window when it ends", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    mockFetch.mockResolvedValue(state({ windows: [{ ...acmeWindow, remaining_seconds: 3 }] }));
    render(<DebugTracePanel />);
    const remaining = await screen.findByTestId("debug-trace-remaining-w1");
    expect(remaining.textContent).toBe("0:03");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(screen.getByTestId("debug-trace-remaining-w1").textContent).toBe("0:01");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(screen.queryByTestId("debug-trace-window-w1")).toBeNull();
    expect(screen.getByTestId("debug-trace-no-windows")).toBeTruthy();
  });

  it("starts a role window with the org, the role and the stated minutes", async () => {
    mockFetch.mockResolvedValue(state());
    mockStart.mockResolvedValue(state({ windows: [acmeWindow] }));
    render(<DebugTracePanel />);
    await screen.findByTestId("debug-trace-no-windows");
    await waitFor(() =>
      expect(screen.getByTestId("debug-trace-org").querySelectorAll("option").length).toBe(2),
    );
    fireEvent.change(screen.getByTestId("debug-trace-scope"), { target: { value: "role" } });
    fireEvent.change(screen.getByTestId("debug-trace-org"), { target: { value: "globex" } });
    fireEvent.change(screen.getByTestId("debug-trace-target"), { target: { value: " analyst " } });
    fireEvent.change(screen.getByTestId("debug-trace-minutes"), { target: { value: "30" } });
    fireEvent.click(screen.getByTestId("debug-trace-start"));
    await waitFor(() =>
      expect(mockStart).toHaveBeenCalledWith({
        scope: "role",
        org_id: "globex",
        target: "analyst",
        minutes: 30,
      }),
    );
    expect(await screen.findByTestId("debug-trace-window-w1")).toBeTruthy();
  });

  it("starts an org window with no target", async () => {
    mockFetch.mockResolvedValue(state());
    mockStart.mockResolvedValue(state());
    render(<DebugTracePanel />);
    await screen.findByTestId("debug-trace-no-windows");
    await waitFor(() =>
      expect(screen.getByTestId("debug-trace-org").querySelectorAll("option").length).toBe(2),
    );
    expect(screen.queryByTestId("debug-trace-target")).toBeNull();
    fireEvent.click(screen.getByTestId("debug-trace-start"));
    await waitFor(() =>
      expect(mockStart).toHaveBeenCalledWith({
        scope: "org",
        org_id: "acme",
        target: null,
        minutes: 15,
      }),
    );
  });

  it("does not start a role window without a role", async () => {
    mockFetch.mockResolvedValue(state());
    render(<DebugTracePanel />);
    await screen.findByTestId("debug-trace-no-windows");
    fireEvent.change(screen.getByTestId("debug-trace-scope"), { target: { value: "role" } });
    expect((screen.getByTestId("debug-trace-start") as HTMLButtonElement).disabled).toBe(true);
  });

  it("stops the window that was clicked", async () => {
    mockFetch.mockResolvedValue(state({ windows: [acmeWindow] }));
    mockStop.mockResolvedValue(state());
    render(<DebugTracePanel />);
    fireEvent.click(await screen.findByTestId("debug-trace-stop-w1"));
    await waitFor(() => expect(mockStop).toHaveBeenCalledWith("w1"));
    expect(await screen.findByTestId("debug-trace-no-windows")).toBeTruthy();
  });

  it("permits the hint for a role and revokes it", async () => {
    mockFetch.mockResolvedValue(state());
    mockHint.mockResolvedValueOnce(state({ hint_roles: [{ org_id: "acme", role_id: "analyst" }] }));
    mockHint.mockResolvedValueOnce(state());
    render(<DebugTracePanel />);
    await screen.findByTestId("debug-trace-no-hint-roles");
    await waitFor(() =>
      expect(screen.getByTestId("debug-trace-hint-org").querySelectorAll("option").length).toBe(2),
    );
    fireEvent.change(screen.getByTestId("debug-trace-hint-role"), {
      target: { value: "analyst" },
    });
    fireEvent.click(screen.getByTestId("debug-trace-hint-permit"));
    await waitFor(() =>
      expect(mockHint).toHaveBeenCalledWith({
        org_id: "acme",
        role_id: "analyst",
        permitted: true,
      }),
    );
    fireEvent.click(await screen.findByTestId("debug-trace-hint-revoke-acme-analyst"));
    await waitFor(() =>
      expect(mockHint).toHaveBeenLastCalledWith({
        org_id: "acme",
        role_id: "analyst",
        permitted: false,
      }),
    );
    expect(await screen.findByTestId("debug-trace-no-hint-roles")).toBeTruthy();
  });

  it("shows the server's reason when a change is refused", async () => {
    mockFetch.mockResolvedValue(state());
    mockStart.mockRejectedValue(new Error("minutes must be between 1 and 1440, got 0"));
    render(<DebugTracePanel />);
    await screen.findByTestId("debug-trace-no-windows");
    await waitFor(() =>
      expect(screen.getByTestId("debug-trace-org").querySelectorAll("option").length).toBe(2),
    );
    fireEvent.click(screen.getByTestId("debug-trace-start"));
    expect((await screen.findByTestId("debug-trace-error")).textContent).toContain(
      "minutes must be between 1 and 1440",
    );
  });
});
