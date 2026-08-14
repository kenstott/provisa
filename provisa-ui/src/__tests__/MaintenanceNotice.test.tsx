// Copyright (c) 2026 Kenneth Stott
// Canary: 4d0b91f7-58ac-4e26-b31f-9a7c2e6d08b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1466: the deployment-wide scheduled-downtime notice, both halves — the banner every user
// sees and the platform admin's on/off control. The cases that matter are the ones where getting it
// wrong misinforms: a banner that survives its own poll failing (a transport error mid-window is
// not evidence the window ended), a banner that admits it has no estimate rather than implying an
// imminent return, and a "clear" that sends active:false rather than merely blanking the message.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { MaintenanceBanner } from "../components/MaintenanceBanner";
import { MaintenanceTab } from "../components/admin/MaintenanceTab";
import type { MaintenanceNotice } from "../api/maintenance";

vi.mock("../api/maintenance", () => ({
  fetchMaintenanceNotice: vi.fn(),
  setMaintenanceNotice: vi.fn(),
}));

import { fetchMaintenanceNotice, setMaintenanceNotice } from "../api/maintenance";
const mockFetch = vi.mocked(fetchMaintenanceNotice);
const mockSet = vi.mocked(setMaintenanceNotice);

function notice(overrides: Partial<MaintenanceNotice> = {}): MaintenanceNotice {
  return {
    active: true,
    message: "Provisa is undergoing scheduled maintenance.",
    ends_at: null,
    started_at: "2026-08-14T10:00:00+00:00",
    ...overrides,
  };
}

describe("MaintenanceBanner", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
  });

  it("shows nothing when no window is open", async () => {
    mockFetch.mockResolvedValue(notice({ active: false, message: null, started_at: null }));
    render(<MaintenanceBanner />);
    await waitFor(() => expect(mockFetch).toHaveBeenCalled());
    expect(screen.queryByTestId("maintenance-banner")).toBeNull();
  });

  it("states that no end time was given rather than implying one", async () => {
    mockFetch.mockResolvedValue(notice());
    render(<MaintenanceBanner />);
    const banner = await screen.findByTestId("maintenance-banner");
    expect(banner.textContent).toContain("No end time has been given yet.");
  });

  it("reports the expected return when one was set", async () => {
    const endsAt = new Date("2026-08-14T12:30:00Z");
    mockFetch.mockResolvedValue(notice({ ends_at: endsAt.toISOString() }));
    render(<MaintenanceBanner />);
    const banner = await screen.findByTestId("maintenance-banner");
    expect(banner.textContent).toContain(`Expected back by ${endsAt.toLocaleString()}.`);
  });

  it("keeps the banner up when a poll fails", async () => {
    vi.useFakeTimers();
    try {
      mockFetch.mockResolvedValueOnce(notice());
      render(<MaintenanceBanner />);
      await vi.waitFor(() => expect(screen.queryByTestId("maintenance-banner")).not.toBeNull());
      // The window is still open; only the transport broke.
      mockFetch.mockRejectedValue(new Error("network"));
      await vi.advanceTimersByTimeAsync(60_000);
      expect(screen.queryByTestId("maintenance-banner")).not.toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("MaintenanceTab", () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
  });

  it("turns the banner on with the standard wording when none is supplied", async () => {
    mockFetch.mockResolvedValue(notice({ active: false, message: null, started_at: null }));
    mockSet.mockResolvedValue(notice());
    render(<MaintenanceTab />);
    fireEvent.click(await screen.findByTestId("maintenance-on"));
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    // A null message is what tells the server to use its own wording — an empty string would be a
    // banner with nothing in it.
    expect(mockSet).toHaveBeenCalledWith({ active: true, message: null, ends_at: null });
  });

  it("sends the administrator's own wording when one is typed", async () => {
    mockFetch.mockResolvedValue(notice({ active: false, message: null, started_at: null }));
    mockSet.mockResolvedValue(notice({ message: "Swapping the engine cluster." }));
    render(<MaintenanceTab />);
    fireEvent.change(await screen.findByTestId("maintenance-message"), {
      target: { value: "Swapping the engine cluster." },
    });
    fireEvent.click(screen.getByTestId("maintenance-on"));
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    expect(mockSet.mock.calls[0][0].message).toBe("Swapping the engine cluster.");
  });

  it("turns the banner off rather than blanking it", async () => {
    mockFetch.mockResolvedValue(notice());
    mockSet.mockResolvedValue(notice({ active: false, started_at: null }));
    render(<MaintenanceTab />);
    const off = await screen.findByTestId("maintenance-off");
    await waitFor(() => expect(off).not.toBeDisabled());
    fireEvent.click(off);
    await waitFor(() => expect(mockSet).toHaveBeenCalled());
    expect(mockSet.mock.calls[0][0].active).toBe(false);
  });

  it("offers nothing to clear when no window is open", async () => {
    mockFetch.mockResolvedValue(notice({ active: false, message: null, started_at: null }));
    render(<MaintenanceTab />);
    await waitFor(() => expect(mockFetch).toHaveBeenCalled());
    expect(screen.getByTestId("maintenance-off")).toBeDisabled();
  });

  it("surfaces a rejected write instead of implying the banner changed", async () => {
    mockFetch.mockResolvedValue(notice({ active: false, message: null, started_at: null }));
    mockSet.mockRejectedValue(new Error("platform_settings capability required"));
    render(<MaintenanceTab />);
    fireEvent.click(await screen.findByTestId("maintenance-on"));
    const error = await screen.findByTestId("maintenance-error");
    expect(error.textContent).toContain("platform_settings capability required");
  });
});
