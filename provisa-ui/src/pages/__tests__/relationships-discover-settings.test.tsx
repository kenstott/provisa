// Copyright (c) 2026 Kenneth Stott
// Canary: 9752f7d5-55c9-47fa-b02f-17b3aee99fe8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413 (amended 2026-11-18): every threshold and sample size a discovery run uses is shown in the
// dialog with its value and can be changed before the run starts: Tick from, the sample values per
// column, and Key value overlap's cutoff, sample size, key match share and shared value ceiling.
// Each starts as the person last set it; "Reset to defaults" restores the defaults.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor } from "@testing-library/react";
import { type DiscoveryScope } from "../../api/admin";
import {
  REPORT,
  SCOPE,
  SETTINGS_DEFAULT,
  SLOW,
  UNMEASURED,
  lastScope,
  openDialog,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  semanticsBox,
  settingsMock,
  startMock,
} from "./discoverDialog.fixtures";

vi.mock("../../hooks/useAdminQueries", async () =>
  (await import("./discoverDialog.state")).adminQueries(),
);
vi.mock("../../context/AuthContext", async () => {
  const { AUTH } = await import("./discoverDialog.state");
  return { useAuth: () => AUTH };
});
vi.mock("../../context/DomainFilterContext", async () => {
  const { FILTER } = await import("./discoverDialog.state");
  return { useDomainFilter: () => FILTER };
});
vi.mock("../../api/admin", async (importOriginal) =>
  (await import("./discoverDialog.state")).adminApi(
    await importOriginal<typeof import("../../api/admin")>(),
  ),
);
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));

beforeEach(resetDiscoveryMocks);

const field = (id: string) => screen.queryByTestId(id) as HTMLInputElement | null;
const threshold = () => field("discover-threshold");
const samples = () => field("discover-samples-per-column");
const containedAt = () => field("discover-contained-at");
const ceiling = () => field("discover-shared-ceiling");

describe("Discovery dialog: every threshold and sample size", () => {
  it("shows each at its default and sends each with the run", async () => {
    await openDialog();
    expect(threshold()?.value).toBe("70%");
    expect(samples()?.value).toBe("5");
    expect(containedAt()?.value).toBe("95%");
    expect(ceiling()?.value).toBe("60%");
    expect(screen.getByLabelText("Tick from")).toBe(threshold());
    expect(screen.getByLabelText("Sample values per column")).toBe(samples());
    expect(screen.getByLabelText("Key match from")).toBe(containedAt());
    expect(screen.getByLabelText("Shared values score at most")).toBe(ceiling());
    await waitFor(() => expect(lastScope()).toEqual(SCOPE), SLOW);
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalledWith(SCOPE));
  });

  it("sends each value set", async () => {
    await openDialog();
    fireEvent.change(threshold() as HTMLInputElement, { target: { value: "55" } });
    fireEvent.change(samples() as HTMLInputElement, { target: { value: "8" } });
    fireEvent.change(containedAt() as HTMLInputElement, { target: { value: "85" } });
    fireEvent.change(ceiling() as HTMLInputElement, { target: { value: "40" } });
    const set: DiscoveryScope = {
      ...SCOPE,
      threshold: 0.55,
      samples_per_column: 8,
      contained_at: 0.85,
      shared_ceiling: 0.4,
    };
    await waitFor(() => expect(lastScope()).toEqual(set), SLOW);
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalledWith(set));
  });

  it("offers Tick from without Semantics and the semantic settings only with it", async () => {
    await openDialog();
    fireEvent.click(semanticsBox());
    expect(threshold()).not.toBeNull();
    expect(samples()).toBeNull();
    expect(containedAt()).toBeNull();
    expect(ceiling()).toBeNull();
    await waitFor(
      () => expect(lastScope()).toEqual({ ...SCOPE, ...UNMEASURED, kinds: ["foreign_keys"] }),
      SLOW,
    );
  });

  it("starts each as the person last set it; Reset to defaults restores the defaults", async () => {
    const own = {
      threshold: 0.6,
      samples_per_column: 3,
      key_values: {
        chosen: true,
        cutoff: 0.25,
        sample_size: 800,
        contained_at: 0.9,
        shared_ceiling: 0.5,
      },
    };
    settingsMock.mockResolvedValue({ ...own, defaults: SETTINGS_DEFAULT });
    const ran: DiscoveryScope = {
      ...SCOPE,
      threshold: 0.6,
      samples_per_column: 3,
      measure_cutoff: 0.25,
      sample_size: 800,
      contained_at: 0.9,
      shared_ceiling: 0.5,
    };
    runMock.mockResolvedValue(runOf({ status: "done", scope: ran, report: REPORT }));
    startMock.mockResolvedValue(runOf({ scope: ran }));
    await openDialog();
    expect(threshold()?.value).toBe("60%");
    expect(samples()?.value).toBe("3");
    expect(containedAt()?.value).toBe("90%");
    expect(ceiling()?.value).toBe("50%");
    await waitFor(() => expect(lastScope()).toEqual(ran), SLOW);
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    expect(threshold()?.value).toBe("60%");
    expect(containedAt()?.value).toBe("90%");
    fireEvent.click(screen.getByTestId("discover-reset-defaults"));
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    expect(threshold()?.value).toBe("70%");
    expect(samples()?.value).toBe("5");
    expect(containedAt()?.value).toBe("95%");
    expect(ceiling()?.value).toBe("60%");
  });
});
