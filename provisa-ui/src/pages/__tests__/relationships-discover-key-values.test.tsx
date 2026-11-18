// Copyright (c) 2026 Kenneth Stott
// Canary: 4b8e1d63-2a95-4f07-9c3e-7d1f5a0b2e86
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413 (amended 2026-11-16): Key value overlap is a choice in the discovery dialog that shows
// once Semantics is ticked, chosen by default. Chosen, it shows its measure cutoff (default 20%)
// and its sample size (default 10,000 distinct values per column). All three start as the person
// last set them; "Reset to defaults" restores the defaults; the run's summary lists it and states
// both settings. A suggestion admitted on its measure says so.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor } from "@testing-library/react";
import { type DiscoveryScope } from "../../api/admin";
import {
  SETTINGS_DEFAULT,
  REPORT,
  SCOPE,
  SLOW,
  UNMEASURED,
  settingsMock,
  lastScope,
  openDialog,
  proposal,
  proposalsMock,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  semanticsBox,
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

const keyValuesBox = () =>
  screen.queryByTestId("discover-kind-key-values") as HTMLInputElement | null;
const cutoff = () => screen.queryByTestId("discover-measure-cutoff") as HTMLInputElement | null;
const sampleSize = () => screen.queryByTestId("discover-sample-size") as HTMLInputElement | null;

describe("Discovery dialog: Key value overlap", () => {
  it("shows under Semantics, chosen, with its cutoff at 20% and sample size at 10,000", async () => {
    await openDialog();
    expect(keyValuesBox()).not.toBeNull();
    expect(keyValuesBox()?.checked).toBe(true);
    expect(screen.getByLabelText("Key value overlap")).toBe(keyValuesBox());
    expect(cutoff()?.value).toBe("20%");
    expect(sampleSize()?.value).toBe("10,000");
    await waitFor(() => expect(lastScope()).toEqual(SCOPE), SLOW);
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalledWith(SCOPE));
  });

  it("is offered only while Semantics is ticked", async () => {
    await openDialog();
    fireEvent.click(semanticsBox());
    expect(keyValuesBox()).toBeNull();
    expect(cutoff()).toBeNull();
    expect(sampleSize()).toBeNull();
    await waitFor(
      () => expect(lastScope()).toEqual({ ...SCOPE, ...UNMEASURED, kinds: ["foreign_keys"] }),
      SLOW,
    );
  });

  it("unticked, hides its two settings and the run measures nothing", async () => {
    await openDialog();
    fireEvent.click(keyValuesBox() as HTMLInputElement);
    expect(cutoff()).toBeNull();
    expect(sampleSize()).toBeNull();
    const unmeasured: DiscoveryScope = {
      ...SCOPE,
      ...UNMEASURED,
      kinds: ["foreign_keys", "semantics"],
    };
    await waitFor(() => expect(lastScope()).toEqual(unmeasured), SLOW);
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalledWith(unmeasured));
  });

  it("sends the cutoff and sample size set", async () => {
    await openDialog();
    fireEvent.change(cutoff() as HTMLInputElement, { target: { value: "35" } });
    fireEvent.change(sampleSize() as HTMLInputElement, { target: { value: "500" } });
    const set = { ...SCOPE, measure_cutoff: 0.35, sample_size: 500 };
    await waitFor(() => expect(lastScope()).toEqual(set), SLOW);
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalledWith(set));
  });

  it("starts as the person last set it", async () => {
    settingsMock.mockResolvedValue({
      ...SETTINGS_DEFAULT,
      key_values: {
        chosen: false,
        cutoff: 0.4,
        sample_size: 2500,
        contained_at: 0.9,
        shared_ceiling: 0.5,
      },
      defaults: SETTINGS_DEFAULT,
    });
    await openDialog();
    expect(keyValuesBox()?.checked).toBe(false);
    fireEvent.click(keyValuesBox() as HTMLInputElement);
    expect(cutoff()?.value).toBe("40%");
    expect(sampleSize()?.value).toBe("2,500");
  });

  it("New run takes the ended run's settings; Reset to defaults restores the defaults", async () => {
    const ran: DiscoveryScope = { ...SCOPE, measure_cutoff: 0.3, sample_size: 2000 };
    settingsMock.mockResolvedValue({
      ...SETTINGS_DEFAULT,
      key_values: {
        chosen: false,
        cutoff: 0.5,
        sample_size: 700,
        contained_at: 0.9,
        shared_ceiling: 0.5,
      },
      defaults: SETTINGS_DEFAULT,
    });
    runMock.mockResolvedValue(runOf({ status: "done", scope: ran, report: REPORT }));
    startMock.mockResolvedValue(runOf({ scope: ran }));
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    expect(keyValuesBox()?.checked).toBe(true);
    expect(cutoff()?.value).toBe("30%");
    expect(sampleSize()?.value).toBe("2,000");
    fireEvent.click(screen.getByTestId("discover-reset-defaults"));
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    expect(keyValuesBox()?.checked).toBe(true);
    expect(cutoff()?.value).toBe("20%");
    expect(sampleSize()?.value).toBe("10,000");
  });

  it("a suggestion admitted on its measure says it was admitted on matching key values", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
    proposalsMock.mockResolvedValue({
      proposals: [proposal({ confidence: 0.45, score: 0.95, admitted_on_key_values: true })],
      threshold: 0.7,
      kept_at: 0.7,
      score_filter: null,
    });
    await openDialog();
    fireEvent.click(runButton());
    const row = await screen.findByTestId("proposal-501", {}, SLOW);
    expect(row).toHaveTextContent(
      "Admitted on key values matching in the data; the measure is shown beside the model's confidence.",
    );
  });
});
