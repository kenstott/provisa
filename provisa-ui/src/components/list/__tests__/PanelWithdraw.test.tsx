// Copyright (c) 2026 Kenneth Stott
// Canary: 9f3b6d28-4e1a-4c75-b8d2-1a7e5c04f963
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a detail panel's Withdraw confirms, saying what stops; Put back names a region the grant
// covers (nothing where no region is declared); a refusal is said where the panel's Delete says one.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../../../test-utils/render";
import { PanelWithdraw } from "../PanelWithdraw";
import type { PutBackRegions } from "../../../hooks/useWithdrawal";

/** The options a select offers, opened. */
async function optionsOf(select: HTMLElement): Promise<HTMLElement> {
  await userEvent.click(select);
  const list = document.getElementById(select.getAttribute("aria-controls") ?? "");
  if (list === null) throw new Error("the region field opened no option list");
  return list;
}

const CONSEQUENCE = "No role reads it, no engine attaches it, and its replica is removed.";

function panel(
  withdrawn: boolean,
  onWithdraw = vi.fn(),
  onPutBack = vi.fn(),
  putBackRegions: PutBackRegions | null = null,
  region: string | null = null,
) {
  render(
    <PanelWithdraw
      name="orders"
      withdrawn={withdrawn}
      consequence={CONSEQUENCE}
      region={region}
      putBackRegions={putBackRegions}
      scope="table"
      onWithdraw={onWithdraw}
      onPutBack={onPutBack}
      testId="item"
    />,
  );
  return { onWithdraw, onPutBack };
}

describe("PanelWithdraw", () => {
  it("withdraws only once the reader confirms, told what stops", async () => {
    const { onWithdraw, onPutBack } = panel(false, vi.fn().mockResolvedValue(undefined));
    expect(screen.queryByTestId("item-put-back")).not.toBeInTheDocument();
    await userEvent.click(screen.getByTestId("item-withdraw"));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText("Withdraw orders?")).toBeInTheDocument();
    expect(within(dialog).getByText(CONSEQUENCE)).toBeInTheDocument();
    expect(onWithdraw).not.toHaveBeenCalled();
    await userEvent.click(within(dialog).getByTestId("item-confirm-dialog-confirm"));
    await waitFor(() => expect(onWithdraw).toHaveBeenCalledTimes(1));
    expect(onPutBack).not.toHaveBeenCalled();
  });

  it("cancelled, withdraws nothing", async () => {
    const { onWithdraw } = panel(false);
    await userEvent.click(screen.getByTestId("item-withdraw"));
    const dialog = await screen.findByRole("dialog");
    await userEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(onWithdraw).not.toHaveBeenCalled();
  });

  it("puts a withdrawn object back at once, asking nothing, where no region is declared", async () => {
    const { onWithdraw, onPutBack } = panel(true, vi.fn(), vi.fn().mockResolvedValue(undefined));
    expect(screen.queryByTestId("item-withdraw")).not.toBeInTheDocument();
    await userEvent.click(screen.getByTestId("item-put-back"));
    await waitFor(() => expect(onPutBack).toHaveBeenCalledWith(null));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(onWithdraw).not.toHaveBeenCalled();
  });

  it("says why a refused Put back did not go through", async () => {
    panel(true, vi.fn(), vi.fn().mockRejectedValue(new Error("Table orders not found")));
    await userEvent.click(screen.getByTestId("item-put-back"));
    expect(await screen.findByTestId("item-problem")).toHaveTextContent("Table orders not found");
    expect(screen.getByTestId("item-put-back")).toBeEnabled();
  });

  it("says why a refused Withdraw did not go through", async () => {
    panel(false, vi.fn().mockRejectedValue(new Error("No right to withdraw orders")));
    await userEvent.click(screen.getByTestId("item-withdraw"));
    const dialog = await screen.findByRole("dialog");
    await userEvent.click(within(dialog).getByTestId("item-confirm-dialog-confirm"));
    expect(await screen.findByTestId("item-problem")).toHaveTextContent(
      "No right to withdraw orders",
    );
  });

  it("asks the region to put it back into, among those the grant covers, starting where it was", async () => {
    const onPutBack = vi.fn().mockResolvedValue(undefined);
    panel(true, vi.fn(), onPutBack, { regions: ["eu", "us"], noRegion: false }, "us");
    await userEvent.click(screen.getByTestId("item-put-back"));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText("Put back orders")).toBeInTheDocument();
    const select = within(dialog).getByTestId("item-put-back-region");
    expect(select).toHaveValue("us");
    const list = await optionsOf(select);
    const options = (await within(list).findAllByRole("option", { hidden: true })).map(
      (o) => o.textContent,
    );
    expect(options).toEqual(["eu", "us"]);
    await userEvent.click(within(list).getByRole("option", { name: "eu", hidden: true }));
    await userEvent.click(within(dialog).getByTestId("item-put-back-confirm"));
    await waitFor(() => expect(onPutBack).toHaveBeenCalledWith("eu"));
  });

  it("starts on a covered region when the one it was in is not covered, and offers no region if covered", async () => {
    const onPutBack = vi.fn().mockResolvedValue(undefined);
    panel(true, vi.fn(), onPutBack, { regions: ["eu"], noRegion: true }, "apac");
    await userEvent.click(screen.getByTestId("item-put-back"));
    const dialog = await screen.findByRole("dialog");
    const select = within(dialog).getByTestId("item-put-back-region");
    expect(select).toHaveValue("eu");
    const list = await optionsOf(select);
    const options = (await within(list).findAllByRole("option", { hidden: true })).map(
      (o) => o.textContent,
    );
    expect(options).toEqual(["eu", "No region"]);
  });

  it("puts back nothing when the grant covers no region", async () => {
    const onPutBack = vi.fn();
    panel(true, vi.fn(), onPutBack, { regions: [], noRegion: false }, "eu");
    await userEvent.click(screen.getByTestId("item-put-back"));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByTestId("item-put-back-no-region")).toBeInTheDocument();
    expect(within(dialog).getByTestId("item-put-back-confirm")).toBeDisabled();
    expect(onPutBack).not.toHaveBeenCalled();
  });
});
