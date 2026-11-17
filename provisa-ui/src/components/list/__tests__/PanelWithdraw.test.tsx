// Copyright (c) 2026 Kenneth Stott
// Canary: 9f3b6d28-4e1a-4c75-b8d2-1a7e5c04f963
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a detail panel's Withdraw confirms, saying what stops; Put back asks nothing; a refusal
// is said where the panel's Delete says one.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../../../test-utils/render";
import { PanelWithdraw } from "../PanelWithdraw";

const CONSEQUENCE = "No role reads it, no engine attaches it, and its replica is removed.";

function panel(withdrawn: boolean, onWithdraw = vi.fn(), onPutBack = vi.fn()) {
  render(
    <PanelWithdraw
      name="orders"
      withdrawn={withdrawn}
      consequence={CONSEQUENCE}
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

  it("puts a withdrawn object back at once, asking nothing", async () => {
    const { onWithdraw, onPutBack } = panel(true, vi.fn(), vi.fn().mockResolvedValue(undefined));
    expect(screen.queryByTestId("item-withdraw")).not.toBeInTheDocument();
    await userEvent.click(screen.getByTestId("item-put-back"));
    await waitFor(() => expect(onPutBack).toHaveBeenCalledTimes(1));
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
});
