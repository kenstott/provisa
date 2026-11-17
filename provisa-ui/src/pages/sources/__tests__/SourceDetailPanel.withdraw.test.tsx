// Copyright (c) 2026 Kenneth Stott
// Canary: 4b8a2e61-7d3c-4f19-a6e0-5c2d9b17f384
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a source's Withdraw, among its actions, confirmed naming what stops; Put back once
// withdrawn, and the panel says it is withdrawn.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import type { Source } from "../../../types/admin";

vi.mock("../../../hooks/useAdminQueries", () => ({
  useRefreshKaggleSource: () => ({ refreshKaggleSource: vi.fn(), loading: false }),
}));

import { SourceDetailPanel } from "../SourceDetailPanel";

function panel(withdrawn: boolean) {
  const s = { id: "warehouse", type: "postgresql", withdrawn } as unknown as Source;
  const onWithdraw = vi.fn().mockResolvedValue(undefined);
  const onPutBack = vi.fn().mockResolvedValue(undefined);
  render(
    <SourceDetailPanel
      s={s}
      fields={null}
      onEdit={vi.fn()}
      onDuplicate={vi.fn()}
      onNavigate={vi.fn()}
      onDelete={vi.fn()}
      onWithdraw={onWithdraw}
      onPutBack={onPutBack}
      putBackRegions={null}
    />,
  );
  return { onWithdraw, onPutBack };
}

describe("SourceDetailPanel — Withdraw and Put back", () => {
  it("withdraws the source once confirmed, beside its other actions", async () => {
    const { onWithdraw, onPutBack } = panel(false);
    const control = screen.getByTestId("source-detail-withdraw");
    expect(control.closest(".mantine-Group-root")).toBe(
      screen.getByTestId("source-detail-delete").closest(".mantine-Group-root"),
    );
    expect(screen.queryByTestId("source-detail-withdrawn-note")).not.toBeInTheDocument();
    fireEvent.click(control);
    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveTextContent("Withdraw warehouse?");
    expect(dialog).toHaveTextContent(
      "No role reads any of its tables, no engine attaches it, and its tables' replicas are removed.",
    );
    fireEvent.click(screen.getByTestId("source-detail-confirm-dialog-confirm"));
    await waitFor(() => expect(onWithdraw).toHaveBeenCalledTimes(1));
    expect(onPutBack).not.toHaveBeenCalled();
  });

  it("puts a withdrawn source back at once, and says it is withdrawn", async () => {
    const { onWithdraw, onPutBack } = panel(true);
    expect(screen.getByTestId("source-detail-withdrawn-note")).toHaveTextContent(
      "Withdrawn: none of its tables serves any role until it is put back.",
    );
    expect(screen.queryByTestId("source-detail-withdraw")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("source-detail-put-back"));
    await waitFor(() => expect(onPutBack).toHaveBeenCalledTimes(1));
    expect(onWithdraw).not.toHaveBeenCalled();
  });
});
