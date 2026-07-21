// Copyright (c) 2026 Kenneth Stott
// Canary: 749d45e7-3c6f-4e22-93b2-c23b1c4b4f9d
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-962: the calendar-create modal exposes the base-system-specific anchors — fiscal (start
// month/day) and retail 4-4-5 (reference year start date) — so a non-Gregorian calendar is fully
// configurable, not silently defaulted to Jan 1.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { CalendarCreateModal } from "../CalendarCreateModal";

function renderModal(initialBaseSystem?: string) {
  render(
    <CalendarCreateModal
      opened
      onClose={vi.fn()}
      onCreated={vi.fn()}
      initialBaseSystem={initialBaseSystem}
    />,
  );
}

describe("CalendarCreateModal — base-system anchors (REQ-962)", () => {
  it("shows no anchor fields for a Gregorian calendar", async () => {
    renderModal("gregorian");
    expect(await screen.findByTestId("calendar-base-system")).toBeInTheDocument();
    expect(screen.queryByTestId("calendar-fiscal-month")).not.toBeInTheDocument();
    expect(screen.queryByTestId("calendar-retail-anchor")).not.toBeInTheDocument();
  });

  it("reveals the fiscal start month + day for a Fiscal calendar", async () => {
    renderModal("fiscal");
    expect(await screen.findByTestId("calendar-fiscal-month")).toBeInTheDocument();
    expect(screen.getByTestId("calendar-fiscal-day")).toBeInTheDocument();
    expect(screen.queryByTestId("calendar-retail-anchor")).not.toBeInTheDocument();
    // fiscal has a sensible default (Jan 1) → Create is not blocked on an anchor
    fireEvent.change(screen.getByTestId("calendar-name"), { target: { value: "fy-us" } });
    expect(screen.getByTestId("calendar-create-submit")).not.toBeDisabled();
  });

  it("requires a reference date for a Retail 4-4-5 calendar", async () => {
    renderModal("retail_445");
    fireEvent.change(await screen.findByTestId("calendar-name"), {
      target: { value: "retail-2026" },
    });
    expect(screen.getByTestId("calendar-retail-anchor")).toBeInTheDocument();
    expect(screen.queryByTestId("calendar-fiscal-month")).not.toBeInTheDocument();
    // no anchor yet → Create is blocked (a retail calendar is unusable without it)
    expect(screen.getByTestId("calendar-create-submit")).toBeDisabled();
  });

  it("exposes holiday + weekend editors", async () => {
    renderModal("gregorian");
    expect(await screen.findByTestId("calendar-holidays")).toBeInTheDocument();
    expect(screen.getByTestId("calendar-weekend")).toBeInTheDocument();
  });

  it("has no delete button in create mode", async () => {
    renderModal("gregorian");
    await screen.findByTestId("calendar-base-system");
    expect(screen.queryByTestId("calendar-delete")).not.toBeInTheDocument();
  });

  it("edit mode pre-fills, locks the name, and offers delete", async () => {
    render(
      <CalendarCreateModal
        opened
        onClose={vi.fn()}
        editCalendar={{
          name: "fiscal-us",
          version: "v2",
          baseSystem: "fiscal",
          tz: "America/New_York",
          fiscalAnchorMonth: 10,
          fiscalAnchorDay: 1,
          retailAnchor: null,
          weekStart: 0,
          holidays: ["2026-07-03"],
          weekend: [5, 6],
        }}
      />,
    );
    const nameInput = (await screen.findByTestId("calendar-name")) as HTMLInputElement;
    expect(nameInput.value).toBe("fiscal-us");
    expect(nameInput).toHaveAttribute("readonly"); // name is the identity in edit mode
    expect(screen.getByTestId("calendar-delete")).toBeInTheDocument();
    // the fiscal anchor was pre-selected from the edited calendar
    expect(screen.getByTestId("calendar-fiscal-month")).toBeInTheDocument();
  });
});
