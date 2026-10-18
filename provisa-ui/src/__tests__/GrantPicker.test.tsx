// Copyright (c) 2026 Kenneth Stott
// Canary: aa0daf2d-39c5-40fb-bc68-7ec32c448592
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A grant list with composite entries: one pill reads "analyst and region__eu", and Combine adds
// an entry a reader must hold every role of.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";

import { GrantPicker } from "../components/GrantPicker";

const ROLES = [
  { id: "analyst", label: "analyst" },
  { id: "marketing", label: "marketing" },
];

describe("GrantPicker", () => {
  it("shows a composite entry as one pill naming every role it needs", () => {
    render(
      <GrantPicker
        options={ROLES}
        value={["marketing", "analyst+region__eu"]}
        onChange={vi.fn()}
        ariaLabel="Visible to"
      />,
    );
    expect(screen.getAllByText("analyst and region__eu").length).toBeGreaterThan(0);
    expect(screen.getAllByText("marketing").length).toBeGreaterThan(0);
  });

  it("combines roles, a derived role typed in among them, into one entry", async () => {
    const onChange = vi.fn();
    render(
      <GrantPicker
        options={ROLES}
        value={["marketing"]}
        onChange={onChange}
        ariaLabel="Visible to"
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Combine roles into one grant" }));
    const parts = await screen.findByRole("textbox", { name: "Roles a reader must all hold" });
    await userEvent.type(parts, "analyst,region__eu,");
    // jsdom reports the popover's content hidden once the parts' suggestion list has opened (the
    // browser shows it, beside the open list); the button is still the one the reader clicks.
    const addButton = screen.getByRole("button", { name: "Add", hidden: true });
    await userEvent.click(addButton);
    await waitFor(() => expect(onChange).toHaveBeenCalledWith(["marketing", "analyst+region__eu"]));
  });

  it("offers nothing to add until a role is listed", async () => {
    render(<GrantPicker options={ROLES} value={[]} onChange={vi.fn()} ariaLabel="Visible to" />);
    await userEvent.click(screen.getByRole("button", { name: "Combine roles into one grant" }));
    expect(await screen.findByRole("button", { name: "Add" })).toBeDisabled();
  });
});
