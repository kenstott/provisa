// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9e5d27-6c41-4f8a-b2d0-7e1a9c4f6d85
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The standard multi-value picker: an option that cannot be picked is shown, and says why --
// one reason for every such option, or each option its own.

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../test-utils/render";
import { MultiSelect } from "../MultiSelect";

describe("an option that cannot be picked", () => {
  it("says its own reason, the way any option that cannot be picked says why", async () => {
    const onChange = vi.fn();
    render(
      <MultiSelect
        label="Tables"
        options={[
          { id: "orders", label: "orders" },
          { id: "events", label: "events", disabled: true, reason: "No column types found." },
          { id: "logs", label: "logs", disabled: true, reason: "Reading its columns." },
        ]}
        value={[]}
        onChange={onChange}
      />,
    );
    await userEvent.click(screen.getByRole("textbox", { name: "Tables" }));
    await userEvent.hover(await screen.findByTestId("option-not-held-events"));
    expect(await screen.findByRole("tooltip", { hidden: true })).toHaveTextContent(
      "No column types found.",
    );
    await userEvent.click(screen.getByTestId("option-not-held-events"));
    expect(onChange).not.toHaveBeenCalled();
  });

  it("takes the shared reason when it has none of its own", async () => {
    render(
      <MultiSelect
        label="Tables"
        options={[
          { id: "a", label: "a", disabled: true },
          { id: "b", label: "b", disabled: true, reason: "Its own." },
        ]}
        disabledHint="Shared."
        value={[]}
        onChange={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByRole("textbox", { name: "Tables" }));
    await userEvent.hover(await screen.findByTestId("option-not-held-a"));
    expect(await screen.findByRole("tooltip", { hidden: true })).toHaveTextContent("Shared.");
  });

  it("is shown without a reason mark when no reason is given", async () => {
    render(
      <MultiSelect
        label="Tables"
        options={[{ id: "a", label: "a", disabled: true }]}
        value={[]}
        onChange={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByRole("textbox", { name: "Tables" }));
    expect(await screen.findByRole("option", { name: "a", hidden: true })).toBeInTheDocument();
    expect(screen.queryByTestId("option-not-held-a")).toBeNull();
  });
});

describe("an option's action", () => {
  it("is a button beside the option in the open list, used without picking it", async () => {
    const onChange = vi.fn();
    const onRegister = vi.fn();
    render(
      <MultiSelect
        label="Tables"
        options={[
          { id: "orders", label: "orders" },
          {
            id: "events",
            label: "events",
            disabled: true,
            reason: "No column types found.",
            action: {
              label: "Register",
              ariaLabel: "Register events on its own",
              onClick: onRegister,
            },
          },
        ]}
        value={[]}
        onChange={onChange}
      />,
    );
    await userEvent.click(screen.getByRole("textbox", { name: "Tables" }));
    const action = await screen.findByTestId("option-action-events");
    expect(action).toHaveAccessibleName("Register events on its own");
    expect(screen.queryByTestId("option-action-orders")).toBeNull();
    await userEvent.click(action);
    expect(onRegister).toHaveBeenCalledTimes(1);
    expect(onChange).not.toHaveBeenCalled();
  });

  it("lists options under their group's heading", async () => {
    render(
      <MultiSelect
        label="Tables"
        options={[
          { id: "a", label: "a", group: "Tables" },
          { id: "b", label: "b", group: "Lookups" },
        ]}
        value={[]}
        onChange={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByRole("textbox", { name: "Tables" }));
    await screen.findAllByRole("option", { hidden: true });
    const headings = [...document.querySelectorAll(".mantine-MultiSelect-groupLabel")].map(
      (g) => g.textContent,
    );
    expect(headings).toEqual(["Tables", "Lookups"]);
  });
});
