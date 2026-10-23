// Copyright (c) 2026 Kenneth Stott
// Canary: f139a62b-3343-4196-8659-9afc4b3d24f2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The one tooltip (components/Tip.tsx): what it shows, when, and what it leaves alone.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { ActionIcon, Menu } from "@mantine/core";
import { fireEvent, render, screen, waitFor } from "../../test-utils/render";
import { Tip, TipSurface } from "../Tip";

describe("Tip", () => {
  it("mounts nothing until the target is hovered or focused", () => {
    render(
      <Tip text="Copies the address">
        <ActionIcon aria-label="Copy">c</ActionIcon>
      </Tip>,
    );
    expect(screen.queryByRole("tooltip", { hidden: true })).toBeNull();
    expect(screen.getByLabelText("Copy")).not.toHaveAttribute("aria-describedby");
  });

  it("shows its text on hover, describes the target and keeps the target's name", async () => {
    render(
      <Tip text="Copies the address">
        <ActionIcon aria-label="Copy">c</ActionIcon>
      </Tip>,
    );
    const target = screen.getByLabelText("Copy");
    fireEvent.mouseEnter(target);
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("Copies the address");
    expect(tip).toHaveClass("tip-surface");
    // Described by the hint, named as before: a screen reader says "Copy", then the hint.
    expect(target).toHaveAttribute("aria-label", "Copy");
    const described = document.getElementById(target.getAttribute("aria-describedby") as string);
    expect(described).toHaveTextContent("Copies the address");
    expect(tip).toContainElement(described);
    expect(target).not.toHaveAttribute("title");
  });

  it("closes when the pointer leaves, and on Escape", async () => {
    render(
      <Tip text="Copies the address">
        <ActionIcon aria-label="Copy">c</ActionIcon>
      </Tip>,
    );
    const target = screen.getByLabelText("Copy");
    fireEvent.mouseEnter(target);
    await screen.findByRole("tooltip");
    fireEvent.mouseLeave(target);
    await waitFor(() => expect(screen.queryByRole("tooltip")).toBeNull());
    fireEvent.mouseEnter(target);
    await screen.findByRole("tooltip");
    fireEvent.keyDown(target, { key: "Escape" });
    await waitFor(() => expect(screen.queryByRole("tooltip")).toBeNull());
  });

  it("opens at once on keyboard focus and closes on blur", async () => {
    render(
      <Tip text="Copies the address">
        <ActionIcon aria-label="Copy">c</ActionIcon>
      </Tip>,
    );
    const target = screen.getByLabelText("Copy");
    // jsdom has no keyboard modality: say what a keyboard focus looks like to the component.
    const matches = vi.spyOn(target, "matches").mockImplementation((s) => s === ":focus-visible");
    fireEvent.focus(target);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Copies the address");
    fireEvent.blur(target);
    await waitFor(() => expect(screen.queryByRole("tooltip")).toBeNull());
    matches.mockRestore();
  });

  it("does not open on a focus the pointer made", async () => {
    render(
      <Tip text="Copies the address">
        <ActionIcon aria-label="Copy">c</ActionIcon>
      </Tip>,
    );
    const target = screen.getByLabelText("Copy");
    const matches = vi.spyOn(target, "matches").mockReturnValue(false);
    fireEvent.focus(target);
    expect(screen.queryByRole("tooltip")).toBeNull();
    matches.mockRestore();
  });

  it("lets a menu's trigger through: a control with a hint still opens its menu", async () => {
    const own: string[] = [];
    render(
      <Menu>
        <Menu.Target>
          <Tip text="More actions">
            <ActionIcon aria-label="More" onClick={() => own.push("click")}>
              m
            </ActionIcon>
          </Tip>
        </Menu.Target>
        <Menu.Dropdown>
          <Menu.Item>Rename</Menu.Item>
        </Menu.Dropdown>
      </Menu>,
    );
    fireEvent.click(screen.getByLabelText("More"));
    expect(await screen.findByText("Rename")).toBeInTheDocument();
    // The trigger's handler and the control's own both ran.
    expect(own).toEqual(["click"]);
    expect(screen.getByLabelText("More")).toHaveAttribute("aria-haspopup");
  });

  it("runs the control's own click before a controlled menu's, so a control that toggles the menu itself still opens it", async () => {
    // The shape of the graph frame's download button: the menu is controlled, and the control
    // flips the same state in its own handler. Whichever order the two run in must leave it open.
    function Controlled() {
      const [opened, setOpened] = useState(false);
      return (
        <Menu opened={opened} onChange={setOpened}>
          <Menu.Target>
            <Tip text="Download">
              <ActionIcon aria-label="Download" onClick={() => setOpened((v) => !v)}>
                d
              </ActionIcon>
            </Tip>
          </Menu.Target>
          <Menu.Dropdown>
            <Menu.Item>CSV</Menu.Item>
          </Menu.Dropdown>
        </Menu>
      );
    }
    render(<Controlled />);
    fireEvent.click(screen.getByLabelText("Download"));
    expect(await screen.findByText("CSV")).toBeInTheDocument();
  });

  it("leaves the target's own handlers and its click working", async () => {
    const seen: string[] = [];
    render(
      <Tip text="Copies the address">
        <ActionIcon
          aria-label="Copy"
          onMouseEnter={() => seen.push("enter")}
          onClick={() => seen.push("click")}
        >
          c
        </ActionIcon>
      </Tip>,
    );
    const target = screen.getByLabelText("Copy");
    fireEvent.mouseEnter(target);
    fireEvent.click(target);
    expect(seen).toEqual(["enter", "click"]);
  });

  it("held open from its first render, shows once its target has mounted, without throwing", async () => {
    render(
      <TipSurface opened content="Pointed at">
        <div data-testid="placed" />
      </TipSurface>,
    );
    expect(await screen.findByText("Pointed at")).toBeInTheDocument();
  });
});
