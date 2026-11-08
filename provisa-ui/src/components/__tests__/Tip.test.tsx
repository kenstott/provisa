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

  it("keeps no floating element once closed, and the target stays described", async () => {
    render(
      <Tip text="Copies the address">
        <ActionIcon aria-label="Copy">c</ActionIcon>
      </Tip>,
    );
    const target = screen.getByLabelText("Copy");
    fireEvent.mouseEnter(target);
    await screen.findByRole("tooltip");
    fireEvent.mouseLeave(target);
    await waitFor(() => expect(screen.queryAllByRole("tooltip", { hidden: true })).toEqual([]));
    const described = document.getElementById(target.getAttribute("aria-describedby") as string);
    expect(described).toHaveTextContent("Copies the address");
    expect(described).not.toBeVisible();
    // Open again: there is one description, the one on the surface.
    fireEvent.mouseEnter(target);
    const tip = await screen.findByRole("tooltip");
    const all = document.querySelectorAll(`[id="${target.getAttribute("aria-describedby")}"]`);
    expect(all).toHaveLength(1);
    expect(tip).toContainElement(all[0] as HTMLElement);
  });

  it("does not mount a floating element for a target the pointer only crossed", () => {
    render(
      <Tip text="Copies the address">
        <ActionIcon aria-label="Copy">c</ActionIcon>
      </Tip>,
    );
    const target = screen.getByLabelText("Copy");
    fireEvent.mouseEnter(target);
    fireEvent.mouseLeave(target);
    expect(screen.queryAllByRole("tooltip", { hidden: true })).toEqual([]);
    const described = document.getElementById(target.getAttribute("aria-describedby") as string);
    expect(described).toHaveTextContent("Copies the address");
  });

  it("opens at its target when the target is an svg, not at the corner of the page", async () => {
    render(
      <Tip text="What this column holds" openDelay={0}>
        <svg data-testid="icon" width={10} height={10} />
      </Tip>,
    );
    const icon = screen.getByTestId("icon");
    vi.spyOn(icon, "getBoundingClientRect").mockReturnValue({
      x: 300,
      y: 200,
      left: 300,
      top: 200,
      right: 310,
      bottom: 210,
      width: 10,
      height: 10,
      toJSON: () => ({}),
    });
    fireEvent.mouseEnter(icon);
    const tip = await screen.findByRole("tooltip");
    await waitFor(() => expect(parseFloat(tip.style.left)).toBeGreaterThan(0));
    expect(parseFloat(tip.style.top)).toBeGreaterThan(0);
  });

  it("survives a layout change after the pointer has crossed more targets than React's update limit", async () => {
    // One floating element per armed target tracks its target's position, and each one that moved
    // commits synchronously: past 50 of those React throws "Maximum update depth exceeded".
    const rejected: unknown[] = [];
    const onRejected = (reason: unknown) => rejected.push(reason);
    process.on("unhandledRejection", onRejected);
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    let moved = 0;
    const rect = vi.spyOn(Element.prototype, "getBoundingClientRect").mockImplementation(() => {
      const top = 200 + moved;
      return {
        x: 300,
        y: top,
        left: 300,
        top,
        right: 310,
        bottom: top + 10,
        width: 10,
        height: 10,
        toJSON: () => ({}),
      };
    });
    try {
      render(
        <>
          {Array.from({ length: 120 }, (_, i) => (
            <Tip key={i} text={`Hint ${i}`}>
              <button data-testid={`t${i}`}>x</button>
            </Tip>
          ))}
        </>,
      );
      for (let i = 0; i < 120; i++) {
        const t = screen.getByTestId(`t${i}`);
        fireEvent.mouseEnter(t);
        fireEvent.mouseLeave(t);
      }
      await new Promise((r) => setTimeout(r, 200));
      for (let n = 0; n < 4; n++) {
        moved += 37;
        window.dispatchEvent(new Event("resize"));
        await new Promise((r) => setTimeout(r, 100));
      }
    } finally {
      process.off("unhandledRejection", onRejected);
      rect.mockRestore();
    }
    const loops = errors.mock.calls.filter((c) => String(c[0]).includes("Maximum update depth"));
    errors.mockRestore();
    expect(loops).toEqual([]);
    expect(rejected.map(String)).toEqual([]);
  });

  it("held open from its first render, shows once its target has mounted, without throwing", async () => {
    render(
      <TipSurface opened content="Pointed at">
        <div data-testid="placed" />
      </TipSurface>,
    );
    expect(await screen.findByText("Pointed at")).toBeInTheDocument();
  });

  describe("nested Tips", () => {
    const nested = () =>
      render(
        <Tip text="Row hint">
          <div data-testid="row">
            row
            <Tip text="Icon hint">
              <ActionIcon aria-label="Info">i</ActionIcon>
            </Tip>
          </div>
        </Tip>,
      );

    it("entering a nested target shows only its hint, and closes the enclosing one", async () => {
      nested();
      const row = screen.getByTestId("row");
      const icon = screen.getByLabelText("Info");
      fireEvent.mouseEnter(row);
      expect(await screen.findByRole("tooltip")).toHaveTextContent("Row hint");
      // What a browser sends for a pointer moving from the row onto the icon.
      fireEvent.mouseOut(row, { relatedTarget: icon });
      fireEvent.mouseOver(icon, { relatedTarget: row });
      // The row's hint fades out while the icon's opens after its delay; only the icon's remains.
      await waitFor(() => {
        expect(screen.getAllByRole("tooltip")).toHaveLength(1);
        expect(screen.getByRole("tooltip")).toHaveTextContent("Icon hint");
      });
    });

    it("entering the nested target straight from outside opens only its hint", async () => {
      nested();
      fireEvent.mouseEnter(screen.getByLabelText("Info"));
      fireEvent.mouseOver(screen.getByLabelText("Info"));
      expect(await screen.findByRole("tooltip")).toHaveTextContent("Icon hint");
      await new Promise((r) => setTimeout(r, 300));
      expect(screen.getAllByRole("tooltip")).toHaveLength(1);
    });

    it("returning from the nested target to the enclosing one shows the enclosing hint", async () => {
      nested();
      const row = screen.getByTestId("row");
      const icon = screen.getByLabelText("Info");
      fireEvent.mouseEnter(icon);
      fireEvent.mouseOver(icon);
      await screen.findByText("Icon hint", { selector: "[role=tooltip] *" });
      fireEvent.mouseOut(icon, { relatedTarget: row });
      fireEvent.mouseOver(row, { relatedTarget: icon });
      await waitFor(() => expect(screen.getByRole("tooltip")).toHaveTextContent("Row hint"));
      expect(screen.getAllByRole("tooltip")).toHaveLength(1);
    });

    it("keyboard focus on the nested target does not open the enclosing hint", async () => {
      nested();
      const icon = screen.getByLabelText("Info");
      const matches = vi.spyOn(icon, "matches").mockImplementation((s) => s === ":focus-visible");
      fireEvent.focus(icon);
      expect(await screen.findByRole("tooltip")).toHaveTextContent("Icon hint");
      expect(screen.getAllByRole("tooltip")).toHaveLength(1);
      matches.mockRestore();
    });
  });
});
