// Copyright (c) 2026 Kenneth Stott
// Canary: 90c4e2a6-1b7f-43d8-a5e6-7c3d1f8b02e9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2155: what the role form offers a role with parents, and what each capability allows.

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../../test-utils/render";
import { FormLayout } from "../../../components/form/FormLayout";
import { MultiSelect } from "../../../components/MultiSelect";
import { ResidencyGrant } from "../../../components/admin/ResidencyGrant";
import { CapabilityNames } from "../CapabilityNames";
import { CapabilityGrid } from "../RoleFormParts";
import type { Capability } from "../../../types/auth";

// The options live in the dropdown: opened once, then each is read through its tick.
const tick = (capability: string) => screen.getByTestId(`capability-tick-${capability}`);
const box = (capability: string) => screen.getByTestId(`capability-box-${capability}`);
const ticked = (capability: string) => box(capability).hasAttribute("data-checked");
const disabled = (capability: string) => box(capability).hasAttribute("data-disabled");
const open = () => userEvent.click(screen.getByRole("textbox", { name: "Capabilities" }));

function grid(value: Capability[], held: ReadonlySet<string> | null, onToggle = vi.fn()) {
  render(
    <FormLayout>
      <CapabilityGrid value={value} held={held} onToggle={onToggle} label="Capabilities" />
    </FormLayout>,
  );
  return onToggle;
}

describe("the capability grid", () => {
  it("shows what a parent holds selected, to unselect", async () => {
    const onToggle = grid(["usage"], new Set(["usage"]));
    await open();
    expect(ticked("usage")).toBe(true);
    expect(ticked("write")).toBe(false);
    await userEvent.click(tick("usage"));
    expect(onToggle).toHaveBeenCalledWith("usage");
    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  it("selects a capability not yet held", async () => {
    const onToggle = grid(["usage"], null);
    await open();
    await userEvent.click(tick("write"));
    expect(onToggle).toHaveBeenCalledExactlyOnceWith("write");
  });

  it("disables a capability no parent holds", async () => {
    grid([], new Set(["usage"]));
    await open();
    expect(disabled("write")).toBe(true);
    expect(disabled("usage")).toBe(false);
  });

  it("disables nothing for a role with no parents", async () => {
    grid([], null);
    await open();
    expect(disabled("write")).toBe(false);
  });

  it("says why a capability is disabled", async () => {
    grid([], new Set(["usage"]));
    await open();
    await userEvent.hover(tick("write"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "No parent of this role holds this capability.",
    );
  });

  it("says what a capability allows", async () => {
    grid([], null);
    await open();
    await userEvent.hover(tick("write"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Write data, where the role has a write grant.",
    );
  });
});

describe("the capability catalog", () => {
  const locales = join(__dirname, "../../../i18n/locales");
  const catalog = (lng: string) =>
    JSON.parse(readFileSync(join(locales, lng, "capabilityHelp.json"), "utf8")).capabilityHelp;

  it("defines every capability the type names", () => {
    const declared = readFileSync(join(__dirname, "../../../types/auth.ts"), "utf8").split(
      "export type Capability =",
    )[1];
    const names = [...declared.matchAll(/^ {2}\| "([a-z_]+)"/gm)].map((m) => m[1]);
    expect(names.length).toBeGreaterThan(30);
    for (const name of names) expect(catalog("en")[name], name).toBeTruthy();
  });

  it("is in every locale with the same keys", () => {
    const keys = Object.keys(catalog("en")).sort();
    const lngs = readdirSync(locales, { withFileTypes: true })
      .filter((e) => e.isDirectory())
      .map((e) => e.name);
    expect(lngs.length).toBeGreaterThanOrEqual(13);
    for (const lng of lngs) expect(Object.keys(catalog(lng)).sort(), lng).toEqual(keys);
  });
});

describe("a list of capabilities", () => {
  it("shows data_residency's regions as its value", () => {
    render(<CapabilityNames capabilities={["data_residency", "usage"]} regions={["eu", "us"]} />);
    expect(screen.getByTestId("capability-name-data_residency")).toHaveTextContent(
      "data_residency: eu, us",
    );
    expect(screen.getByTestId("capability-name-usage")).toHaveTextContent(/^usage$/);
  });

  it("gives each capability its definition as a hint", async () => {
    render(<CapabilityNames capabilities={["catalog_read"]} />);
    await userEvent.hover(screen.getByTestId("capability-name-catalog_read"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Read the meta domain.");
  });
});

describe("a domain no parent reaches", () => {
  it("is shown, cannot be picked, and says why", async () => {
    const onChange = vi.fn();
    render(
      <MultiSelect
        label="Domain Access"
        options={[
          { id: "sales", label: "sales" },
          { id: "finance", label: "finance", disabled: true },
        ]}
        disabledHint="No parent of this role reaches this domain."
        value={[]}
        onChange={onChange}
      />,
    );
    await userEvent.click(screen.getByRole("textbox", { name: "Domain Access" }));
    const held = await screen.findByTestId("option-not-held-finance");
    await userEvent.hover(held);
    expect(await screen.findByRole("tooltip", { hidden: true })).toHaveTextContent(
      "No parent of this role reaches this domain.",
    );
    expect(onChange).not.toHaveBeenCalled();
  });
});

describe("data_residency's control", () => {
  function grant(held: boolean, parents: Parameters<typeof ResidencyGrant>[0]["parents"]) {
    render(
      <FormLayout>
        <ResidencyGrant
          held={held}
          values={[]}
          regions={["eu", "us"]}
          parents={parents}
          onHeld={vi.fn()}
          onValues={vi.fn()}
        />
      </FormLayout>,
    );
  }

  it("is disabled when no parent holds the capability", () => {
    grant(false, { capabilities: new Set(["usage"]), regions: new Set() });
    expect(screen.getByTestId("residency-grant-checkbox")).toBeDisabled();
  });

  it("opens the region control under the tick, offering what parents cover", async () => {
    grant(true, { capabilities: new Set(["data_residency"]), regions: new Set(["eu"]) });
    await userEvent.click(screen.getByRole("textbox", { name: "Region values covered" }));
    expect(await screen.findByRole("option", { name: "eu", hidden: true })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "us", hidden: true })).toBeNull();
  });
});
