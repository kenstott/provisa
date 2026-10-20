// Copyright (c) 2026 Kenneth Stott
// Canary: f389f07d-a185-4e08-86bf-ccea96359e1d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2115: Polly operates the screen as the user would -- fills a field, chooses an option,
// presses a button, shows a list -- naming each element by test id or by role and name, never
// guessing one, and pressing what changes something only on the user's yes.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { Select, TextInput } from "@mantine/core";
import { render, screen, waitFor } from "../test-utils/render";
import { FormFooter } from "../components/form/FormLayout";
import { parseUiActions, runUiActions, type UiAction } from "../mcp/uiActions";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";

function SourceForm({ onSave, onCancel }: { onSave: (v: string) => void; onCancel: () => void }) {
  const [id, setId] = useState("");
  const [type, setType] = useState<string | null>(null);
  return (
    <div>
      <TextInput
        label="Source ID"
        required
        value={id}
        onChange={(e) => setId(e.currentTarget.value)}
        data-testid="source-id"
      />
      <Select
        label="Type"
        data={["PostgreSQL", "MySQL"]}
        value={type}
        onChange={setType}
        data-testid="source-type"
      />
      <span data-testid="typed">{`${id}|${type ?? ""}`}</span>
      <button type="button" data-testid="opener">
        + Source
      </button>
      <FormFooter onSave={() => onSave(`${id}|${type}`)} onCancel={onCancel} />
    </div>
  );
}

const pace = (confirm: (q: string) => Promise<boolean> = vi.fn()) => ({
  confirm,
  paceMs: 0,
  waitMs: 300,
});

function form() {
  const onSave = vi.fn();
  const onCancel = vi.fn();
  render(<SourceForm onSave={onSave} onCancel={onCancel} />);
  return { onSave, onCancel };
}

describe("Polly's steps on the screen", () => {
  it("fills a field and chooses an option as the user's own input would", async () => {
    form();
    const out = await runUiActions(
      [
        { do: "fill", target: { testId: "source-id" }, value: "shop-db" },
        { do: "choose", target: { testId: "source-type" }, option: "PostgreSQL" },
      ],
      pace(),
    );
    expect(out.success).toBe(true);
    await waitFor(() =>
      expect(screen.getByTestId("typed")).toHaveTextContent("shop-db|PostgreSQL"),
    );
    expect(out.steps).toEqual([
      'Filled the element with test id "source-id" with "shop-db".',
      'Chose "PostgreSQL" in the element with test id "source-type".',
    ]);
  });

  it("names an element by its role and accessible name", async () => {
    form();
    const out = await runUiActions(
      [{ do: "fill", target: { role: "textbox", name: "Source ID" }, value: "pets" }],
      pace(),
    );
    expect(out.success).toBe(true);
    expect(screen.getByTestId("typed")).toHaveTextContent("pets|");
  });

  it("answers back an element the screen does not have, by name, and takes no later step", async () => {
    const { onCancel } = form();
    const out = await runUiActions(
      [
        { do: "press", target: { testId: "no-such-button" } },
        { do: "press", target: { role: "button", name: "Cancel" } },
      ],
      pace(),
    );
    expect(out).toEqual({
      success: false,
      steps: ['The screen has no element with test id "no-such-button".'],
    });
    expect(onCancel).not.toHaveBeenCalled();
  });

  it("answers back an option the list does not offer, naming the ones it does", async () => {
    form();
    const out = await runUiActions(
      [{ do: "choose", target: { testId: "source-type" }, option: "Oracle" }],
      pace(),
    );
    expect(out.success).toBe(false);
    expect(out.steps[0]).toContain('it offers no option "Oracle"; it offers "PostgreSQL", "MySQL"');
  });

  it("presses what changes nothing without asking", async () => {
    const { onCancel } = form();
    const confirm = vi.fn();
    const out = await runUiActions(
      [
        { do: "press", target: { testId: "opener" } },
        { do: "press", target: { role: "button", name: "Cancel" } },
      ],
      pace(confirm),
    );
    expect(out.success).toBe(true);
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(confirm).not.toHaveBeenCalled();
  });

  it("presses a form's Save only on the user's yes, outlined as she asks", async () => {
    const { onSave } = form();
    const save = screen.getByRole("button", { name: "Save" });
    let outlined = false;
    const declined = await runUiActions(
      [{ do: "press", target: { role: "button", name: "Save" } }],
      {
        ...pace(async () => {
          outlined = save.classList.contains("polly-acting");
          return false;
        }),
      },
    );
    expect(outlined).toBe(true);
    expect(declined).toEqual({
      success: false,
      steps: ['The user declined: the button named "Save" was not pressed.'],
    });
    expect(onSave).not.toHaveBeenCalled();
    expect(save.classList.contains("polly-acting")).toBe(false);

    const confirm = vi.fn(async () => true);
    const pressed = await runUiActions(
      [{ do: "press", target: { role: "button", name: "Save" } }],
      pace(confirm),
    );
    expect(pressed.success).toBe(true);
    expect(confirm).toHaveBeenCalledWith('Press "Save"? It changes something.');
    expect(onSave).toHaveBeenCalledTimes(1);
  });

  it("asks before a press she says changes something, though the screen does not mark it", async () => {
    form();
    const confirm = vi.fn(async () => false);
    await runUiActions(
      [{ do: "press", target: { testId: "opener" }, changes: true }],
      pace(confirm),
    );
    expect(confirm).toHaveBeenCalledTimes(1);
  });

  it("refuses a name more than one element answers to", async () => {
    render(
      <div>
        <button type="button">Add</button>
        <button type="button">Add</button>
      </div>,
    );
    const out = await runUiActions(
      [{ do: "press", target: { role: "button", name: "Add" } }],
      pace(),
    );
    expect(out.steps).toEqual(['The screen has 2 of the button named "Add"; name it by test id.']);
  });

  it("answers back a malformed step by its position", () => {
    expect(parseUiActions([{ do: "press", target: { testId: "x" } }])).toEqual([
      { do: "press", target: { testId: "x" }, changes: false },
    ] satisfies UiAction[]);
    expect(parseUiActions([{ do: "fill", target: { testId: "x" } }])).toBe(
      'actions[0]: "do" must be fill (with value), choose (with option), press or show',
    );
    expect(parseUiActions([{ do: "press", target: {} }])).toBe(
      "actions[0].target names neither a testId nor a role and name",
    );
    expect(parseUiActions("press")).toBe("actions must be a list");
  });
});

describe("navigate with steps", () => {
  it("navigates, then takes the steps, asking the user in the chat before a Save", async () => {
    const { onSave } = form();
    const ctx: ClientToolContext = {
      navigate: vi.fn(),
      confirm: vi.fn(),
      runMutation: vi.fn(),
      presentChoice: vi.fn(async () => true),
    };
    const out = await executeClientTool(
      "navigate",
      {
        route: "/sources",
        actions: [
          { do: "fill", target: { testId: "source-id" }, value: "shop-db" },
          { do: "press", target: { role: "button", name: "Save" } },
        ],
      },
      ctx,
    );
    expect(ctx.navigate).toHaveBeenCalledWith("/sources", undefined);
    expect(ctx.presentChoice).toHaveBeenCalledWith({
      question: 'Press "Save"? It changes something.',
      mode: "yes_no",
    });
    expect(onSave).toHaveBeenCalledWith("shop-db|null");
    expect(out.success).toBe(true);
    expect(out.message.split("\n")).toEqual([
      "Navigated to /sources.",
      'Filled the element with test id "source-id" with "shop-db".',
      'Pressed the button named "Save".',
    ]);
  }, 15000);

  it("answers a malformed step without navigating", async () => {
    const ctx: ClientToolContext = {
      navigate: vi.fn(),
      confirm: vi.fn(),
      runMutation: vi.fn(),
      presentChoice: vi.fn(),
    };
    const out = await executeClientTool(
      "navigate",
      { route: "/sources", actions: [{ do: "jump", target: { testId: "x" } }] },
      ctx,
    );
    expect(out.success).toBe(false);
    expect(ctx.navigate).not.toHaveBeenCalled();
  });
});
