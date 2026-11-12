// Copyright (c) 2026 Kenneth Stott
// Canary: 5b330d17-eae2-4cb3-bdc3-8eb4aee64c7a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi } from "vitest";
import { Checkbox, TextInput } from "@mantine/core";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import i18n from "../../../i18n";
import {
  FieldGroup,
  FormChecks,
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
} from "../FormLayout";
import { SplitPane } from "../../list/SplitPane";

const t = i18n.getFixedT("en");

// REQ-1968: one shared form layout; identifiers in a section of their own above the flow.
describe("FormLayout", () => {
  // First in the file: React warns of an unknown prop once per run, and a later tick would find
  // it already said. The UI e2e core lane failed on every page with such a tick (alpha.477 to .481): React's
  // "does not recognize the withAsterisk prop on a DOM element", an uncaught browser error.
  it("gives a tick with a (?) no prop its DOM input does not take", () => {
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      render(
        <FormLayout>
          <FormChecks>
            <FormField size="short" help="Precompute this view.">
              <Checkbox label="Materialized View" />
            </FormField>
          </FormChecks>
        </FormLayout>,
      );
      const box = screen.getByRole("checkbox", { name: "Materialized View" });
      expect(box).not.toHaveAttribute("withasterisk");
      expect(errors.mock.calls.flat().join(" ")).not.toMatch(/withAsterisk/);
    } finally {
      errors.mockRestore();
    }
  });

  it("renders a field in its size class", () => {
    render(
      <FormLayout>
        <FormField size="short" data-testid="port-field">
          <TextInput label="Port" />
        </FormField>
      </FormLayout>,
    );
    expect(screen.getByTestId("port-field")).toHaveClass("form-field", "form-field--short");
    expect(screen.getByLabelText("Port")).toBeInTheDocument();
  });

  it("renders as a form and submits", () => {
    const onSubmit = vi.fn((e: React.FormEvent) => e.preventDefault());
    render(
      <FormLayout component="form" onSubmit={onSubmit} data-testid="the-form">
        <button type="submit">go</button>
      </FormLayout>,
    );
    const form = screen.getByTestId("the-form");
    expect(form.tagName).toBe("FORM");
    expect(form).toHaveClass("form-layout");
    screen.getByRole("button", { name: "go" }).click();
    expect(onSubmit).toHaveBeenCalledTimes(1);
  });

  it("puts identifiers in a headed section before the flow", () => {
    render(
      <FormLayout data-testid="layout">
        <FormIdentifiers>
          <FormField size="medium">
            <TextInput label="Source ID" />
          </FormField>
        </FormIdentifiers>
        <FormField size="long">
          <TextInput label="Description" />
        </FormField>
      </FormLayout>,
    );
    const section = screen.getByRole("region", { name: t("common.identity") });
    expect(section).toContainElement(screen.getByLabelText("Source ID"));
    expect(section).not.toContainElement(screen.getByLabelText("Description"));
    expect(screen.getByTestId("layout").firstElementChild).toBe(section);
  });

  it("keeps grouped fields in one wrapping unit, in reading order", () => {
    render(
      <FormLayout>
        <FieldGroup data-testid="host-port">
          <FormField size="medium">
            <TextInput label="Host" />
          </FormField>
          <FormField size="short">
            <TextInput label="Port" />
          </FormField>
        </FieldGroup>
      </FormLayout>,
    );
    const group = screen.getByTestId("host-port");
    expect(group).toHaveClass("form-field-group");
    const inputs = Array.from(group.querySelectorAll("input"));
    expect(inputs).toEqual([screen.getByLabelText("Host"), screen.getByLabelText("Port")]);
  });

  it("gives a list entry a line to itself", () => {
    render(
      <FormLayout>
        <FieldGroup row data-testid="entry">
          <FormField size="medium">
            <TextInput label="Role" />
          </FormField>
        </FieldGroup>
      </FormLayout>,
    );
    expect(screen.getByTestId("entry")).toHaveClass("form-field-group", "form-field-group--row");
  });

  it("puts a brief explanation below the control, and nothing between label and control", () => {
    render(
      <FormLayout>
        <FormField size="medium" hint="The mailbox this source reads.">
          <TextInput label="Mailbox address" />
        </FormField>
      </FormLayout>,
    );
    const input = screen.getByLabelText("Mailbox address");
    const hint = screen.getByText("The mailbox this source reads.");
    expect(input.compareDocumentPosition(hint) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(input).toHaveAccessibleDescription("The mailbox this source reads.");
  });

  it("keeps an explanation a form passes straight to a control below it too", () => {
    render(<TextInput label="Host" description="Where it is." />);
    const input = screen.getByLabelText("Host");
    const text = screen.getByText("Where it is.");
    expect(input.compareDocumentPosition(text) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("puts a longer explanation behind a (?) right after the label", () => {
    render(
      <FormLayout>
        <FormField size="medium" help="One mailbox is connected by its owner.">
          <TextInput label="Whose mail" />
        </FormField>
      </FormLayout>,
    );
    const input = screen.getByLabelText("Whose mail");
    const icon = screen.getByRole("img", { name: "One mailbox is connected by its owner." });
    expect(icon.closest("label")).toBe(
      input.closest(".mantine-InputWrapper-root")?.querySelector("label"),
    );
    expect(icon).toHaveAttribute("tabindex", "0");
    expect(screen.queryByText("One mailbox is connected by its owner.")).not.toBeInTheDocument();
    // Named by the label's text alone; the help is the description, not part of the name.
    expect(screen.getByRole("textbox", { name: "Whose mail" })).toBe(input);
    expect(input).toHaveAttribute("aria-description", "One mailbox is connected by its owner.");
  });

  it("draws the required mark right after the label text, before the (?)", () => {
    render(
      <FormLayout>
        <FormField size="medium" help="Which mailbox.">
          <TextInput label="Mailbox address" required />
        </FormField>
      </FormLayout>,
    );
    const input = screen.getByRole("textbox");
    expect(input).toBeRequired();
    const label = input.closest(".mantine-InputWrapper-root")?.querySelector("label");
    expect(label?.textContent).toBe("Mailbox address *");
    const mark = label?.querySelector(".form-required");
    const icon = screen.getByRole("img", { name: "Which mailbox." });
    expect(mark!.compareDocumentPosition(icon) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("keeps the ticks of a form together, each with its optional (?)", () => {
    render(
      <FormLayout>
        <FormChecks data-testid="ticks">
          <Checkbox label="Cache enabled" />
          <FormField size="short" help="Serve only what is fresh.">
            <Checkbox label="Freshness gate" />
          </FormField>
        </FormChecks>
      </FormLayout>,
    );
    const ticks = screen.getByTestId("ticks");
    expect(ticks).toHaveClass("form-checks");
    expect(ticks).toContainElement(screen.getByLabelText("Cache enabled"));
    expect(ticks).toContainElement(screen.getByRole("img", { name: "Serve only what is fresh." }));
    expect(screen.getByRole("checkbox", { name: "Freshness gate" })).toHaveAttribute(
      "aria-description",
      "Serve only what is fresh.",
    );
  });

  it("refuses hint or help without the control as its only child", () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() =>
      render(
        <FormLayout>
          <FormField size="medium" hint="x">
            text
          </FormField>
        </FormLayout>,
      ),
    ).toThrow("needs the field's control as its only child");
    spy.mockRestore();
  });

  it.each([
    ["FormField", <FormField size="short">x</FormField>],
    ["FieldGroup", <FieldGroup>x</FieldGroup>],
    ["FormIdentifiers", <FormIdentifiers>x</FormIdentifiers>],
    ["FormChecks", <FormChecks>x</FormChecks>],
  ])("%s outside a FormLayout throws", (name, element) => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(() => render(element)).toThrow(`<${name}> must be rendered inside a <FormLayout>`);
    spy.mockRestore();
  });

  it("ends in one Save and one Cancel, icon and word, Save first", () => {
    render(
      <FormLayout data-testid="layout">
        <FormField size="medium">
          <TextInput label="Name" />
        </FormField>
        <FormFooter data-testid="footer" onSave={() => {}} onCancel={() => {}} />
      </FormLayout>,
    );
    const footer = screen.getByTestId("footer");
    expect(footer).toHaveClass("form-footer");
    expect(screen.getByTestId("layout").lastElementChild).toBe(footer);
    const buttons = Array.from(footer.querySelectorAll("button"));
    expect(buttons.map((b) => b.textContent)).toEqual([t("common.save"), t("common.cancel")]);
    for (const b of buttons) expect(b.querySelector("svg")).not.toBeNull();
    // The one Cancel: bordered and full size, the same as in a panel's head.
    expect(buttons[1]).toHaveAttribute("data-variant", "default");
    expect(buttons[1]).toHaveAttribute("data-size", "sm");
  });

  describe("as a detail panel's actions", () => {
    const LABELS = {
      panel: "Detail",
      resize: "Resize",
      expand: "Expand",
      restore: "Restore",
      close: "Close",
      backToList: "Back",
    };
    const inPanel = (form: React.ReactNode) =>
      render(
        <SplitPane
          list={<div>list</div>}
          title="orders"
          onClose={() => {}}
          storageKey="test-panel"
          labels={LABELS}
          testId="pane"
        >
          {form}
        </SplitPane>,
      );
    const head = () => screen.getByTestId("pane-title").parentElement as HTMLElement;

    it("sits in the panel's head beside the name, Save borderless and compact, Cancel the one Cancel, with the same ids", async () => {
      inPanel(
        <FormFooter
          panel
          onSave={() => {}}
          onCancel={() => {}}
          saveProps={{ "data-testid": "the-save" }}
          cancelProps={{ "data-testid": "the-cancel" }}
        />,
      );
      const save = await screen.findByTestId("the-save");
      const cancel = screen.getByTestId("the-cancel");
      expect(head()).toContainElement(save);
      expect(head()).toContainElement(cancel);
      // Icon and word, as at a form's foot.
      expect([save.textContent, cancel.textContent]).toEqual([
        t("common.save"),
        t("common.cancel"),
      ]);
      for (const b of [save, cancel]) expect(b.querySelector("svg")).not.toBeNull();
      // Save takes the head's look; Cancel is bordered and full size, as everywhere.
      expect(save).toHaveAttribute("data-variant", "subtle");
      expect(save).toHaveAttribute("data-size", "compact-sm");
      expect(cancel).toHaveAttribute("data-variant", "default");
      expect(cancel).toHaveAttribute("data-size", "sm");
      // Save is marked as the primary (and tinted as one), Cancel is not.
      expect(save).toHaveAttribute("data-primary", "true");
      expect(cancel).not.toHaveAttribute("data-primary");
      expect(save.closest(".form-footer")).toHaveClass("form-footer--head");
      // Nothing is left at the foot of the panel's content.
      expect(
        screen.getByTestId("pane-panel").querySelector(".split-pane__body .form-footer"),
      ).toBeNull();
    });

    it("keeps Save's spinner and both buttons' rest while a save runs", async () => {
      let settle: () => void = () => {};
      inPanel(
        <FormFooter
          panel
          onSave={() => new Promise<void>((resolve) => (settle = resolve))}
          onCancel={() => {}}
        />,
      );
      const save = await screen.findByRole("button", { name: t("common.save") });
      fireEvent.click(save);
      expect(save).toBeDisabled();
      expect(screen.getByRole("button", { name: t("common.cancel") })).toBeDisabled();
      expect(save.querySelector(".mantine-Loader-root")).not.toBeNull();
      settle();
      await waitFor(() => expect(save).toBeEnabled());
    });

    it("says what is wrong in one line under the panel's head", async () => {
      inPanel(<FormFooter panel onSave={() => {}} onCancel={() => {}} problem="Name is taken" />);
      const said = await screen.findByTestId("form-problem");
      expect(said).toHaveAttribute("role", "alert");
      expect(said).toHaveTextContent("Name is taken");
      expect(said.parentElement).toHaveClass("split-pane__notice");
      expect(
        screen.getByTestId("pane-panel").querySelector(".split-pane__body"),
      ).not.toContainElement(said);
    });

    it("keeps another action of the form with them, in Save's place", async () => {
      inPanel(
        <FormFooter panel onCancel={() => {}} action={<button data-testid="run">Create</button>} />,
      );
      const run = await screen.findByTestId("run");
      expect(head()).toContainElement(run);
      expect(
        run.compareDocumentPosition(screen.getByRole("button", { name: t("common.cancel") })),
      ).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    });

    it("submits the form it names from the panel's head, where it sits outside that form", async () => {
      const onSubmit = vi.fn((e: React.FormEvent) => e.preventDefault());
      inPanel(
        <FormLayout component="form" id="the-form" onSubmit={onSubmit}>
          <FormField size="medium">
            <TextInput label="Name" required defaultValue="orders" />
          </FormField>
          <FormFooter panel submit form="the-form" onCancel={() => {}} />
        </FormLayout>,
      );
      const save = await screen.findByRole("button", { name: t("common.save") });
      expect(head()).toContainElement(save);
      // Outside the form's element, and still that form's submit.
      expect(save.closest("form")).toBeNull();
      expect(save).toHaveAttribute("type", "submit");
      expect(save).toHaveAttribute("form", "the-form");
      expect((save as HTMLButtonElement).form).toBe(document.getElementById("the-form"));
      fireEvent.click(save);
      expect(onSubmit).toHaveBeenCalledTimes(1);
    });

    it("names no form unless it is given one", async () => {
      inPanel(<FormFooter panel onSave={() => {}} onCancel={() => {}} />);
      expect(await screen.findByRole("button", { name: t("common.save") })).not.toHaveAttribute(
        "form",
      );
    });

    it("stays the row at the form's foot outside a panel, and for a form that does not ask", () => {
      render(
        <FormFooter
          panel
          data-testid="footer"
          onSave={() => {}}
          onCancel={() => {}}
          problem="No"
        />,
      );
      const footer = screen.getByTestId("footer");
      expect(footer).not.toHaveClass("form-footer--head");
      expect(footer).toContainElement(screen.getByTestId("form-problem"));
      expect(screen.getByRole("button", { name: t("common.save") })).not.toHaveAttribute(
        "data-variant",
        "subtle",
      );
    });

    it("leaves a form in a panel that does not ask where it was", async () => {
      inPanel(<FormFooter data-testid="footer" onSave={() => {}} onCancel={() => {}} />);
      const footer = await screen.findByTestId("footer");
      expect(head()).not.toContainElement(footer);
      expect(screen.getByTestId("pane-panel").querySelector(".split-pane__body")).toContainElement(
        footer,
      );
    });
  });

  it("shows a spinner on Save and disables both buttons until the save settles", async () => {
    let settle: () => void = () => {};
    const onSave = vi.fn(() => new Promise<void>((resolve) => (settle = resolve)));
    render(<FormFooter onSave={onSave} onCancel={() => {}} />);
    const save = screen.getByRole("button", { name: t("common.save") });
    const cancel = screen.getByRole("button", { name: t("common.cancel") });
    fireEvent.click(save);
    expect(onSave).toHaveBeenCalledTimes(1);
    expect(save).toBeDisabled();
    expect(cancel).toBeDisabled();
    expect(save).toHaveAttribute("data-saving", "true");
    expect(save.querySelector(".mantine-Loader-root")).not.toBeNull();
    expect(save).toHaveTextContent(t("common.save"));
    settle();
    await waitFor(() => expect(save).toBeEnabled());
    expect(cancel).toBeEnabled();
    expect(save.querySelector(".mantine-Loader-root")).toBeNull();
  });

  it("returns Save to rest when the save is refused", async () => {
    let refuse: (e: Error) => void = () => {};
    render(<FormFooter onSave={() => new Promise<void>((_, reject) => (refuse = reject))} />);
    const save = screen.getByRole("button", { name: t("common.save") });
    fireEvent.click(save);
    expect(save).toBeDisabled();
    refuse(new Error("refused"));
    await waitFor(() => expect(save).toBeEnabled());
  });

  it("takes the saving state from a form that submits itself", () => {
    const onSubmit = vi.fn((e: React.FormEvent) => e.preventDefault());
    const { rerender } = render(
      <form onSubmit={onSubmit}>
        <FormFooter submit onCancel={() => {}} />
      </form>,
    );
    const save = screen.getByRole("button", { name: t("common.save") });
    expect(save).toHaveAttribute("type", "submit");
    fireEvent.click(save);
    expect(onSubmit).toHaveBeenCalledTimes(1);
    rerender(
      <form onSubmit={onSubmit}>
        <FormFooter submit saving onCancel={() => {}} />
      </form>,
    );
    expect(save).toBeDisabled();
    expect(screen.getByRole("button", { name: t("common.cancel") })).toBeDisabled();
  });

  it("puts a first action that is not a save in Save's place, before Cancel", () => {
    render(
      <FormFooter onCancel={() => {}} action={<button type="button">Generate invite</button>}>
        <span>after</span>
      </FormFooter>,
    );
    const names = screen.getAllByRole("button").map((b) => b.textContent);
    expect(names).toEqual(["Generate invite", "Cancel"]);
  });
});
