// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968: a form read flows and sizes as its edit form does. Each field is a cell of its size in
// the form's flow, its label above its value and its hint below; no grid of its own.

import { describe, expect, it } from "vitest";
import { Button, TextInput } from "@mantine/core";
import { render, screen, within } from "../../../test-utils/render";
import { FormField, FormLayout, FormRead, ReadRow } from "../FormLayout";

describe("a form read", () => {
  it("draws its fields in the form's flow, each at its size", () => {
    render(
      <FormRead testId="read">
        <FormLayout>
          <FormField size="full">
            <TextInput label="Name" value="Orders" readOnly />
          </FormField>
          <FormField size="short">
            <TextInput label="Port" value="5432" readOnly />
          </FormField>
        </FormLayout>
      </FormRead>,
    );
    const read = screen.getByTestId("read");
    expect(read).toHaveClass("form-layout", "form-read");
    const cells = [...read.querySelectorAll(":scope > .form-field")];
    expect(cells.map((c) => c.className)).toEqual([
      expect.stringContaining("form-field--full"),
      expect.stringContaining("form-field--short"),
    ]);
    // Label, then value, in each cell.
    expect(cells.map((c) => [...c.children].map((e) => e.textContent))).toEqual([
      ["Name", "Orders"],
      ["Port", "5432"],
    ]);
    expect(within(read).queryAllByRole("textbox")).toEqual([]);
  });

  it("shows a field's hint below its value, dimmed", () => {
    render(
      <FormRead testId="read">
        <FormField size="medium" hint="The port it listens on.">
          <TextInput label="Port" value="5432" readOnly />
        </FormField>
      </FormRead>,
    );
    const cell = screen.getByTestId("read").querySelector(":scope > .form-field") as HTMLElement;
    const [label, value, hint] = [...cell.children];
    expect(label).toHaveTextContent("Port");
    expect(value).toHaveTextContent("5432");
    expect(hint).toHaveTextContent("The port it listens on.");
    expect(hint).toHaveClass("form-read__hint");
  });

  it("draws a row a control draws for itself at its field's size, with its hint", () => {
    render(
      <FormRead testId="read">
        <FormField size="long" hint="Where it runs.">
          <ReadRow label="Region" value="eu-west-1" />
        </FormField>
      </FormRead>,
    );
    const cell = screen.getByTestId("read").querySelector(":scope > .form-field") as HTMLElement;
    expect(cell).toHaveClass("form-field--long");
    expect(cell).toHaveTextContent("Where it runs.");
  });

  it("reads a derived value with its own size and hint after the fields", () => {
    render(
      <FormRead
        testId="read"
        derived={[
          { label: "Status", value: "Draft", size: "medium", hint: "Approve it.", hintTestId: "h" },
        ]}
      >
        <FormField size="short">
          <TextInput label="Port" value="5432" readOnly />
        </FormField>
      </FormRead>,
    );
    const cells = [...screen.getByTestId("read").querySelectorAll(":scope > .form-field")];
    expect(cells.map((c) => c.querySelector("dt")?.textContent)).toEqual(["Port", "Status"]);
    expect(cells[1]).toHaveClass("form-field--medium");
    expect(screen.getByTestId("h")).toHaveTextContent("Approve it.");
  });

  it("draws none of the form's furniture", () => {
    render(
      <FormRead testId="read">
        <FormLayout>
          <FormField size="short">
            <TextInput label="Port" value="5432" readOnly />
          </FormField>
          <Button>Test connection</Button>
        </FormLayout>
      </FormRead>,
    );
    const read = screen.getByTestId("read");
    const drawn = [...read.children].filter((c) => !c.matches(".form-field, .form-read__heading"));
    // Anything else is in the DOM but not drawn (App.css hides it).
    expect(drawn.map((c) => c.textContent)).toEqual(["Test connection"]);
  });
});
