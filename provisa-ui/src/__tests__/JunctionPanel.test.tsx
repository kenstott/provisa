// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3f5d61-27ce-4a90-9c14-1f6ad8e73c02
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1586: declaring a junction is rare next to ordinary relationship registration, so its six
// fields must not sit inline on the surface — one checkbox carries the declaration and the mapping
// appears only behind it, and the label nomination is asked for explicitly rather than inferred.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { JunctionPanel } from "../components/relationships/JunctionPanel";
import { EMPTY_FORM, type RelForm } from "../components/relationships/relationship-types";
import type { RegisteredTable } from "../types/admin";

const TABLES = [
  {
    tableName: "glossary_term_edges",
    columns: [
      { columnName: "from_domain" },
      { columnName: "from_term_id" },
      { columnName: "to_term_id" },
      { columnName: "rel_type" },
    ],
  },
] as unknown as RegisteredTable[];

function mount(form: RelForm = EMPTY_FORM, setForm = vi.fn()) {
  render(
    <MantineProvider theme={{ components: {} }}>
      <JunctionPanel form={form} setForm={setForm} tables={TABLES} testIdPrefix="rel-form" />
    </MantineProvider>,
  );
  return setForm;
}

describe("REQ-1586 junction declaration panel", () => {
  it("hides the mapping until the junction box is checked", () => {
    mount();
    const toggle = screen.getByTestId("rel-form-junction-toggle") as HTMLInputElement;
    expect(toggle.checked).toBe(false);
    expect(screen.queryByTestId("rel-form-via-table")).toBeNull();
  });

  it("shows the six fields once a steward checks the box", () => {
    const setForm = vi.fn();
    const { rerender } = render(
      <MantineProvider theme={{ components: {} }}>
        <JunctionPanel form={EMPTY_FORM} setForm={setForm} tables={TABLES} testIdPrefix="rel-form" />
      </MantineProvider>,
    );
    fireEvent.click(screen.getByTestId("rel-form-junction-toggle"));
    expect(setForm).toHaveBeenCalledWith(expect.objectContaining({ junctionDeclared: true }));
    rerender(
      <MantineProvider theme={{ components: {} }}>
        <JunctionPanel
          form={{ ...EMPTY_FORM, junctionDeclared: true }}
          setForm={setForm}
          tables={TABLES}
          testIdPrefix="rel-form"
        />
      </MantineProvider>,
    );
    for (const id of [
      "rel-form-via-table",
      "rel-form-via-source-column",
      "rel-form-via-target-column",
      "rel-form-via-type-column",
      "rel-form-via-type-value",
      "rel-form-via-label-source",
    ]) {
      expect(screen.getByTestId(id)).toBeTruthy();
    }
  });

  it("reads a stored junction back as a checked box with its mapping open", () => {
    mount({ ...EMPTY_FORM, viaTable: "glossary_term_edges" });
    expect((screen.getByTestId("rel-form-junction-toggle") as HTMLInputElement).checked).toBe(true);
    expect(screen.getByTestId("rel-form-via-table")).toBeTruthy();
  });

  it("reads a composite end back as its ordered list of picked columns", () => {
    // REQ-1586: a composite foreign key is declared by listing its columns in order, stored as one
    // comma-separated declaration — the panel must show every column, not just the first.
    mount({
      ...EMPTY_FORM,
      viaTable: "glossary_term_edges",
      viaSourceColumn: "from_domain,from_term_id",
    });
    const pills = screen
      .getByTestId("rel-form-via-source-column")
      .closest(".mantine-InputWrapper-root")!
      .querySelectorAll(".mantine-Pill-root");
    expect([...pills].map((p) => p.textContent)).toEqual(["from_domain", "from_term_id"]);
  });

  it("discards the whole mapping when the junction box is cleared", () => {
    const setForm = mount({
      ...EMPTY_FORM,
      junctionDeclared: true,
      viaTable: "glossary_term_edges",
      viaSourceColumn: "from_term_id",
      viaTargetColumn: "to_term_id",
      viaTypeColumn: "rel_type",
      viaTypeValue: "KIND_OF",
      viaLabelSource: "column",
    });
    fireEvent.click(screen.getByTestId("rel-form-junction-toggle"));
    expect(setForm).toHaveBeenCalledWith(
      expect.objectContaining({
        junctionDeclared: false,
        viaTable: "",
        viaSourceColumn: "",
        viaTargetColumn: "",
        viaTypeColumn: "",
        viaTypeValue: "",
        viaLabelSource: "",
      }),
    );
  });
});
