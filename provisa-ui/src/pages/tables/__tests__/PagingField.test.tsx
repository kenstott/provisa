// Copyright (c) 2026 Kenneth Stott
// Canary: 99e50618-de26-4a4d-81a5-86e08dd441f6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-318: the Paging section shows what the table's reader takes — a REST endpoint's type and
// the parameters that type reads, or a connection table's max rows — and nothing else.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { PagingField } from "../PagingField";
import { NO_PAGING } from "../paging";

describe("PagingField (REQ-318)", () => {
  it("shows an offset endpoint's parameters with the names sent when left empty", () => {
    render(
      <PagingField
        kind="endpoint"
        paging={{ ...NO_PAGING, type: "offset", maxPages: 3 }}
        onChange={vi.fn()}
        ceilingRows={1000}
      />,
    );
    expect(screen.getByRole("textbox", { name: "Page parameter" })).toHaveAttribute(
      "placeholder",
      "offset",
    );
    expect(screen.getByRole("textbox", { name: "Page size parameter" })).toHaveAttribute(
      "placeholder",
      "limit",
    );
    expect(screen.getByRole("textbox", { name: "Max pages" })).toHaveValue("3");
    expect(screen.queryByRole("textbox", { name: "Max rows per read" })).not.toBeInTheDocument();
  });

  it("names what paging after the last row reads, with the names sent when left empty", () => {
    render(
      <PagingField
        kind="endpoint"
        paging={{ ...NO_PAGING, type: "last_row", cursorParam: "starting_after" }}
        onChange={vi.fn()}
        ceilingRows={1000}
      />,
    );
    expect(screen.getByRole("textbox", { name: "Starts-after parameter" })).toHaveValue(
      "starting_after",
    );
    expect(screen.getByRole("textbox", { name: "Last row's field" })).toHaveAttribute(
      "placeholder",
      "id",
    );
    expect(screen.getByRole("textbox", { name: "Page size parameter" })).toHaveAttribute(
      "placeholder",
      "limit",
    );
    expect(screen.queryByRole("textbox", { name: "Next-cursor field" })).not.toBeInTheDocument();
  });

  it("lets a cursor-paged table name its page-size parameter, sending none when left empty", () => {
    render(
      <PagingField
        kind="endpoint"
        paging={{ ...NO_PAGING, type: "cursor", cursorParam: "page", pageSizeParam: "limit" }}
        onChange={vi.fn()}
        ceilingRows={1000}
      />,
    );
    expect(screen.getByRole("textbox", { name: "Page size parameter" })).toHaveValue("limit");
    expect(screen.getByRole("textbox", { name: "Page size parameter" })).not.toHaveAttribute(
      "placeholder",
    );
    expect(screen.getByRole("textbox", { name: "Page size" })).toBeInTheDocument();
  });

  it("lets the steward say where the rows are when registering (REQ-316)", () => {
    const onChange = vi.fn();
    render(
      <PagingField
        kind="endpoint"
        paging={{ ...NO_PAGING, rowsField: "values" }}
        onChange={onChange}
        ceilingRows={1000}
        rowsFieldEditable
      />,
    );
    const field = screen.getByRole("textbox", { name: "Rows are under" });
    expect(field).toHaveValue("values");
    expect(field).not.toHaveAttribute("readonly");
    // Where the rows are needs no paging type beside it.
    expect(screen.queryByText("Choose a paging type, or clear the paging fields.")).toBeNull();
    fireEvent.change(field, { target: { value: "data" } });
    expect(onChange).toHaveBeenCalledWith({ ...NO_PAGING, rowsField: "data" });
  });

  it("shows where a registered table's rows are without letting it be edited", () => {
    const { rerender } = render(
      <PagingField
        kind="endpoint"
        paging={{ ...NO_PAGING, type: "offset", rowsField: "values" }}
        onChange={vi.fn()}
        ceilingRows={1000}
      />,
    );
    expect(screen.getByRole("textbox", { name: "Rows are under" })).toHaveAttribute("readonly");
    rerender(
      <PagingField
        kind="endpoint"
        paging={{ ...NO_PAGING, type: "offset" }}
        onChange={vi.fn()}
        ceilingRows={1000}
      />,
    );
    expect(screen.queryByRole("textbox", { name: "Rows are under" })).toBeNull();
  });

  it("shows a connection table its row bound only", () => {
    render(
      <PagingField kind="connection" paging={null} onChange={vi.fn()} ceilingRows={1000} />,
    );
    expect(screen.getByRole("textbox", { name: "Max rows per read" })).toHaveAttribute(
      "placeholder",
      "Default: 1000",
    );
    expect(screen.queryByLabelText("Paging type")).not.toBeInTheDocument();
  });
});
