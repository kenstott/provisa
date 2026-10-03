// Copyright (c) 2026 Kenneth Stott
// Canary: 99e50618-de26-4a4d-81a5-86e08dd441f6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-318: the Paging section shows what the table's reader takes — a REST endpoint's type and
// the parameters that type reads, or a connection table's max rows — and nothing else.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
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
