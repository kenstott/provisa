// Copyright (c) 2026 Kenneth Stott
// Canary: 2b7d0e94-6c15-4a83-8f2e-c4d19a6e3b70
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: a result cell shows an IRI compacted, with the whole IRI on hover and on copy; a
// literal is shown compactly, its datatype whole on hover.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { TableView } from "../TableView";

const IRI = "https://provisa.example/id/sales/orders/1";
const rows = [
  {
    s: { rdfTerm: "iri", value: IRI, shown: "id:sales/orders/1" },
    amount: {
      rdfTerm: "literal",
      value: "12.50",
      shown: '"12.50"^^xsd:decimal',
      datatype: "http://www.w3.org/2001/XMLSchema#decimal",
    },
  },
];

function table() {
  render(
    <TableView columns={["s", "amount"]} rows={rows} colWidths={[200, 200]} setColWidths={vi.fn()} />,
  );
}

describe("a result cell", () => {
  it("shows an IRI compacted, whole on hover", () => {
    table();
    const cell = screen.getByText("id:sales/orders/1").closest("td");
    expect(cell).toHaveAttribute("title", IRI);
  });

  it("copies the whole IRI", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    table();
    fireEvent.click(screen.getByRole("button", { name: "Copy the full IRI" }));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(IRI));
  });

  it("shows a literal's datatype compactly, whole on hover, with no IRI to copy", () => {
    table();
    const cell = screen.getByText('"12.50"^^xsd:decimal').closest("td");
    expect(cell).toHaveAttribute("title", "http://www.w3.org/2001/XMLSchema#decimal");
    expect(screen.getAllByRole("button", { name: "Copy the full IRI" })).toHaveLength(1);
  });
});
