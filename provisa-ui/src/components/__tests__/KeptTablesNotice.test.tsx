// Copyright (c) 2026 Kenneth Stott
// Canary: d146fb7c-3a5a-483e-ab99-ef168f3fed2a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Registering a remote source again keeps the tables something still refers to (REQ-1918).
// The registration went through, so nothing else on the page says a table was left in place:
// the notice names each kept table and what refers to it.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../test-utils/render";
import { KeptTablesNotice } from "../KeptTablesNotice";
import { keptTablesOf, referrersOf } from "../../lib/keptTables";

const GONE = {
  id: 7,
  name: "legacy_orders",
  dependents: [
    { kind: "relationship", id: "orders-to-customers", name: "orders-to-customers", via: [] },
    { kind: "table", id: 12, name: "big_orders", via: ["registered_tables.view_sql"] },
  ],
};
const LOST_COLUMNS = {
  id: 9,
  name: "customers",
  columns: {
    region: [{ kind: "row_filter", id: 3, name: "analyst on customers", via: [] }],
  },
};

describe("keptTablesOf", () => {
  it("reads the kept tables a registration answer reports", () => {
    expect(keptTablesOf({ source_id: "crm", kept_tables: [GONE, LOST_COLUMNS] })).toEqual([
      GONE,
      LOST_COLUMNS,
    ]);
  });

  it("is empty for an answer that reports none, or is not an answer at all", () => {
    expect(keptTablesOf({ source_id: "crm", kept_tables: [] })).toEqual([]);
    expect(keptTablesOf({ source_id: "crm" })).toEqual([]);
    expect(keptTablesOf(null)).toEqual([]);
  });
});

describe("referrersOf", () => {
  it("names what refers to a table the remote no longer has", () => {
    expect(referrersOf(GONE)).toEqual(["orders-to-customers", "big_orders"]);
  });

  it("names what refers to each column a table would have lost", () => {
    expect(referrersOf(LOST_COLUMNS)).toEqual(["analyst on customers (region)"]);
  });
});

describe("KeptTablesNotice", () => {
  it("renders nothing when no table was kept", () => {
    render(<KeptTablesNotice kept={[]} onClose={vi.fn()} />);
    expect(screen.queryByTestId("kept-tables")).toBeNull();
  });

  it("names each kept table with what refers to it, and can be dismissed", () => {
    const onClose = vi.fn();
    render(<KeptTablesNotice kept={[GONE, LOST_COLUMNS]} onClose={onClose} />);

    const notice = screen.getByTestId("kept-tables");
    expect(notice).toHaveTextContent("Tables kept: 2");
    expect(notice).toHaveTextContent(
      "legacy_orders: referred to by orders-to-customers, big_orders",
    );
    expect(notice).toHaveTextContent("customers: referred to by analyst on customers (region)");

    fireEvent.click(notice.querySelector("button")!);
    expect(onClose).toHaveBeenCalled();
  });
});
