// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e1f72-3b8a-4d06-a5e2-7f1d0b6c8e39
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2171: a column's subtype is shown beside its base wherever its type is shown -- the type a
// steward assigns is the base, and the subtype the source's type registers as stays beside it
// until the steward gives the column another type.

import { useState } from "react";
import { describe, it, expect } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import "../../../i18n";
import { shownType } from "../../../irTypes";
import { ColumnsTable } from "../ColumnsTable";
import type { ColumnForm } from "../types";

const column = (name: string, over: Partial<ColumnForm> = {}): ColumnForm => ({
  name,
  selected: true,
  dataType: "text",
  isPrimaryKey: false,
  visibleTo: ["analyst"],
  masks: [],
  alias: "",
  description: "",
  scope: "domain",
  nativeFilterType: null,
  ...over,
});

function Editor({ initial }: { initial: ColumnForm[] }) {
  const [columns, setColumns] = useState(initial);
  return (
    <ColumnsTable
      columns={columns}
      updateCol={(i, key, value) =>
        setColumns((cols) =>
          cols.map((c, n) =>
            n === i ? { ...c, [key]: value, ...(key === "dataType" ? { subtype: null } : {}) } : c,
          ),
        )
      }
      roles={[{ id: "analyst" }] as never}
      irTypes={["integer", "text"]}
      sourceType="postgresql"
    />
  );
}

describe("a column's subtype", () => {
  it("is shown with its base", () => {
    expect(shownType("text", "inet")).toBe("text · inet");
    expect(shownType("numeric(21,4)", "money")).toBe("numeric · money");
    expect(shownType("varchar", null)).toBe("text");
    expect(shownType(null, "inet")).toBe("");
  });

  it("is shown beside the base a suggested column is assigned", () => {
    render(<Editor initial={[column("ip", { subtype: "inet" }), column("name")]} />);
    fireEvent.click(screen.getByTestId("column-tab-type"));
    expect(screen.getByTestId("register-table-col-datatype-ip")).toHaveValue("text");
    expect(screen.getByTestId("register-table-col-subtype-ip")).toHaveTextContent("text · inet");
    expect(screen.queryByTestId("register-table-col-subtype-name")).toBeNull();
  });
});
