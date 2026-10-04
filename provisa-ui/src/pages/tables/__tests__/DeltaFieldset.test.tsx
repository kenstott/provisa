// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-874: the table form's incremental-reload (delta) fieldset — enabling it declares a delta,
// it warns when the table has no watermark (the cursor), and the tombstone column shows only for
// tombstone deletes.

import { useState } from "react";
import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { DeltaFieldset } from "../DeltaFieldset";
import type { DeltaConfig, RegisteredTable } from "../../../types/admin";

function Harness({ watermark, delta }: { watermark: string | null; delta: DeltaConfig | null }) {
  const [tbl, setTbl] = useState<RegisteredTable | null>({
    watermarkColumn: watermark,
    delta,
  } as RegisteredTable);
  return (
    <DeltaFieldset
      editingTable={tbl as RegisteredTable}
      setEditingTable={setTbl as React.Dispatch<React.SetStateAction<RegisteredTable | null>>}
    />
  );
}

describe("DeltaFieldset (REQ-874)", () => {
  it("hides the delta controls until it is enabled, then shows them", () => {
    render(<Harness watermark="updated_at" delta={null} />);
    expect(screen.queryByTestId("delta-apply")).toBeNull();
    fireEvent.click(screen.getByTestId("delta-enable"));
    expect(screen.getByTestId("delta-apply")).toBeTruthy();
    expect(screen.getByTestId("delta-deletes")).toBeTruthy();
  });

  it("warns when the table has no watermark column (the cursor a delta reads past)", () => {
    render(<Harness watermark={null} delta={{ apply: "upsert", deletes: "none" }} />);
    expect(screen.getByTestId("delta-no-watermark")).toBeTruthy();
  });

  it("does not warn once a watermark column is set", () => {
    render(<Harness watermark="updated_at" delta={{ apply: "upsert", deletes: "none" }} />);
    expect(screen.queryByTestId("delta-no-watermark")).toBeNull();
  });

  it("shows the tombstone column field only for tombstone deletes", () => {
    render(
      <Harness
        watermark="updated_at"
        delta={{ apply: "upsert", deletes: "tombstone", tombstoneColumn: "_deleted" }}
      />,
    );
    expect(screen.getByTestId("delta-tombstone-column")).toBeTruthy();
  });

  it("has no tombstone column field for deletes=none", () => {
    render(<Harness watermark="updated_at" delta={{ apply: "upsert", deletes: "none" }} />);
    expect(screen.queryByTestId("delta-tombstone-column")).toBeNull();
  });
});
