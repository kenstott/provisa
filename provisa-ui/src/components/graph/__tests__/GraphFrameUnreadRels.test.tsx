// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-392: a relationship type the auto-impute could not read is shown in the frame, with the
// server's reason on its hint, as a count the engine could not make is shown as "?". It is never
// left out of the view unsaid.

import { afterEach, describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { GraphFrame } from "../GraphFrame";
import type { FrameData, GNode } from "../graph-model";

vi.mock("../GraphCanvas", () => ({
  GraphCanvas: () => null,
  GraphCutNote: () => null,
}));

const CUSTOMER: GNode = { id: 1, label: "Customer", tableLabel: "customers", properties: {} };
const ORDER: GNode = { id: 2, label: "Order", tableLabel: "orders", properties: {} };

function frame(): FrameData {
  return {
    id: "f1",
    query: "MATCH (n) RETURN n",
    status: "done",
    nodes: new Map([
      ["1", CUSTOMER],
      ["2", ORDER],
    ]),
    edges: new Map(),
    rows: [],
    columns: [],
  };
}

function imputeAnswers(body: unknown) {
  const fetchMock = vi.fn(async (url: string) => {
    if (url === "/data/impute-relationships") {
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    throw new Error(`unexpected fetch ${url}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function show(autoImpute: boolean) {
  return render(
    <GraphFrame
      frame={frame()}
      language="cypher"
      autoImpute={autoImpute}
      onClose={() => {}}
      onRerun={() => {}}
      schemaRels={[]}
      parameterized={new Set()}
      pathScope={{ nodeLabels: [], checkedDomains: new Set() }}
      colorOverrides={{}}
      sizeOverrides={{}}
      labelProperty={{}}
      sizeByProperty={{}}
      sizeMultiplier={{}}
      relLineOverrides={{}}
      onColorChange={() => {}}
      pkMap={{}}
      labelToTableLabel={{}}
      labelToTableId={{}}
    />,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("auto-impute relationship types it could not read", () => {
  it("shows each unread type as '?' with its reason on the hint", async () => {
    imputeAnswers({
      columns: ["node"],
      rows: [],
      unread: [
        { type: "PLACED", error: "permission denied for table orders" },
        { type: "SHIPPED_TO", error: "column ship_id does not exist" },
      ],
    });
    show(true);

    const badges = await screen.findAllByTestId("graph-frame-unread-rel");
    expect(badges.map((b) => b.textContent)).toEqual(["PLACED(?)", "SHIPPED_TO(?)"]);

    fireEvent.mouseEnter(badges[0]);
    expect(
      await screen.findByText("PLACED not imputed: permission denied for table orders"),
    ).toBeTruthy();
    fireEvent.mouseLeave(badges[0]);

    fireEvent.mouseEnter(badges[1]);
    expect(
      await screen.findByText("SHIPPED_TO not imputed: column ship_id does not exist"),
    ).toBeTruthy();
  });

  it("shows nothing when every type was read", async () => {
    const fetchMock = imputeAnswers({ columns: ["node"], rows: [], unread: [] });
    show(true);
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(screen.queryByTestId("graph-frame-unread-rel")).toBeNull();
  });

  it("clears the unread types when auto-impute is turned off", async () => {
    imputeAnswers({
      columns: ["node"],
      rows: [],
      unread: [{ type: "PLACED", error: "permission denied for table orders" }],
    });
    show(true);
    await screen.findByTestId("graph-frame-unread-rel");
    fireEvent.click(screen.getByTestId("graph-frame-auto-impute-btn"));
    expect(screen.queryByTestId("graph-frame-unread-rel")).toBeNull();
  });
});
