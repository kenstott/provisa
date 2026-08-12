// Copyright (c) 2026 Kenneth Stott
// Canary: 849c6649-d1b8-4c97-a851-1c37eb79b255
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";
import { ResultsGrid } from "../../../pages/sql/ResultsGrid";
import { useResultsGrid } from "../../../pages/sql/useResultsGrid";
import { formatTelemetryValue, telemetryIdKind, traceDetailSql } from "../traceDetails";
import { runSql } from "../../../api/admin";

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  runSql: vi.fn(),
}));

const TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
const SPAN = "00f067aa0ba902b7";

describe("telemetryIdKind", () => {
  it("classifies the telemetry id columns and nothing else", () => {
    expect(telemetryIdKind("trace_id")).toBe("trace");
    expect(telemetryIdKind("TRACE_ID")).toBe("trace");
    expect(telemetryIdKind("span_id")).toBe("span");
    expect(telemetryIdKind("parent_span_id")).toBe("span");
    expect(telemetryIdKind("PARENT SPAN ID")).toBe("span");
    expect(telemetryIdKind("query_text")).toBeNull();
    expect(telemetryIdKind("id")).toBeNull();
  });
});

describe("traceDetailSql", () => {
  it("selects every column of the governed ops table", () => {
    expect(traceDetailSql("trace", TRACE)).toBe(
      `SELECT * FROM "ops"."traces" WHERE trace_id = '${TRACE}' ORDER BY "timestamp"`,
    );
    expect(traceDetailSql("span", SPAN)).toBe(
      `SELECT * FROM "ops"."traces" WHERE span_id = '${SPAN}' ORDER BY "timestamp"`,
    );
  });

  it("rejects an id that is not hex rather than interpolating it", () => {
    expect(() => traceDetailSql("trace", "abc' OR '1'='1")).toThrow(/not a telemetry id/);
  });
});

describe("formatTelemetryValue", () => {
  it("expands attribute maps and passes scalars through", () => {
    expect(formatTelemetryValue({ "db.system": "trino" })).toBe(
      '{\n  "db.system": "trino"\n}',
    );
    expect(formatTelemetryValue(42)).toBe("42");
    expect(formatTelemetryValue(null)).toBe("");
  });
});

const ROWS = [{ trace_id: TRACE, span_id: SPAN, query_text: "SELECT 1" }];
const COLS = ["trace_id", "span_id", "query_text"];

function Harness() {
  const grid = useResultsGrid(ROWS, COLS);
  return <ResultsGrid grid={grid} totalRowCount={ROWS.length} />;
}

describe("ResultsGrid telemetry drill-down", () => {
  beforeEach(() => {
    vi.mocked(runSql).mockReset();
  });

  it("links trace ids and shows every column of the span in a modal", async () => {
    vi.mocked(runSql).mockResolvedValue({
      columns: ["trace_id", "span_attributes"],
      rows: [{ trace_id: TRACE, span_attributes: { "db.system": "trino" } }],
    });
    render(<Harness />);

    const link = screen.getByRole("button", { name: TRACE });
    fireEvent.click(link);

    await waitFor(() => expect(vi.mocked(runSql)).toHaveBeenCalledWith(traceDetailSql("trace", TRACE)));
    expect(await screen.findByText("span_attributes")).toBeTruthy();
    expect(screen.getByText(/db\.system/)).toBeTruthy();
  });

  it("leaves non-telemetry columns as plain text", () => {
    render(<Harness />);
    expect(screen.queryByRole("button", { name: "SELECT 1" })).toBeNull();
  });

  it("reports a query error instead of an empty modal", async () => {
    vi.mocked(runSql).mockResolvedValue({ columns: [], rows: [], error: "access denied" });
    render(<Harness />);
    fireEvent.click(screen.getByRole("button", { name: SPAN }));
    expect(await screen.findByText("access denied")).toBeTruthy();
  });
});
