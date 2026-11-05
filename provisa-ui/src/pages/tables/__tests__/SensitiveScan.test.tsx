// Copyright (c) 2026 Kenneth Stott
// Canary: 3724f94d-6382-4c27-9a19-137cecab6bff
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147 (CONTAINED VALUES ARE HINTS): the column access settings ask for a scan for sensitive
// values; the scan runs in the background, its status is shown and followed to its end, and each
// column's hints read "Matches <noun> in N% of sampled values" beside its grant and masks. A table
// not saved yet cannot be scanned and says why.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const api = vi.hoisted(() => ({
  fetchSensitiveScan: vi.fn(),
  startSensitiveScan: vi.fn(),
}));
vi.mock("../../../api/profiler", () => api);

import { SensitiveHintText, SensitiveScanBar } from "../SensitiveScan";
import { useSensitiveScan } from "../useSensitiveScan";

const RUNNING = {
  scanId: "s1",
  status: "running",
  startedAt: "2026-11-05T12:00:00+00:00",
  finishedAt: null,
  sampledRows: null,
  scannedColumns: null,
  error: null,
};
const SUCCEEDED = {
  ...RUNNING,
  status: "succeeded",
  finishedAt: "2026-11-05T12:00:03+00:00",
  sampledRows: 2000,
  scannedColumns: 3,
};
const SSN_HINT = {
  column: "notes",
  kind: "us_ssn",
  label: "US social security number",
  sensitive: true,
  valuesTested: 10,
  valuesMatched: 3,
  share: 0.3,
  evidence: "matches a US social security number in 30% of sampled values",
};

function Harness({ tableId }: { tableId: number | undefined }) {
  const scan = useSensitiveScan(tableId);
  return (
    <>
      <SensitiveScanBar scan={scan} />
      <SensitiveHintText column="notes" hints={scan.hintsOf("notes")} />
      <SensitiveHintText column="city" hints={scan.hintsOf("city")} />
    </>
  );
}

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
});

describe("SensitiveScan", () => {
  it("a table not saved yet cannot be scanned, and says why", () => {
    render(<Harness tableId={undefined} />);
    expect(screen.getByTestId("sensitive-scan-start")).toBeDisabled();
    expect(screen.getByTestId("sensitive-scan-status")).toHaveTextContent(
      "Save the table to scan its values.",
    );
    expect(api.fetchSensitiveScan).not.toHaveBeenCalled();
  });

  it("starts a scan, follows it while it runs, then shows each column's hints", async () => {
    api.fetchSensitiveScan
      .mockResolvedValueOnce({ scan: null, hints: [] })
      .mockResolvedValue({ scan: SUCCEEDED, hints: [SSN_HINT] });
    api.startSensitiveScan.mockResolvedValue(RUNNING);
    render(<Harness tableId={7} />);
    await waitFor(() => expect(api.fetchSensitiveScan).toHaveBeenCalledWith(7));
    expect(screen.queryByTestId("sensitive-hint-notes")).toBeNull();

    fireEvent.click(screen.getByTestId("sensitive-scan-start"));
    await waitFor(() =>
      expect(screen.getByTestId("sensitive-scan-status")).toHaveTextContent(
        "Scanning a sample of the text columns",
      ),
    );
    expect(api.startSensitiveScan).toHaveBeenCalledWith(7);
    expect(screen.getByTestId("sensitive-scan-start")).toBeDisabled();

    // The running scan is looked at again until it ends.
    await waitFor(
      () =>
        expect(screen.getByTestId("sensitive-hint-notes")).toHaveTextContent(
          "Matches a US social security number in 30% of sampled values",
        ),
      { timeout: 4000 },
    );
    expect(screen.getByTestId("sensitive-scan-status")).toHaveTextContent(
      "Last scan: 2000 sampled rows, 3 text columns.",
    );
    expect(screen.getByTestId("sensitive-scan-start")).not.toBeDisabled();
    expect(screen.queryByTestId("sensitive-hint-city")).toBeNull();
  });

  it("a failed scan says why", async () => {
    api.fetchSensitiveScan.mockResolvedValue({
      scan: { ...RUNNING, status: "failed", error: "table 7 is no longer registered" },
      hints: [],
    });
    render(<Harness tableId={7} />);
    await waitFor(() =>
      expect(screen.getByTestId("sensitive-scan-status")).toHaveTextContent(
        "The scan failed: table 7 is no longer registered",
      ),
    );
  });

  it("a refused start is shown", async () => {
    api.fetchSensitiveScan.mockResolvedValue({ scan: null, hints: [] });
    api.startSensitiveScan.mockRejectedValue(new Error("No table_registration right"));
    render(<Harness tableId={7} />);
    await waitFor(() => expect(api.fetchSensitiveScan).toHaveBeenCalled());
    fireEvent.click(screen.getByTestId("sensitive-scan-start"));
    await waitFor(() =>
      expect(screen.getByTestId("sensitive-scan-error")).toHaveTextContent(
        "No table_registration right",
      ),
    );
  });
});
