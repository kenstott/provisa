// Copyright (c) 2026 Kenneth Stott
// Canary: 5c81e2a7-0d34-4b96-8f71-2ae63c9d5810
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1443 clause 7: the contract builder panel. What these hold is the panel's one claim — the rows
// ARE the editor and the contract text is serialized from them server-side: the rows come from
// parsing the registered text, editing a row's args or the dataset rewrites the whole contract, the
// picker offers only what the server says a column's type supports, and adding a check hands back
// new TEXT rather than a model the text would have to be reconciled with later.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";

const parseContract = vi.fn();
const buildContract = vi.fn();
const checkCatalog = vi.fn();
const buildCheck = vi.fn();
const dryRunContract = vi.fn();

// vmThreads shares the module registry, so declare a complete pass-through and replace only the one
// hook under test — a partial mock would drop the other hooks this module exports.
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useDqContract: () => ({
    parseContract,
    buildContract,
    checkCatalog,
    buildCheck,
    dryRunContract,
  }),
}));

import { DataQualityPanel } from "../DataQualityPanel";

const SODA =
  "dataset: provisa/sales/orders\ncolumns:\n  - name: customer\n    checks:\n      - missing:\n";

function kind(checkType: string, over: Record<string, unknown> = {}) {
  return {
    checkType,
    scope: "column",
    params: [],
    comparators: [],
    metrics: [],
    levels: ["fail", "warn"],
    thresholdUnits: [],
    ...over,
  };
}

const CATALOG = {
  error: null,
  datasetChecks: [kind("row_count", { scope: "dataset" })],
  columns: [
    { name: "amount", dataType: "numeric", checks: [kind("aggregate"), kind("missing")] },
    { name: "customer", dataType: "text", checks: [kind("missing")] },
  ],
};

function renderPanel(onChange = vi.fn(), contractText = SODA) {
  render(
    <DataQualityPanel
      checker="soda"
      sourceId="dq"
      contractText={contractText}
      onChange={onChange}
    />,
  );
  return onChange;
}

beforeEach(() => {
  vi.clearAllMocks();
  parseContract.mockResolvedValue({
    dataset: "provisa/sales/orders",
    checks: [{ columnName: "customer", checkType: "missing", definition: "missing:" }],
    error: null,
  });
  checkCatalog.mockResolvedValue(CATALOG);
  buildCheck.mockResolvedValue({ definition: "aggregate:\n  function: avg", error: null });
  buildContract.mockResolvedValue({ text: "REBUILT", error: null });
});

describe("DataQualityPanel", () => {
  it("shows the dataset and each check's own args, so a hand-written contract survives being opened", async () => {
    renderPanel();
    expect(await screen.findByTestId("dq-dataset-input")).toHaveValue("provisa/sales/orders");
    expect(screen.getByTestId("dq-args-0")).toHaveValue("missing:");
    expect(screen.queryByTestId("dq-contract-text")).not.toBeInTheDocument();
  });

  it("editing a row's args rewrites the contract from all the rows", async () => {
    const onChange = renderPanel();
    await screen.findByTestId("dq-check-rows");
    const args = screen.getByTestId("dq-args-0");
    fireEvent.change(args, { target: { value: "missing:\n  threshold: 0" } });
    fireEvent.blur(args);

    await waitFor(() =>
      expect(buildContract).toHaveBeenCalledWith({
        checker: "soda",
        dataset: "provisa/sales/orders",
        checks: [
          {
            columnName: "customer",
            checkType: "missing",
            definition: "missing:\n  threshold: 0",
          },
        ],
      }),
    );
    expect(onChange).toHaveBeenCalledWith("REBUILT");
  });

  it("retyping the dataset re-aims the contract at another table", async () => {
    renderPanel();
    const input = await screen.findByTestId("dq-dataset-input");
    fireEvent.change(input, { target: { value: "provisa/sales/invoices" } });
    fireEvent.blur(input);

    await waitFor(() =>
      expect(buildContract).toHaveBeenCalledWith({
        checker: "soda",
        dataset: "provisa/sales/invoices",
        checks: [{ columnName: "customer", checkType: "missing", definition: "missing:" }],
      }),
    );
  });

  it("leaving a row untouched rewrites nothing", async () => {
    renderPanel();
    await screen.findByTestId("dq-check-rows");
    fireEvent.blur(screen.getByTestId("dq-args-0"));
    fireEvent.blur(screen.getByTestId("dq-dataset-input"));
    expect(buildContract).not.toHaveBeenCalled();
  });

  it("lists the checks the server parsed out of that text", async () => {
    renderPanel();
    const rows = await screen.findByTestId("dq-check-rows");
    expect(rows).toHaveTextContent("customer");
    expect(rows).toHaveTextContent("missing");
  });

  it("scopes the picker by the dataset the contract names, not by a table it was handed", async () => {
    renderPanel();
    await waitFor(() =>
      expect(checkCatalog).toHaveBeenCalledWith({
        checker: "soda",
        dataset: "provisa/sales/orders",
      }),
    );
  });

  it("offers a column only the checks its own type supports", async () => {
    renderPanel();
    await screen.findByTestId("dq-check-rows");

    fireEvent.click(screen.getByTestId("dq-column"));
    fireEvent.click(await screen.findByText("amount — numeric"));
    fireEvent.click(screen.getByTestId("dq-check-type"));
    expect(await screen.findByText("aggregate")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("dq-column"));
    fireEvent.click(await screen.findByText("customer — text"));
    fireEvent.click(screen.getByTestId("dq-check-type"));
    await waitFor(() => expect(screen.queryByText("aggregate")).not.toBeInTheDocument());
  });

  it("writes an added check back as contract TEXT rather than keeping its own model", async () => {
    const onChange = renderPanel();
    await screen.findByTestId("dq-check-rows");

    fireEvent.click(screen.getByTestId("dq-column"));
    fireEvent.click(await screen.findByText("amount — numeric"));
    fireEvent.click(screen.getByTestId("dq-check-type"));
    fireEvent.click(await screen.findByText("aggregate"));
    fireEvent.click(screen.getByTestId("dq-add-check"));

    await waitFor(() => expect(onChange).toHaveBeenCalledWith("REBUILT"));
    // The rewrite carries the parsed rows plus the new one — the text is rebuilt from all of them,
    // never patched, so nothing the parser understood can be dropped on the way back out.
    expect(buildContract).toHaveBeenCalledWith({
      checker: "soda",
      dataset: "provisa/sales/orders",
      checks: [
        { columnName: "customer", checkType: "missing", definition: "missing:" },
        { columnName: "amount", checkType: "aggregate", definition: "aggregate:\n  function: avg" },
      ],
    });
  });

  it("removing a check rewrites the text without it", async () => {
    const onChange = renderPanel();
    await screen.findByTestId("dq-check-rows");
    fireEvent.click(screen.getByTestId("dq-remove-0"));
    await waitFor(() =>
      expect(buildContract).toHaveBeenCalledWith({
        checker: "soda",
        dataset: "provisa/sales/orders",
        checks: [],
      }),
    );
    expect(onChange).toHaveBeenCalledWith("REBUILT");
  });

  it("shows the message when the registered contract does not parse, and offers no rows to edit", async () => {
    parseContract.mockResolvedValue({
      dataset: null,
      checks: [],
      error: "contract is not parseable YAML",
    });
    const onChange = renderPanel(vi.fn(), "dataset: [unclosed");
    expect(await screen.findByTestId("dq-parse-error")).toHaveTextContent("not parseable");
    expect(screen.queryByTestId("dq-check-rows")).not.toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  });

  it("says so when the contract's dataset resolves to no governed table", async () => {
    checkCatalog.mockResolvedValue({
      error: "contract dataset 'provisa/sales/invoices' resolves to no governed table",
      datasetChecks: [],
      columns: [],
    });
    renderPanel();
    expect(await screen.findByTestId("dq-catalog-error")).toHaveTextContent("no governed table");
  });

  it("reports a build the server refused instead of writing text it rejected", async () => {
    buildCheck.mockResolvedValue({
      definition: "",
      error: "check 'aggregate' requires 'function'",
    });
    const onChange = renderPanel();
    await screen.findByTestId("dq-check-rows");

    fireEvent.click(screen.getByTestId("dq-column"));
    fireEvent.click(await screen.findByText("amount — numeric"));
    fireEvent.click(screen.getByTestId("dq-check-type"));
    fireEvent.click(await screen.findByText("aggregate"));
    fireEvent.click(screen.getByTestId("dq-add-check"));

    expect(await screen.findByTestId("dq-build-error")).toHaveTextContent("requires 'function'");
    expect(onChange).not.toHaveBeenCalled();
    expect(buildContract).not.toHaveBeenCalled();
  });

  it("dry-runs the text on screen and reads back what the scan observed", async () => {
    dryRunContract.mockResolvedValue({
      success: true,
      message: "scanned provisa/sales/orders",
      checks: [
        {
          columnName: "customer",
          checkType: "missing",
          outcome: "pass",
          value: "0",
          failedRows: 0,
        },
      ],
    });
    renderPanel();
    await screen.findByTestId("dq-check-rows");
    fireEvent.click(screen.getByTestId("dq-dry-run"));

    const result = await screen.findByTestId("dq-dry-run-result");
    expect(result).toHaveTextContent("scanned provisa/sales/orders");
    expect(result).toHaveTextContent("pass");
    expect(dryRunContract).toHaveBeenCalledWith({ sourceId: "dq", contractText: SODA });
  });
});
