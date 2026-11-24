// Copyright (c) 2026 Kenneth Stott
// Canary: 5c7a2e91-0f48-4b63-a8d1-3e6b9f02c7d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225: a saved Spark source's script validated in three steps: the form's script is sent,
// each step's status and each function's result are shown, a function's table parameters are
// given governed tables and a resolver its two keys, and validating again sends them.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor, within } from "../../../test-utils/render";
import type { ValidationReport } from "../../../api/codeHosts";

vi.mock("../../../api/codeHosts", () => ({ validateScript: vi.fn() }));
vi.mock("../../../hooks/useTableQueries", () => ({
  useTables: () => ({
    tables: [
      { id: 1, domainId: "sales", tableName: "orders", withdrawn: false },
      { id: 2, domainId: "geo", tableName: "stores", withdrawn: false },
      { id: 3, domainId: "geo", tableName: "old_regions", withdrawn: true },
    ],
    loading: false,
  }),
}));

import { validateScript } from "../../../api/codeHosts";
import { ScriptValidationPanel } from "../ScriptValidationPanel";

const SCRIPT = "def orders():\n    '''@table'''\n";

const FUNCTIONS = [
  { name: "orders", kind: "table", tables: [], arguments: [], columns: ["id"], resolver: false },
  {
    name: "read_of",
    kind: "query",
    tables: ["orders"],
    arguments: [],
    columns: ["statement"],
    resolver: false,
  },
  {
    name: "match_stores",
    kind: "query",
    tables: ["stores", "regions"],
    arguments: [],
    columns: ["store_id", "region_id", "score"],
    resolver: true,
  },
];

const INCOMPLETE: ValidationReport = {
  cluster: { address: "sc://spark.example:15002", origin: "local_test" },
  static: { status: "ok", functions: FUNCTIONS, problems: [], hash: "h" },
  dry_run: {
    status: "failed",
    functions: [
      {
        function: "orders",
        status: "mismatch",
        rows: 2,
        mismatches: ["the rows do not convert to the declared types: id"],
      },
      { function: "read_of", status: "needs_input", missing: ["orders"] },
      { function: "match_stores", status: "error", rows: 0, error: "RuntimeError: refused" },
    ],
  },
  keys: {
    status: "incomplete",
    functions: [{ function: "match_stores", status: "needs_input", missing: ["keys"] }],
  },
};

const PASSED: ValidationReport = {
  cluster: { address: "sc://spark.example:15002", origin: "organisation" },
  static: { status: "ok", functions: FUNCTIONS, problems: [], hash: "h" },
  dry_run: {
    status: "ok",
    functions: [
      { function: "orders", status: "ok", rows: 2 },
      { function: "read_of", status: "ok", rows: 1 },
      { function: "match_stores", status: "ok", rows: 2 },
    ],
  },
  keys: {
    status: "ok",
    functions: [
      {
        function: "match_stores",
        status: "ok",
        rows: 2,
        source_key: "store_id",
        target_key: "region_id",
        problems: [],
      },
    ],
  },
};

const STATIC_FAILED: ValidationReport = {
  cluster: null,
  static: { status: "failed", functions: [], problems: ["read_of: unknown tag @qurey"] },
  dry_run: { status: "not_run", functions: [], reason: "the static check failed" },
  keys: { status: "not_run", functions: [], reason: "the static check failed" },
};

function show(script = SCRIPT) {
  renderInForm(<ScriptValidationPanel orgId="acme" sourceId="spark_prod" script={script} />);
}

/** A Select opened once, and the options its listbox offers when they are shown. */
async function options(testId: string): Promise<HTMLElement[]> {
  const input = screen.getByTestId(testId);
  fireEvent.click(input);
  return waitFor(() => {
    const list = document.getElementById(input.getAttribute("aria-controls") ?? "");
    const found = [...(list?.querySelectorAll<HTMLElement>('[role="option"]') ?? [])];
    expect(found.length).toBeGreaterThan(0);
    return found;
  });
}

async function pick(testId: string, option: string) {
  const choice = (await options(testId)).find((o) => o.textContent === option);
  expect(choice).toBeDefined();
  fireEvent.click(choice!);
}

beforeEach(() => {
  vi.mocked(validateScript).mockReset();
});

describe("ScriptValidationPanel", () => {
  it("cannot validate an empty script", () => {
    show("  ");
    expect(screen.getByTestId("script-validation-run")).toBeDisabled();
  });

  it("sends the form's script and shows each step and function", async () => {
    vi.mocked(validateScript).mockResolvedValue(INCOMPLETE);
    show();
    fireEvent.click(screen.getByTestId("script-validation-run"));
    await screen.findByTestId("script-validation-report");
    expect(validateScript).toHaveBeenCalledWith("acme", "spark_prod", {
      script: SCRIPT,
      inputs: {},
      keys: {},
    });
    expect(screen.getByTestId("script-validation-cluster")).toHaveTextContent(
      "sc://spark.example:15002 (local test cluster)",
    );
    expect(screen.getByTestId("script-validation-status-static")).toHaveTextContent("Passed");
    expect(screen.getByTestId("script-validation-status-dry_run")).toHaveTextContent("Failed");
    expect(screen.getByTestId("script-validation-status-keys")).toHaveTextContent("Incomplete");
    const orders = screen.getByTestId("script-validation-fn-dry_run-orders");
    expect(orders).toHaveTextContent("Mismatch");
    expect(orders).toHaveTextContent("2 rows");
    expect(orders).toHaveTextContent("the rows do not convert to the declared types: id");
    const readOf = screen.getByTestId("script-validation-fn-dry_run-read_of");
    expect(readOf).toHaveTextContent("Needs input");
    expect(within(readOf).getByTestId("script-validation-missing")).toHaveTextContent(
      "Pick a governed table for orders",
    );
    expect(screen.getByTestId("script-validation-fn-dry_run-match_stores")).toHaveTextContent(
      "RuntimeError: refused",
    );
    expect(screen.getByTestId("script-validation-fn-keys-match_stores")).toHaveTextContent(
      "Needs input",
    );
    expect(screen.getByText("Validate again")).toBeInTheDocument();
  });

  it("validates again with the tables and keys picked", async () => {
    vi.mocked(validateScript).mockResolvedValueOnce(INCOMPLETE).mockResolvedValueOnce(PASSED);
    show();
    fireEvent.click(screen.getByTestId("script-validation-run"));
    await screen.findByTestId("script-validation-report");
    await pick("script-validation-input-read_of-orders", "sales.orders");
    await pick("script-validation-input-match_stores-stores", "geo.stores");
    await pick("script-validation-key-match_stores-source", "store_id");
    await pick("script-validation-key-match_stores-target", "region_id");
    fireEvent.click(screen.getByTestId("script-validation-run"));
    await waitFor(() =>
      expect(screen.getByTestId("script-validation-status-keys")).toHaveTextContent("Passed"),
    );
    expect(validateScript).toHaveBeenLastCalledWith("acme", "spark_prod", {
      script: SCRIPT,
      inputs: { read_of: { orders: "sales.orders" }, match_stores: { stores: "geo.stores" } },
      keys: { match_stores: ["store_id", "region_id"] },
    });
    expect(screen.getByTestId("script-validation-cluster")).toHaveTextContent(
      "the organization's default",
    );
  });

  it("offers only the tables not withdrawn", async () => {
    vi.mocked(validateScript).mockResolvedValue(INCOMPLETE);
    show();
    fireEvent.click(screen.getByTestId("script-validation-run"));
    await screen.findByTestId("script-validation-report");
    const offered = await options("script-validation-input-read_of-orders");
    expect(offered.map((o) => o.textContent)).toEqual(["sales.orders", "geo.stores"]);
  });

  it("shows a failed static check and why nothing else ran", async () => {
    vi.mocked(validateScript).mockResolvedValue(STATIC_FAILED);
    show();
    fireEvent.click(screen.getByTestId("script-validation-run"));
    await screen.findByTestId("script-validation-report");
    expect(screen.getByTestId("script-validation-status-static")).toHaveTextContent("Failed");
    expect(screen.getByTestId("script-validation-problem")).toHaveTextContent(
      "read_of: unknown tag @qurey",
    );
    expect(screen.getByTestId("script-validation-status-dry_run")).toHaveTextContent("Not run");
    expect(screen.getByTestId("script-validation-reason-dry_run")).toHaveTextContent(
      "the static check failed",
    );
    expect(screen.getByTestId("script-validation-cluster")).toHaveTextContent(
      "No Spark cluster is configured",
    );
  });

  it("names a request the server refused", async () => {
    vi.mocked(validateScript).mockRejectedValue(new Error("serves no script"));
    show();
    fireEvent.click(screen.getByTestId("script-validation-run"));
    expect(await screen.findByTestId("script-validation-failed")).toHaveTextContent(
      "serves no script",
    );
  });
});
