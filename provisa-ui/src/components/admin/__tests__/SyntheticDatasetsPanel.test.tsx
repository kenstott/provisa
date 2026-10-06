// Copyright (c) 2026 Kenneth Stott
// Canary: 0d6e2f85-9a31-4b7c-a4e8-f1c3b5d97a20
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1939: an environment's synthetic datasets are defined, generated, reported on and dropped
// from the Environments page; production is never offered; creating an environment may seed it.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const api = vi.hoisted(() => ({
  fetchDatasets: vi.fn(),
  fetchProfileRuns: vi.fn(),
  fetchRelationships: vi.fn(),
  defineDataset: vi.fn(),
  generateDataset: vi.fn(),
  fetchReport: vi.fn(),
  dropDataset: vi.fn(),
}));
vi.mock("../../../api/synthetic", () => api);

import { SyntheticDatasetsPanel } from "../SyntheticDatasetsPanel";
import { SyntheticSeedFields } from "../SyntheticSeedFields";
import { EMPTY_SEED } from "../syntheticSeed";

const DATASET = {
  id: "load_test",
  seed: 7,
  scale: 2,
  status: "generated",
  storeSchema: "org_a_env_dev_syn__load_test",
  error: null,
  generatedAt: "2026-10-06T00:00:00Z",
  tables: [{ tableId: 3, profileEnv: "prod", runId: "r1", scale: null }],
  fanoutConditions: [],
  assertions: [],
  privateEpsilon: null,
  closenessThreshold: null,
  closenessDraws: null,
};

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  api.fetchDatasets.mockResolvedValue([DATASET]);
  api.fetchProfileRuns.mockResolvedValue([
    {
      tableId: 3,
      tableName: "customers",
      runs: [{ runId: "r1", runTime: "2026-10-05T03:00:00Z", rowCount: 200 }],
    },
    {
      tableId: 4,
      tableName: "orders",
      runs: [{ runId: "r2", runTime: "2026-10-05T03:00:00Z", rowCount: 800 }],
    },
  ]);
  api.fetchRelationships.mockResolvedValue([
    {
      id: "cust-orders",
      parentTableId: 3,
      parentTable: "customers",
      childTableId: 4,
      childTable: "orders",
    },
  ]);
  api.defineDataset.mockResolvedValue({ id: "big", storeSchema: "s" });
  api.generateDataset.mockResolvedValue({ id: "load_test", status: "generated" });
  api.fetchReport.mockResolvedValue([
    {
      table: "customers",
      column: null,
      measure: "row_count",
      source: 200,
      synthetic: 400,
      delta: 2,
      note: "expected ratio 2",
    },
    {
      table: "",
      column: null,
      measure: "assertion",
      source: null,
      synthetic: 0,
      delta: null,
      note: "SELECT false",
    },
    {
      table: "orders",
      column: null,
      measure: "conditional_parents",
      source: null,
      synthetic: 12,
      delta: null,
      note: "tier = 'gold'",
    },
  ]);
});

describe("SyntheticDatasetsPanel", () => {
  it("offers only non-production environments and acts in the one chosen", async () => {
    render(<SyntheticDatasetsPanel envs={["prod", "dev"]} />);
    expect(await screen.findByTestId("synthetic-row-load_test")).toBeInTheDocument();
    expect(api.fetchDatasets).toHaveBeenCalledWith("dev");
    expect(api.fetchProfileRuns).toHaveBeenCalledWith("dev", "prod");

    fireEvent.click(screen.getByTestId("synthetic-generate-load_test"));
    await waitFor(() => expect(api.generateDataset).toHaveBeenCalledWith("dev", "load_test"));

    fireEvent.click(screen.getByTestId("synthetic-report-load_test"));
    const report = await screen.findByTestId("synthetic-report");
    expect(report).toHaveTextContent("expected ratio 2");
    expect(report).toHaveTextContent("Assertion");
    expect(report).toHaveTextContent("False");
    expect(report).toHaveTextContent("Parents meeting the condition");
  });

  it("defines a dataset of the picked tables and their profile runs", async () => {
    render(<SyntheticDatasetsPanel envs={["prod", "dev"]} />);
    fireEvent.click(await screen.findByTestId("synthetic-table-customers"));
    fireEvent.change(screen.getByTestId("synthetic-name"), { target: { value: "big" } });
    fireEvent.click(screen.getByTestId("synthetic-define"));
    await waitFor(() =>
      expect(api.defineDataset).toHaveBeenCalledWith("dev", "big", {
        seed: 1,
        scale: 1,
        tables: [{ tableId: 3, profileEnv: "prod", runId: "r1", scale: null }],
        fanoutConditions: [],
        assertions: [],
        privateEpsilon: null,
        closenessThreshold: null,
        closenessDraws: null,
      }),
    );
  });

  it("checks closeness to real rows with a threshold and a number of draws", async () => {
    render(<SyntheticDatasetsPanel envs={["prod", "dev"]} />);
    fireEvent.click(await screen.findByTestId("synthetic-table-customers"));
    fireEvent.click(screen.getByTestId("synthetic-closeness"));
    fireEvent.change(screen.getByTestId("synthetic-closeness-draws"), { target: { value: "5" } });
    fireEvent.change(screen.getByTestId("synthetic-name"), { target: { value: "big" } });
    fireEvent.click(screen.getByTestId("synthetic-define"));
    await waitFor(() =>
      expect(api.defineDataset).toHaveBeenCalledWith(
        "dev",
        "big",
        expect.objectContaining({ closenessThreshold: 0.5, closenessDraws: 5 }),
      ),
    );
  });

  it("declares a dataset private with its privacy budget", async () => {
    render(<SyntheticDatasetsPanel envs={["prod", "dev"]} />);
    fireEvent.click(await screen.findByTestId("synthetic-table-customers"));
    fireEvent.click(screen.getByTestId("synthetic-private"));
    fireEvent.change(screen.getByTestId("synthetic-epsilon"), { target: { value: "0.5" } });
    fireEvent.change(screen.getByTestId("synthetic-name"), { target: { value: "big" } });
    fireEvent.click(screen.getByTestId("synthetic-define"));
    await waitFor(() =>
      expect(api.defineDataset).toHaveBeenCalledWith(
        "dev",
        "big",
        expect.objectContaining({ privateEpsilon: 0.5 }),
      ),
    );
  });

  it("offers conditional fan-out over the relationships whose tables are picked, and assertions", async () => {
    render(<SyntheticDatasetsPanel envs={["prod", "dev"]} />);
    fireEvent.click(await screen.findByTestId("synthetic-table-customers"));
    expect(screen.getByTestId("synthetic-add-condition")).toBeDisabled();
    fireEvent.click(screen.getByTestId("synthetic-table-orders"));
    await waitFor(() => expect(screen.getByTestId("synthetic-add-condition")).toBeEnabled());
    fireEvent.click(screen.getByTestId("synthetic-add-assertion"));
    fireEvent.change(screen.getByRole("textbox", { name: "Assertion" }), {
      target: { value: "SELECT true" },
    });
    fireEvent.change(screen.getByTestId("synthetic-name"), { target: { value: "big" } });
    fireEvent.click(screen.getByTestId("synthetic-define"));
    await waitFor(() =>
      expect(api.defineDataset).toHaveBeenCalledWith(
        "dev",
        "big",
        expect.objectContaining({ assertions: ["SELECT true"], fanoutConditions: [] }),
      ),
    );
  });

  it("says production cannot hold one when no other environment exists", () => {
    render(<SyntheticDatasetsPanel envs={["prod"]} />);
    expect(screen.getByTestId("synthetic-no-env")).toBeInTheDocument();
    expect(api.fetchDatasets).not.toHaveBeenCalled();
  });
});

describe("SyntheticSeedFields", () => {
  it("turns the seed option on and off", () => {
    const setSeed = vi.fn();
    const { rerender } = render(
      <SyntheticSeedFields envs={["prod"]} seed={null} setSeed={setSeed} />,
    );
    fireEvent.click(screen.getByTestId("env-new-synthetic"));
    expect(setSeed).toHaveBeenCalledWith(EMPTY_SEED);
    rerender(<SyntheticSeedFields envs={["prod"]} seed={EMPTY_SEED} setSeed={setSeed} />);
    expect(screen.getByTestId("env-new-synthetic-name")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("env-new-synthetic"));
    expect(setSeed).toHaveBeenLastCalledWith(null);
  });
});
