// Copyright (c) 2026 Kenneth Stott
// Canary: d1047be9-2c65-4f3a-b9e1-8a5c62f0e713
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2228: the setup form of a dbt source.

import { describe, expect, it, vi, afterEach } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { fireEvent, renderInForm as render, screen, waitFor } from "../../../test-utils/render";
import { DbtFormSection } from "../DbtFormSection";
import { newWarehouse, useDbtSourceForm, warehousesToCreate } from "../dbt";

const offer = {
  project: "jaffle",
  adapter: "postgres",
  models: 3,
  commands: 12,
  warehouses: [{ database: "analytics", source_type: "postgresql", source_id: null }],
};

afterEach(() => vi.restoreAllMocks());

function Harness({ sourceId = "dbt-one" }: { sourceId?: string }) {
  const dbt = useDbtSourceForm();
  return <DbtFormSection dbt={dbt} sourceId={sourceId} />;
}

function previewAnswers(body: unknown) {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(
    new Response(JSON.stringify(body), { status: 200 }),
  );
}

describe("the dbt source's setup", () => {
  it("offers a URL, an upload or dbt Cloud as where the manifest comes from", () => {
    render(<Harness />);
    expect(screen.getByTestId("dbt-spec-location")).toBeTruthy();
    expect(screen.getByTestId("dbt-spec-upload")).toBeTruthy();
    expect(screen.getByTestId("dbt-spec-cloud")).toBeTruthy();
    expect(screen.getByTestId("dbt-location-input")).toBeTruthy();
    expect(screen.getByTestId("dbt-command-line")).toBeTruthy();
  });

  it("asks dbt Cloud for its account, job and token instead of a command", () => {
    render(<Harness />);
    fireEvent.click(screen.getByTestId("dbt-spec-cloud"));
    expect(screen.getByTestId("dbt-cloud-account")).toBeTruthy();
    expect(screen.getByTestId("dbt-cloud-job")).toBeTruthy();
    expect(screen.getByTestId("dbt-cloud-token")).toBeTruthy();
    expect(screen.queryByTestId("dbt-command-line")).toBeNull();
  });

  it("names the warehouse a registered source reads", async () => {
    previewAnswers({
      ...offer,
      warehouses: [{ database: "analytics", source_type: "postgresql", source_id: "pg-main" }],
    });
    render(<Harness />);
    fireEvent.click(screen.getByTestId("dbt-preview"));
    await waitFor(() => expect(screen.getByTestId("dbt-warehouse-found")).toBeTruthy());
    expect(screen.getByTestId("dbt-warehouse-found").textContent).toContain("pg-main");
    expect(screen.queryByTestId("dbt-warehouse-host")).toBeNull();
  });

  it("offers to create a warehouse no registered source reads", async () => {
    previewAnswers(offer);
    render(<Harness />);
    fireEvent.click(screen.getByTestId("dbt-preview"));
    await waitFor(() => expect(screen.getByTestId("dbt-warehouse-host")).toBeTruthy());
    expect(screen.getByTestId("dbt-warehouse-credential")).toBeTruthy();
    expect((screen.getByTestId("dbt-warehouse-id") as HTMLInputElement).value).toBe(
      "dbt-one-analytics",
    );
  });

  it("says so when no source type reads the adapter", async () => {
    previewAnswers({
      ...offer,
      adapter: "exotic",
      warehouses: [{ database: "d", source_type: null, source_id: null }],
    });
    render(<Harness />);
    fireEvent.click(screen.getByTestId("dbt-preview"));
    await waitFor(() => expect(screen.getByTestId("dbt-adapter-unknown")).toBeTruthy());
    expect(screen.queryByTestId("dbt-warehouse-host")).toBeNull();
  });

  it("shows what the server refused", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ detail: "not a dbt manifest" }), { status: 422 }),
    );
    render(<Harness />);
    fireEvent.click(screen.getByTestId("dbt-preview"));
    await waitFor(() => expect(screen.getByTestId("dbt-preview-error")).toBeTruthy());
  });

  it("sends the spec without inventing a credential", () => {
    const { result } = renderHook(() => useDbtSourceForm());
    act(() => result.current.setLocation("s3://b/manifest.json"));
    expect(result.current.specBody()).toMatchObject({
      spec: "location",
      location: "s3://b/manifest.json",
      token: "",
    });
  });
});

describe("the warehouses a registration creates", () => {
  it("creates one filled in with what the manifest knows and what was asked for", () => {
    const fills = {
      analytics: { sourceId: "wh", host: "db.example", username: "u", password: "held-as-entered" },
    };
    expect(warehousesToCreate(offer, fills)).toEqual([
      {
        id: "wh",
        type: "postgresql",
        host: "db.example",
        database: "analytics",
        path: undefined,
        username: "u",
        password: "held-as-entered",
      },
    ]);
  });

  it("creates nothing for a database a source reads, or left empty", () => {
    const read = {
      ...offer,
      warehouses: [{ database: "analytics", source_type: "postgresql", source_id: "pg" }],
    };
    const filled = { analytics: { sourceId: "w", host: "h", username: "", password: "" } };
    expect(warehousesToCreate(read, filled)).toEqual([]);
    expect(
      warehousesToCreate(offer, { analytics: { sourceId: "w", host: " ", username: "", password: "" } }),
    ).toEqual([]);
    expect(warehousesToCreate(null, {})).toEqual([]);
  });

  it("gives a file database its path", () => {
    const duck = {
      ...offer,
      warehouses: [{ database: "jaffle", source_type: "duckdb", source_id: null }],
    };
    const made = warehousesToCreate(duck, {
      jaffle: { sourceId: "w", host: "/data/jaffle.duckdb", username: "", password: "" },
    });
    expect(made[0]).toMatchObject({ host: "", path: "/data/jaffle.duckdb" });
  });

  it("derives a safe source id from the database", () => {
    expect(newWarehouse("dbt.one", "my db").sourceId).toBe("dbt-one-my-db");
  });
});
