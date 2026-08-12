// Copyright (c) 2026 Kenneth Stott
// Canary: d1710198-bbf1-4dc0-8cd0-4f0f069b094f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1432: the observability tab carries one switch per logical subsystem, and saving sends the
// whole map back.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";

const updateSettings = vi.fn().mockResolvedValue({ updated: ["otel.subsystem_traces"] });

vi.mock("../../../api/admin", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../api/admin")>();
  return {
    ...actual,
    updateSettings: (...a: unknown[]) => updateSettings(...a),
    reloadQueryEngineCatalog: vi.fn(),
    restartQueryEngine: vi.fn(),
    recomputeSchemaClusters: vi.fn(),
  };
});

import { ObservabilityTab } from "../ObservabilityTab";
import type { PlatformSettings } from "../../../api/admin";

function makeSettings(): PlatformSettings {
  return {
    otel: {
      endpoint: "",
      service_name: "provisa",
      sample_rate: 1,
      log_level: "WARNING",
      compact_cron: "* * * * *",
      compact_batch_size: 1000,
      compact_file_chunk: 50,
      ops_snapshot_retention_hours: null,
      span_export_delay_millis: 1000,
      otlp2parquet_max_age_secs: 5,
      collector_batch_timeout_ms: 200,
      s3_endpoint: "http://minio:9000",
      support_endpoint: "",
      support_redact_sql_literals: true,
      support_redact_attributes: [],
      subsystem_traces: {
        http_api: true,
        outbound_http: true,
        catalog_database: false,
        result_cache: true,
        document_sources: true,
        search_sources: true,
        grpc_services: true,
      },
    },
  } as unknown as PlatformSettings;
}

// Mantine puts the data-testid on the Switch's own input element.
const switchFor = (key: string) => screen.getByTestId(`subsystem-trace-${key}`);

// The settings pane is tabbed; the switches live behind the subsystems tab.
const openSubsystems = () => fireEvent.click(screen.getByRole("tab", { name: "Subsystem traces" }));

describe("ObservabilityTab subsystem trace switches", () => {
  beforeEach(() => updateSettings.mockClear());

  it("renders the catalog database off and the rest on", () => {
    render(<ObservabilityTab settings={makeSettings()} setSettings={vi.fn()} />);
    openSubsystems();

    expect(switchFor("catalog_database")).not.toBeChecked();
    expect(switchFor("http_api")).toBeChecked();
    expect(switchFor("grpc_services")).toBeChecked();
  });

  it("hands the whole map back with only the toggled subsystem changed", () => {
    const setSettings = vi.fn();
    render(<ObservabilityTab settings={makeSettings()} setSettings={setSettings} />);
    openSubsystems();

    fireEvent.click(switchFor("catalog_database"));

    expect(setSettings).toHaveBeenCalledTimes(1);
    expect(setSettings.mock.calls[0][0].otel.subsystem_traces).toEqual({
      http_api: true,
      outbound_http: true,
      catalog_database: true,
      result_cache: true,
      document_sources: true,
      search_sources: true,
      grpc_services: true,
    });
  });

  it("saves the switches with the rest of the otel settings", () => {
    const settings = makeSettings();
    render(<ObservabilityTab settings={settings} setSettings={vi.fn()} />);

    fireEvent.click(screen.getByLabelText("Save"));

    expect(updateSettings).toHaveBeenCalledWith({ otel: settings.otel });
  });
});
