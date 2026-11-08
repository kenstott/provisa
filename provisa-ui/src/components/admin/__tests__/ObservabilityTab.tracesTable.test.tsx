// Copyright (c) 2026 Kenneth Stott
// Canary: 4b8f2d71-6c3e-49a5-9e0d-1a7c5b3f8e26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-302, REQ-303, REQ-1349: the tab says when an endpoint is set but the ops traces table is
// not filling, from what the server reports of the table, and says what the status means.

import { describe, it, expect, vi, afterEach, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../../test-utils/render";

vi.mock("../../../api/admin", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../api/admin")>();
  return {
    ...actual,
    updateSettings: vi.fn(),
    reloadQueryEngineCatalog: vi.fn(),
    restartQueryEngine: vi.fn(),
    recomputeSchemaClusters: vi.fn(),
  };
});

import { PlatformObservabilityTab as ObservabilityTab } from "../ObservabilityTab";
import type { PlatformSettings } from "../../../api/admin";

function makeSettings(endpoint: string): PlatformSettings {
  return {
    otel: {
      endpoint,
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

function serve(latest: object) {
  const fetchMock = vi.fn(async (url: string) =>
    String(url).includes("/admin/traces/latest")
      ? new Response(JSON.stringify(latest))
      : new Response(JSON.stringify({ traces: [] })),
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

const asked = (fetchMock: ReturnType<typeof serve>) =>
  fetchMock.mock.calls.some(([url]) => String(url).includes("/admin/traces/latest"));

afterEach(() => vi.unstubAllGlobals());

describe("ObservabilityTab status line", () => {
  it("says traces are kept for the page when no endpoint is set", () => {
    serve({ tracked: true, latest: null, age_seconds: null, stale_after_seconds: 600 });
    render(<ObservabilityTab settings={makeSettings("")} setSettings={vi.fn()} />);

    expect(
      screen.getByText("Active (traces are kept for this page, not exported)"),
    ).toBeInTheDocument();
    expect(screen.queryByText(/spans dropped/)).toBeNull();
  });
});

describe("ObservabilityTab traces table notice", () => {
  it("does not ask the server while no endpoint is set", () => {
    const fetchMock = serve({
      tracked: true,
      latest: null,
      age_seconds: null,
      stale_after_seconds: 600,
    });
    render(<ObservabilityTab settings={makeSettings("")} setSettings={vi.fn()} />);

    expect(asked(fetchMock)).toBe(false);
    expect(screen.queryByTestId("traces-table-stale")).toBeNull();
  });

  it("warns when the table's newest row is older than the window", async () => {
    serve({
      tracked: true,
      latest: "2026-11-05T09:00:00",
      age_seconds: 3600,
      stale_after_seconds: 600,
    });
    render(
      <ObservabilityTab
        settings={makeSettings("https://otlp.example:4318")}
        setSettings={vi.fn()}
      />,
    );

    const notice = await screen.findByTestId("traces-table-stale");
    expect(notice).toHaveTextContent("received no rows in the last 10 minutes");
  });

  it("warns when the table has no rows at all", async () => {
    serve({ tracked: true, latest: null, age_seconds: null, stale_after_seconds: 600 });
    render(
      <ObservabilityTab
        settings={makeSettings("https://otlp.example:4318")}
        setSettings={vi.fn()}
      />,
    );

    expect(await screen.findByTestId("traces-table-stale")).toHaveTextContent("have no rows yet");
  });

  it("stays quiet while the table is receiving rows", async () => {
    const fetchMock = serve({
      tracked: true,
      latest: "2026-11-05T09:00:00",
      age_seconds: 30,
      stale_after_seconds: 600,
    });
    render(
      <ObservabilityTab
        settings={makeSettings("https://otlp.example:4318")}
        setSettings={vi.fn()}
      />,
    );

    await waitFor(() => expect(asked(fetchMock)).toBe(true));
    await new Promise((resolve) => setTimeout(resolve, 0)); // the answer has been applied
    expect(screen.queryByTestId("traces-table-stale")).toBeNull();
  });

  it("stays quiet for an engine that reads no traces table", async () => {
    const fetchMock = serve({ tracked: false, stale_after_seconds: 600 });
    render(
      <ObservabilityTab
        settings={makeSettings("https://otlp.example:4318")}
        setSettings={vi.fn()}
      />,
    );

    await waitFor(() => expect(asked(fetchMock)).toBe(true));
    expect(screen.queryByTestId("traces-table-stale")).toBeNull();
  });
});

describe("ObservabilityTab traces table check failure", () => {
  const endpoint = "https://otlp.example:4318";
  const goodLatest = {
    tracked: true,
    latest: "2026-11-05T09:00:00",
    age_seconds: 30,
    stale_after_seconds: 600,
  };

  beforeEach(() => vi.useFakeTimers({ shouldAdvanceTime: true }));
  afterEach(() => vi.useRealTimers());

  const other = () => new Response(JSON.stringify({ traces: [] }));

  it("says the table could not be checked, with the status, when the server answers 500", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) =>
        String(url).includes("/admin/traces/latest")
          ? new Response("boom", { status: 500 })
          : other(),
      ),
    );
    render(<ObservabilityTab settings={makeSettings(endpoint)} setSettings={vi.fn()} />);

    const notice = await screen.findByTestId("traces-table-unchecked");
    expect(notice).toHaveTextContent("could not be checked");
    expect(notice).toHaveTextContent("500");
  });

  it("says the table could not be checked, with the message, on a network error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        if (String(url).includes("/admin/traces/latest")) throw new TypeError("Failed to fetch");
        return other();
      }),
    );
    render(<ObservabilityTab settings={makeSettings(endpoint)} setSettings={vi.fn()} />);

    const notice = await screen.findByTestId("traces-table-unchecked");
    expect(notice).toHaveTextContent("could not be checked");
    expect(notice).toHaveTextContent("Failed to fetch");
  });

  it("clears the notice when the next check succeeds, and keeps checking", async () => {
    let failing = true;
    const fetchMock = vi.fn(async (url: string) => {
      if (!String(url).includes("/admin/traces/latest")) return other();
      return failing
        ? new Response("boom", { status: 503 })
        : new Response(JSON.stringify(goodLatest));
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<ObservabilityTab settings={makeSettings(endpoint)} setSettings={vi.fn()} />);

    await screen.findByTestId("traces-table-unchecked");
    failing = false;
    const checks = () =>
      fetchMock.mock.calls.filter(([u]) => String(u).includes("/admin/traces/latest")).length;
    const before = checks();
    await vi.advanceTimersByTimeAsync(30_000);

    await waitFor(() => expect(screen.queryByTestId("traces-table-unchecked")).toBeNull());
    expect(checks()).toBeGreaterThan(before);
  });
});
