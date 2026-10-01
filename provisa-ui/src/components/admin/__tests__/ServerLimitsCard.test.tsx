// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8d2c64-7e19-4a3f-9c05-d1f6a2e48b73
// REQ-1905: per-transport request timeouts on the admin settings page.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import { ServerLimitsCard } from "../settingsCards";
import * as api from "../../../api/admin";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettings: vi.fn(),
  updateSettings: vi.fn(),
}));

const TRANSPORTS = [
  "graphql",
  "rest",
  "jsonapi",
  "sql_http",
  "cypher_http",
  "pgwire",
  "flight",
  "bolt",
  "grpc",
  "mcp",
] as const;

function settings(
  over: Record<string, unknown> = {},
  timeouts: Record<string, number | null> = {},
) {
  const request_timeouts: Record<string, number | null> = {};
  for (const k of TRANSPORTS) request_timeouts[k] = null;
  request_timeouts.flight = 3600;
  request_timeouts.pgwire = 300;
  return {
    features: { live_config_export: true, platform_settings: true, deployment_settings: true },
    redirect: { enabled: false, threshold: 0, default_format: "csv", ttl: 0 },
    cache: { default_ttl: 0 },
    naming: { use_domains: null, default_domain: "" },
    limits: {
      default_row_limit: 1000,
      request_timeout: 60,
      request_timeouts: { ...request_timeouts, ...timeouts },
      ...over,
    },
  };
}

function renderCard() {
  render(
    <MantineProvider>
      <ServerLimitsCard />
    </MantineProvider>,
  );
}

const fetchMock = api.fetchSettings as unknown as ReturnType<typeof vi.fn>;
const updateMock = api.updateSettings as unknown as ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock.mockReset();
  updateMock.mockReset();
  updateMock.mockResolvedValue({ success: true, updated: ["limits"], restart_required: false });
});

describe("ServerLimitsCard", () => {
  it("shows the default and one labelled row per transport", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    expect(await screen.findByTestId("limit-request-timeout")).toHaveValue("60");
    for (const k of TRANSPORTS)
      expect(screen.getByTestId(`limit-timeout-${k}`)).toBeInTheDocument();
    for (const label of [
      "GraphQL",
      "REST",
      "JSON:API",
      "SQL over HTTP",
      "Cypher over HTTP",
      "PostgreSQL wire",
      "Arrow Flight",
      "Bolt",
      "gRPC",
      "MCP",
    ]) {
      expect(screen.getByText(label)).toBeInTheDocument();
    }
    expect(screen.getByTestId("limit-timeout-flight")).toHaveValue("3600");
    expect(screen.getByTestId("limit-timeout-pgwire")).toHaveValue("300");
    expect(screen.getByTestId("limit-timeout-graphql")).toHaveValue("");
  });

  it("explains why Flight and pgwire ship their own values", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    expect(await screen.findByTestId("limit-timeout-flight-help")).toHaveTextContent(
      "Arrow Flight carries large data transfers",
    );
    expect(screen.getByTestId("limit-timeout-pgwire-help")).toHaveTextContent(
      "PostgreSQL wire carries BI dashboards and ad-hoc SQL",
    );
    expect(screen.queryByTestId("limit-timeout-graphql-help")).toBeNull();
  });

  it("says an empty transport uses the default and shows the effective value", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    const note = await screen.findByTestId("limit-timeout-graphql-effective");
    expect(note).toHaveTextContent("60");
    expect(note).toHaveTextContent(/default/i);
  });

  it("refuses a non-numeric or non-positive value before saving", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    const input = await screen.findByTestId("limit-timeout-rest");
    for (const bad of ["abc", "0", "-5"]) {
      fireEvent.change(input, { target: { value: bad } });
      fireEvent.click(screen.getByRole("button", { name: /save/i }));
      expect(await screen.findByTestId("limit-timeout-rest-error")).toBeInTheDocument();
    }
    expect(updateMock).not.toHaveBeenCalled();
  });

  it("refuses an empty or invalid default", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    const input = await screen.findByTestId("limit-request-timeout");
    fireEvent.change(input, { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(await screen.findByTestId("limit-request-timeout-error")).toBeInTheDocument();
    expect(updateMock).not.toHaveBeenCalled();
  });

  it("saves changed values and sends null for a cleared transport", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    fireEvent.change(await screen.findByTestId("limit-timeout-graphql"), {
      target: { value: "120.5" },
    });
    fireEvent.change(screen.getByTestId("limit-timeout-flight"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(updateMock).toHaveBeenCalledTimes(1));
    const body = updateMock.mock.calls[0][0];
    expect(body.limits.request_timeout).toBe(60);
    expect(body.limits.request_timeouts.graphql).toBe(120.5);
    expect(body.limits.request_timeouts.flight).toBeNull();
    expect(body.limits.request_timeouts.rest).toBeNull();
  });

  it("shows a server 400 on the field it names", async () => {
    fetchMock.mockResolvedValue(settings());
    updateMock.mockRejectedValue(
      Object.assign(new Error("bad value"), {
        code: "invalid_request_timeout",
        params: { field: "limits.request_timeouts.pgwire" },
      }),
    );
    renderCard();
    fireEvent.click(await screen.findByRole("button", { name: /save/i }));
    expect(await screen.findByTestId("limit-timeout-pgwire-error")).toHaveTextContent("bad value");
  });

  it("shows an error state, not an empty form, when request_timeouts is missing", async () => {
    const s = settings();
    delete (s.limits as Record<string, unknown>).request_timeouts;
    fetchMock.mockResolvedValue(s);
    renderCard();
    expect(await screen.findByTestId("limit-settings-error")).toBeInTheDocument();
    expect(screen.queryByTestId("limit-timeout-graphql")).toBeNull();
  });

  it("renders nothing without deployment_settings", async () => {
    const s = settings();
    s.features.deployment_settings = false;
    delete (s as Record<string, unknown>).limits;
    fetchMock.mockResolvedValue(s);
    renderCard();
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(screen.queryByTestId("limit-settings")).toBeNull();
    expect(screen.queryByTestId("limit-settings-error")).toBeNull();
  });

  it("sends null when the Flight or pgwire value is cleared", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    fireEvent.change(await screen.findByTestId("limit-timeout-flight"), { target: { value: "" } });
    fireEvent.change(screen.getByTestId("limit-timeout-pgwire"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(updateMock).toHaveBeenCalledTimes(1));
    const t = updateMock.mock.calls[0][0].limits.request_timeouts;
    expect(t.flight).toBeNull();
    expect(t.pgwire).toBeNull();
  });

  it("edits the default row limit and saves it", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    const input = await screen.findByTestId("limit-default-row-limit");
    expect(input).toHaveValue("1000");
    fireEvent.change(input, { target: { value: "250" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(updateMock).toHaveBeenCalledTimes(1));
    expect(updateMock.mock.calls[0][0].limits.default_row_limit).toBe(250);
  });

  it("refuses a non-integer or non-positive row limit", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    const input = await screen.findByTestId("limit-default-row-limit");
    for (const bad of ["", "abc", "0", "-1", "1.5"]) {
      fireEvent.change(input, { target: { value: bad } });
      fireEvent.click(screen.getByRole("button", { name: /save/i }));
      expect(await screen.findByTestId("limit-default-row-limit-error")).toBeInTheDocument();
    }
    expect(updateMock).not.toHaveBeenCalled();
  });

  it("shows a server 400 on the row limit field", async () => {
    fetchMock.mockResolvedValue(settings());
    updateMock.mockRejectedValue(
      Object.assign(new Error("too big"), {
        code: "invalid_default_row_limit",
        params: { field: "limits.default_row_limit" },
      }),
    );
    renderCard();
    fireEvent.click(await screen.findByRole("button", { name: /save/i }));
    expect(await screen.findByTestId("limit-default-row-limit-error")).toHaveTextContent("too big");
  });

  it("shows the platform-administrator state on a 403, not an error alert", async () => {
    fetchMock.mockRejectedValue(Object.assign(new Error("forbidden"), { status: 403 }));
    renderCard();
    expect(await screen.findByTestId("limit-forbidden")).toBeInTheDocument();
    expect(screen.queryByTestId("limit-settings-error")).toBeNull();
  });

  it("shows the value in force for a cleared row as the shipped value, from the API response", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    fireEvent.change(await screen.findByTestId("limit-timeout-flight"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(updateMock).toHaveBeenCalledTimes(1));
    expect(updateMock.mock.calls[0][0].limits.request_timeouts.flight).toBeNull();
    const note = await screen.findByTestId("limit-timeout-flight-shipped");
    expect(note).toHaveTextContent("3600");
    expect(screen.getByTestId("limit-timeout-flight")).toHaveValue("");
    expect(screen.queryByTestId("limit-timeout-flight-effective")).toBeNull();
  });

  it("keeps the per-transport rows in an expandable panel, in columns, collapsed at first", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    const toggle = await screen.findByTestId("limit-timeouts-toggle");
    expect(toggle).toHaveTextContent("Request timeout per transport");
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    const grid = screen.getByTestId("limit-timeouts-grid");
    for (const k of TRANSPORTS)
      expect(grid).toContainElement(screen.getByTestId(`limit-timeout-${k}`));
    expect(grid.className).toMatch(/SimpleGrid/);
    expect(grid).not.toContainElement(screen.getByTestId("limit-request-timeout"));
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
  });

  it("opens the per-transport panel when one of its rows has an error", async () => {
    fetchMock.mockResolvedValue(settings());
    renderCard();
    const toggle = await screen.findByTestId("limit-timeouts-toggle");
    fireEvent.change(screen.getByTestId("limit-timeout-rest"), { target: { value: "0" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(await screen.findByTestId("limit-timeout-rest-error")).toBeInTheDocument();
    expect(toggle).toHaveAttribute("aria-expanded", "true");
  });
});
