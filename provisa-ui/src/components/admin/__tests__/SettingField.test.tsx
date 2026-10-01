// Copyright (c) 2026 Kenneth Stott
// Canary: 9c3e7a15-4d82-4b60-a1f9-6e0b5d2c8f47
// REQ-1913: catalog-driven settings field, card, pending-restart banner and panel.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import {
  SettingField,
  CatalogCard,
  PendingRestartBanner,
  SettingsCatalogPanel,
} from "../SettingField";
import type { CatalogSetting, SettingsCatalog } from "../../../api/admin";
import * as api from "../../../api/admin";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettingsCatalog: vi.fn(),
  updateSettingsCatalog: vi.fn(),
}));

function mk(over: Partial<CatalogSetting> & { key: string }): CatalogSetting {
  return {
    type: "int",
    value: 120,
    source: "stored",
    default: 120,
    stored: 120,
    env: null,
    config_key: null,
    restart_required: false,
    pending_restart: false,
    secret: false,
    editable: true,
    guard: null,
    updated_by: null,
    updated_at: null,
    min: 1,
    max: 3600,
    ...over,
  } as CatalogSetting;
}

const num = mk({ key: "limits.engine_query_timeout" });
const id = (k: string) => `setting-${k}`;

function wrap(ui: React.ReactElement) {
  return render(<MantineProvider>{ui}</MantineProvider>);
}
const noop = () => {};

describe("SettingField", () => {
  it("edits a number draft", () => {
    const onChange = vi.fn();
    wrap(
      <SettingField
        setting={num}
        draft="120"
        error=""
        cleared={false}
        onChange={onChange}
        onClear={noop}
      />,
    );
    const input = screen.getByTestId(id(num.key));
    expect(input).toHaveValue("120");
    fireEvent.change(input, { target: { value: "30" } });
    expect(onChange).toHaveBeenCalledWith("30");
  });

  it("shows the restart-required badge only for restart settings", () => {
    const { rerender } = wrap(
      <SettingField
        setting={num}
        draft="120"
        error=""
        cleared={false}
        onChange={noop}
        onClear={noop}
      />,
    );
    expect(screen.queryByTestId(`${id(num.key)}-restart`)).toBeNull();
    rerender(
      <MantineProvider>
        <SettingField
          setting={{ ...num, restart_required: true }}
          draft="120"
          error=""
          cleared={false}
          onChange={noop}
          onClear={noop}
        />
      </MantineProvider>,
    );
    expect(screen.getByTestId(`${id(num.key)}-restart`)).toBeInTheDocument();
  });

  it("names the source unless it is the stored value", () => {
    const { rerender } = wrap(
      <SettingField
        setting={num}
        draft="120"
        error=""
        cleared={false}
        onChange={noop}
        onClear={noop}
      />,
    );
    expect(screen.queryByTestId(`${id(num.key)}-source`)).toBeNull();
    rerender(
      <MantineProvider>
        <SettingField
          setting={{ ...num, source: "env", stored: null }}
          draft="120"
          error=""
          cleared={false}
          onChange={noop}
          onClear={noop}
        />
      </MantineProvider>,
    );
    expect(screen.getByTestId(`${id(num.key)}-source`)).toHaveTextContent(/environment/i);
  });

  it("renders a boolean as a checkbox", () => {
    const onChange = vi.fn();
    wrap(
      <SettingField
        setting={mk({ key: "relationships.auto_track_fk", type: "bool", value: true })}
        draft="true"
        error=""
        cleared={false}
        onChange={onChange}
        onClear={noop}
      />,
    );
    const box = screen.getByTestId(id("relationships.auto_track_fk"));
    expect(box).toBeChecked();
    fireEvent.click(box);
    expect(onChange).toHaveBeenCalledWith("false");
  });

  it("renders an enum as a select of its choices", () => {
    wrap(
      <SettingField
        setting={mk({
          key: "security.mode",
          type: "enum",
          choices: ["standard", "high"],
          value: "standard",
        })}
        draft="standard"
        error=""
        cleared={false}
        onChange={noop}
        onClear={noop}
      />,
    );
    expect(screen.getByTestId(id("security.mode"))).toHaveValue("standard");
  });

  it("shows a secret as set or not set and never echoes a value", () => {
    wrap(
      <SettingField
        setting={mk({
          key: "redis.password",
          type: "secret",
          secret: true,
          set: true,
          value: undefined,
          stored: undefined,
        })}
        draft=""
        error=""
        cleared={false}
        onChange={noop}
        onClear={noop}
      />,
    );
    expect(screen.getByTestId(`${id("redis.password")}-secret-state`)).toHaveTextContent(/set/i);
    expect(screen.getByTestId(id("redis.password"))).toHaveValue("");
    expect(screen.getByTestId(id("redis.password"))).toHaveAttribute("type", "password");
  });

  it("shows a non-editable setting read-only with its reason", () => {
    wrap(
      <SettingField
        setting={mk({
          key: "network.port",
          editable: false,
          readonly_reason: "locates_control_plane",
          value: 8000,
        })}
        draft="8000"
        error=""
        cleared={false}
        onChange={noop}
        onClear={noop}
      />,
    );
    expect(screen.getByTestId(id("network.port"))).toBeDisabled();
    expect(screen.getByTestId(`${id("network.port")}-reason`)).toHaveTextContent(/control plane/i);
  });

  it("shows an inline error and offers reset only when a value is stored", () => {
    const onClear = vi.fn();
    wrap(
      <SettingField
        setting={num}
        draft="0"
        error="Must be at least 1"
        cleared={false}
        onChange={noop}
        onClear={onClear}
      />,
    );
    expect(screen.getByTestId(`${id(num.key)}-error`)).toHaveTextContent("Must be at least 1");
    fireEvent.click(screen.getByTestId(`${id(num.key)}-reset`));
    expect(onClear).toHaveBeenCalled();
  });

  it("offers no reset when nothing is stored", () => {
    wrap(
      <SettingField
        setting={{ ...num, stored: null, source: "default" }}
        draft="120"
        error=""
        cleared={false}
        onChange={noop}
        onClear={noop}
      />,
    );
    expect(screen.queryByTestId(`${id(num.key)}-reset`)).toBeNull();
  });

  it("shows the running value for a restart setting whose stored value differs", () => {
    wrap(
      <SettingField
        setting={mk({
          key: "server.workers",
          restart_required: true,
          value: 2,
          stored: 4,
          pending_restart: true,
        })}
        draft="4"
        error=""
        cleared={false}
        onChange={noop}
        onClear={noop}
      />,
    );
    expect(screen.getByTestId(`${id("server.workers")}-running`)).toHaveTextContent("2");
  });
});

describe("CatalogCard", () => {
  it("refuses bad numbers before calling save", async () => {
    const onSave = vi.fn();
    wrap(<CatalogCard title="Limits" settings={[num]} onSave={onSave} />);
    const input = screen.getByTestId(id(num.key));
    for (const bad of ["abc", "0", "5000", "1.5"]) {
      fireEvent.change(input, { target: { value: bad } });
      fireEvent.click(screen.getByRole("button", { name: /save/i }));
      expect(await screen.findByTestId(`${id(num.key)}-error`)).toBeInTheDocument();
    }
    expect(onSave).not.toHaveBeenCalled();
  });

  it("saves only changed settings, typed, with no confirmations", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    const other = mk({ key: "limits.retry_budget_secs", type: "float", value: 30, stored: 30 });
    wrap(<CatalogCard title="Limits" settings={[num, other]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id(num.key)), { target: { value: "45" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(onSave).toHaveBeenCalledWith({ "limits.engine_query_timeout": 45 }, []);
  });

  it("sends null for a reset setting", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    wrap(<CatalogCard title="Limits" settings={[num]} onSave={onSave} />);
    fireEvent.click(screen.getByTestId(`${id(num.key)}-reset`));
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() =>
      expect(onSave).toHaveBeenCalledWith({ "limits.engine_query_timeout": null }, []),
    );
  });

  it("parses list and map drafts", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    const list = mk({
      key: "udf.allowlist",
      type: "list",
      value: ["a"],
      stored: ["a"],
      min: undefined,
      max: undefined,
    });
    const map = mk({
      key: "x.map",
      type: "map",
      value: {},
      stored: {},
      min: undefined,
      max: undefined,
    });
    wrap(<CatalogCard title="C" settings={[list, map]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id("udf.allowlist")), { target: { value: "a, b ,, c" } });
    fireEvent.change(screen.getByTestId(id("x.map")), { target: { value: '{"k":1}' } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(onSave).toHaveBeenCalledWith(
      { "udf.allowlist": ["a", "b", "c"], "x.map": { k: 1 } },
      [],
    );
  });

  it("refuses an invalid map document", async () => {
    const onSave = vi.fn();
    const map = mk({
      key: "x.map",
      type: "map",
      value: {},
      stored: {},
      min: undefined,
      max: undefined,
    });
    wrap(<CatalogCard title="C" settings={[map]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id("x.map")), { target: { value: "{nope" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(await screen.findByTestId(`${id("x.map")}-error`)).toBeInTheDocument();
    expect(onSave).not.toHaveBeenCalled();
  });

  it("sends a secret only when a new value was typed", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    const secret = mk({
      key: "redis.password",
      type: "secret",
      secret: true,
      set: false,
      value: undefined,
      stored: undefined,
      min: undefined,
      max: undefined,
    });
    wrap(<CatalogCard title="C" settings={[secret]} onSave={onSave} />);
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(onSave).toHaveBeenCalledWith({}, []));
    fireEvent.change(screen.getByTestId(id("redis.password")), { target: { value: "s3cret" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() =>
      expect(onSave).toHaveBeenLastCalledWith({ "redis.password": "s3cret" }, []),
    );
  });

  it("asks for confirmation on a guarded setting and sends it in confirm", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    const guarded = mk({
      key: "network.port",
      guard: "confirm",
      value: 8000,
      stored: 8000,
      min: 1,
      max: 65535,
    });
    wrap(<CatalogCard title="Network" settings={[guarded]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id("network.port")), { target: { value: "9000" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    const dialog = await screen.findByRole("dialog");
    expect(onSave).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole("button", { name: /confirm/i }));
    await waitFor(() =>
      expect(onSave).toHaveBeenCalledWith({ "network.port": 9000 }, ["network.port"]),
    );
  });

  it("shows a server reason on the field it names", async () => {
    const onSave = vi.fn().mockRejectedValue(
      Object.assign(new Error("x"), {
        code: "settings.invalid_value",
        params: { field: "limits.engine_query_timeout", reason: "above_max", max: 3600 },
      }),
    );
    wrap(<CatalogCard title="Limits" settings={[num]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id(num.key)), { target: { value: "45" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(await screen.findByTestId(`${id(num.key)}-error`)).toHaveTextContent("3600");
  });

  it("shows would_lock_out on the field", async () => {
    const onSave = vi.fn().mockRejectedValue(
      Object.assign(new Error("x"), {
        code: "settings.invalid_value",
        params: { field: "limits.engine_query_timeout", reason: "would_lock_out" },
      }),
    );
    wrap(<CatalogCard title="Limits" settings={[num]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id(num.key)), { target: { value: "45" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(await screen.findByTestId(`${id(num.key)}-error`)).toHaveTextContent(/lock/i);
  });
});

describe("map settings with map_keys", () => {
  const timeouts = mk({
    key: "limits.request_timeouts",
    type: "map",
    map_keys: ["graphql", "flight", "pgwire"],
    value: { graphql: 60, flight: 3600, pgwire: 300 },
    stored: { flight: 3600 },
    sources: { graphql: "default", flight: "stored", pgwire: "default" },
    min: 1,
    max: undefined,
  });
  const row = (k: string) => `setting-limits.request_timeouts.${k}`;

  it("renders one row per map key with the effective value and its source", () => {
    wrap(<CatalogCard title="Limits" settings={[timeouts]} onSave={vi.fn()} />);
    expect(screen.getByTestId(row("graphql"))).toHaveValue("");
    expect(screen.getByTestId(row("graphql"))).toHaveAttribute("placeholder", "60");
    expect(screen.getByTestId(row("flight"))).toHaveValue("3600");
    expect(screen.getByTestId(`${row("graphql")}-source`)).toHaveTextContent(/default/i);
    expect(screen.queryByTestId(`${row("flight")}-source`)).toBeNull();
  });

  it("sends a partial map: changed rows set, a cleared stored row as null, untouched rows not sent", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    const withTwo = {
      ...timeouts,
      stored: { flight: 3600, pgwire: 300 },
    };
    wrap(<CatalogCard title="Limits" settings={[withTwo]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(row("graphql")), { target: { value: "120" } });
    fireEvent.change(screen.getByTestId(row("flight")), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(onSave).toHaveBeenCalledWith(
      { "limits.request_timeouts": { graphql: 120, flight: null } },
      [],
    );
  });

  it("clearing an unset row sends nothing for it", async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    wrap(<CatalogCard title="Limits" settings={[timeouts]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(row("graphql")), { target: { value: "7" } });
    fireEvent.change(screen.getByTestId(row("graphql")), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() => expect(onSave).toHaveBeenCalledWith({}, []));
  });

  it("validates each entry against min", async () => {
    const onSave = vi.fn();
    wrap(<CatalogCard title="Limits" settings={[timeouts]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(row("pgwire")), { target: { value: "0" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(await screen.findByTestId(`${row("pgwire")}-error`)).toBeInTheDocument();
    expect(onSave).not.toHaveBeenCalled();
  });

  it("puts a server error for <key>.<map_key> on that row", async () => {
    const onSave = vi.fn().mockRejectedValue(
      Object.assign(new Error("x"), {
        params: { field: "limits.request_timeouts.pgwire", reason: "below_min", min: 1 },
      }),
    );
    wrap(<CatalogCard title="Limits" settings={[timeouts]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(row("pgwire")), { target: { value: "5" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    expect(await screen.findByTestId(`${row("pgwire")}-error`)).toHaveTextContent("1");
  });
});

describe("new server reasons", () => {
  it.each([
    "not_an_integer",
    "not_a_boolean",
    "not_a_string",
    "not_a_list",
    "not_a_map",
    "empty",
    "unknown_key",
    "not_editable",
  ])("has a text for %s", async (reason) => {
    const onSave = vi.fn().mockRejectedValue(
      Object.assign(new Error("raw server text"), {
        params: { field: "limits.engine_query_timeout", reason },
      }),
    );
    wrap(<CatalogCard title="Limits" settings={[num]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id(num.key)), { target: { value: "45" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    const err = await screen.findByTestId(`${id(num.key)}-error`);
    expect(err.textContent).not.toMatch(/adminPage\.setting\.reason/);
    expect(err.textContent).not.toBe("raw server text");
  });
});

describe("unusable current value", () => {
  it("shows the setting's own error on the field", () => {
    wrap(
      <CatalogCard
        title="Limits"
        settings={[
          mk({
            key: "limits.default_row_limit",
            value: null,
            stored: "abc",
            error: { field: "limits.default_row_limit", source: "stored", reason: "not_a_number" },
          }),
        ]}
        onSave={vi.fn()}
      />,
    );
    expect(screen.getByTestId(id("limits.default_row_limit"))).toHaveValue("abc");
    expect(screen.getByTestId(`${id("limits.default_row_limit")}-error`)).toBeInTheDocument();
  });

  it("has a text for not_encrypted", async () => {
    const onSave = vi.fn().mockRejectedValue(
      Object.assign(new Error("raw server text"), {
        params: { field: "limits.engine_query_timeout", reason: "not_encrypted" },
      }),
    );
    wrap(<CatalogCard title="Limits" settings={[num]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id(num.key)), { target: { value: "45" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    const err = await screen.findByTestId(`${id(num.key)}-error`);
    expect(err.textContent).not.toMatch(/adminPage\.setting\.reason/);
    expect(err.textContent).not.toBe("raw server text");
  });
});

describe("new PUT reasons", () => {
  it.each([
    "port_in_use",
    "file_not_found",
    "invalid_tls_pair",
    "redis_tls_required",
    "cannot_decrypt",
  ])("has a text for %s and shows the other listener", async (reason) => {
    const onSave = vi.fn().mockRejectedValue(
      Object.assign(new Error("raw server text"), {
        params: { field: "limits.engine_query_timeout", reason, other: "server.grpc_port" },
      }),
    );
    wrap(<CatalogCard title="Limits" settings={[num]} onSave={onSave} />);
    fireEvent.change(screen.getByTestId(id(num.key)), { target: { value: "45" } });
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    const err = await screen.findByTestId(`${id(num.key)}-error`);
    expect(err.textContent).not.toMatch(/adminPage\.setting\.reason/);
    expect(err.textContent).not.toBe("raw server text");
    if (reason === "port_in_use" || reason === "invalid_tls_pair") {
      expect(err.textContent).toContain("server.grpc_port");
    }
  });
});

describe("PendingRestartBanner", () => {
  it("lists the waiting settings and renders nothing when none wait", () => {
    const { rerender } = wrap(
      <PendingRestartBanner keys={["server.workers", "network.tls_cert"]} />,
    );
    const banner = screen.getByTestId("pending-restart-banner");
    expect(banner).toHaveTextContent("server.workers");
    expect(banner).toHaveTextContent("network.tls_cert");
    rerender(
      <MantineProvider>
        <PendingRestartBanner keys={[]} />
      </MantineProvider>,
    );
    expect(screen.queryByTestId("pending-restart-banner")).toBeNull();
  });
});

describe("SettingsCatalogPanel", () => {
  const fetchMock = api.fetchSettingsCatalog as unknown as ReturnType<typeof vi.fn>;
  const updateMock = api.updateSettingsCatalog as unknown as ReturnType<typeof vi.fn>;
  const catalog = (pending: string[] = []): SettingsCatalog => ({
    snapshot_ttl_seconds: 5,
    pending_restart: pending,
    cards: [
      { id: "limits", settings: [num] },
      {
        id: "concurrency",
        settings: [mk({ key: "server.workers", restart_required: true, value: 2, stored: 2 })],
      },
    ],
  });
  beforeEach(() => {
    fetchMock.mockReset();
    updateMock.mockReset();
  });

  it("renders one card per catalog card plus the pending banner", async () => {
    fetchMock.mockResolvedValue(catalog(["server.workers"]));
    wrap(<SettingsCatalogPanel />);
    expect(await screen.findByTestId("catalog-card-limits")).toBeInTheDocument();
    expect(screen.getByTestId("catalog-card-concurrency")).toBeInTheDocument();
    expect(screen.getByTestId("pending-restart-banner")).toHaveTextContent("server.workers");
  });

  it("saves through the catalog API and re-reads it", async () => {
    fetchMock.mockResolvedValueOnce(catalog()).mockResolvedValueOnce(catalog(["server.workers"]));
    updateMock.mockResolvedValue({
      updated: ["server.workers"],
      pending_restart: ["server.workers"],
    });
    wrap(<SettingsCatalogPanel />);
    const input = await screen.findByTestId(id("server.workers"));
    fireEvent.change(input, { target: { value: "4" } });
    fireEvent.click(
      within(screen.getByTestId("catalog-card-concurrency")).getByRole("button", { name: /save/i }),
    );
    await waitFor(() => expect(updateMock).toHaveBeenCalledWith({ "server.workers": 4 }, []));
    expect(await screen.findByTestId("pending-restart-banner")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("shows a platform-administrator state on 403, not an empty page", async () => {
    fetchMock.mockRejectedValue(Object.assign(new Error("forbidden"), { status: 403 }));
    wrap(<SettingsCatalogPanel />);
    expect(await screen.findByTestId("catalog-forbidden")).toBeInTheDocument();
    expect(screen.queryByTestId("catalog-card-limits")).toBeNull();
  });

  it("shows other load failures as an error", async () => {
    fetchMock.mockRejectedValue(new Error("boom"));
    wrap(<SettingsCatalogPanel />);
    expect(await screen.findByTestId("catalog-error")).toHaveTextContent("boom");
  });
});
