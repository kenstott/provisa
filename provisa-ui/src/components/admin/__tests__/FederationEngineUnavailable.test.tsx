// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1f0b8e-3c7a-4e2b-9a61-2f4d8c0e7b13
// REQ-947: an engine whose driver library the install lacks is listed as unavailable, naming what
// failed to load, and cannot be selected; the other engines are offered as before.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import * as api from "../../../api/admin";
import { FederationEngineTab } from "../FederationEngineTab";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchFederationEngine: vi.fn(),
  setFederationEngine: vi.fn(),
}));

vi.mock("../settingsCards", () => ({ FederationSettingsCards: () => null }));

const MISSING = "libodbc.so.2: cannot open shared object file: No such file or directory";

const STATE: api.FederationEngineState = {
  current: "duckdb",
  persisted: "duckdb",
  env_pinned_engine: null,
  config: {},
  secret_set: {},
  engines: [
    {
      key: "duckdb",
      label: "DuckDB",
      description: "In-process.",
      config_fields: [],
      unavailable: null,
    },
    {
      key: "fabric",
      label: "Microsoft Fabric",
      description: "T-SQL warehouse.",
      config_fields: [],
      unavailable: MISSING,
    },
  ],
  restart_required_note: "restart",
};

beforeEach(() => {
  vi.mocked(api.fetchFederationEngine).mockReset().mockResolvedValue(STATE);
});

describe("FederationEngineTab unavailable engine", () => {
  it("names the missing library and refuses the selection", async () => {
    const user = userEvent.setup();
    render(
      <MantineProvider>
        <FederationEngineTab />
      </MantineProvider>,
    );

    await user.click(await screen.findByTestId("federation-engine-select"));
    const fabric = await screen.findByRole("option", {
      name: `Microsoft Fabric (unavailable: ${MISSING})`,
      hidden: true,
    });
    expect(fabric).toHaveAttribute("data-combobox-disabled", "true");
    const duckdb = screen.getByRole("option", { name: /^DuckDB/, hidden: true });
    expect(duckdb).not.toHaveAttribute("data-combobox-disabled");
  });
});
