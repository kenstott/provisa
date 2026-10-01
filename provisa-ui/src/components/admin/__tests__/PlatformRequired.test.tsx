// Copyright (c) 2026 Kenneth Stott
// Canary: 6d1b8f23-5c47-4e90-a3d6-0b9e2f7c4a18
// REQ-1913: a platform endpoint that answers 403 shows "platform administrator required", never an
// error alert or an empty page.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import "../../../i18n";
import * as api from "../../../api/admin";
import * as secrets from "../../../api/secrets";
import { FederationEngineTab } from "../FederationEngineTab";
import { EncryptionTab } from "../EncryptionTab";
import { AuthTab } from "../AuthTab";
import { HotTablesSettingsPanel } from "../CacheStorageTab";
import { SecretsServicePanel } from "../SecretsTab";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchFederationEngine: vi.fn(),
  fetchEncryption: vi.fn(),
  fetchAuthConfig: vi.fn(),
  fetchCacheStorage: vi.fn(),
}));
vi.mock("../../../api/secrets", async (orig) => ({
  ...(await orig<typeof import("../../../api/secrets")>()),
  fetchSecretsService: vi.fn(),
}));

const forbidden = () => Object.assign(new Error("Settings fetch failed (403)"), { status: 403 });
const failed = () => Object.assign(new Error("boom"), { status: 500 });

const CASES: [string, () => unknown, () => React.ReactElement][] = [
  ["federation engine", () => api.fetchFederationEngine, () => <FederationEngineTab />],
  ["encryption", () => api.fetchEncryption, () => <EncryptionTab />],
  ["auth", () => api.fetchAuthConfig, () => <AuthTab />],
  ["cache storage", () => api.fetchCacheStorage, () => <HotTablesSettingsPanel />],
  ["secrets service", () => secrets.fetchSecretsService, () => <SecretsServicePanel />],
];

function wrap(ui: React.ReactElement) {
  return render(<MantineProvider>{ui}</MantineProvider>);
}

beforeEach(() => {
  vi.mocked(api.fetchFederationEngine).mockReset();
  vi.mocked(api.fetchEncryption).mockReset();
  vi.mocked(api.fetchAuthConfig).mockReset();
  vi.mocked(api.fetchCacheStorage).mockReset();
  vi.mocked(secrets.fetchSecretsService).mockReset();
});

describe("platform-administrator-required state", () => {
  it.each(CASES)("%s shows the state on a 403", async (_name, getFetch, ui) => {
    (getFetch() as unknown as ReturnType<typeof vi.fn>).mockRejectedValue(forbidden());
    wrap(ui());
    expect(await screen.findByTestId("platform-required")).toHaveTextContent(
      /platform administrator/i,
    );
  });

  it.each(CASES)("%s still shows other failures as an error", async (_name, getFetch, ui) => {
    (getFetch() as unknown as ReturnType<typeof vi.fn>).mockRejectedValue(failed());
    wrap(ui());
    expect(await screen.findByText(/boom/)).toBeInTheDocument();
    expect(screen.queryByTestId("platform-required")).toBeNull();
  });
});
