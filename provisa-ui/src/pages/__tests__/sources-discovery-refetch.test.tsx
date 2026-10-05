// Copyright (c) 2026 Kenneth Stott
// Canary: 0616eb2c-df54-4bdd-ae4d-3d90e9676399
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-150: the Sources page re-reads its list and settings after a change (a source saved, a
// table registered). That refetch must not unmount an open Schema Discovery panel. Before, the
// page swapped itself for the full-page loader on every refetch, so the panel remounted empty: a
// topic just typed, or columns just discovered, were gone. Seen in the Kafka sampling e2e, whose
// discovery opened while the new source's save was still finishing.

import { describe, it, expect, vi } from "vitest";
import { useEffect, useState } from "react";
import { render, screen, waitFor } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { Source } from "../../types/admin";

const SETTINGS = {
  redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
  sampling: { default_sample_size: 1000 },
  cache: { default_ttl: 300 },
  naming: { domain_prefix: false, convention: "none" },
};

// The second settings read (the refetch) is held open, so the test sees the page mid-refetch.
let releaseRefetch: () => void = () => {};
const fetchSettings = vi
  .fn()
  .mockResolvedValueOnce(SETTINGS)
  .mockImplementation(
    () =>
      new Promise((resolve) => {
        releaseRefetch = () => resolve(SETTINGS);
      }),
  );

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchSettings: () => fetchSettings(),
  fetchFederationEngine: vi.fn().mockResolvedValue(null),
}));

vi.mock("../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../context/AuthContext")>()),
  useAuth: () => ({
    role: "admin",
    selectedRoles: ["admin"],
    capabilities: ["admin"],
    domainAccess: ["*"],
  }),
}));

const SOURCES = [
  {
    id: "orders-kafka",
    type: "kafka",
    host: "broker",
    port: 9092,
    database: "",
    username: "",
    cacheEnabled: true,
    cacheTtl: null,
  } as unknown as Source,
];

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => ({ sources: SOURCES, loading: false, refetch: vi.fn().mockResolvedValue({}) }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));

// The discovery panel as the page sees it: state of its own (a typed topic) and the page's
// onRegistered refetch. Mounts are counted to catch a remount.
const mounts = { count: 0 };
vi.mock("../../components/SchemaDiscovery", () => ({
  SchemaDiscovery: (props: { sourceId: string; onRegistered: () => void }) => {
    const [topic, setTopic] = useState("");
    useEffect(() => {
      mounts.count += 1;
    }, []);
    return (
      <div data-testid="discovery-stub">
        <input aria-label="Topic" value={topic} onChange={(e) => setTopic(e.target.value)} />
        <button onClick={props.onRegistered}>registered</button>
      </div>
    );
  },
}));

import { SourcesPage } from "../SourcesPage";

describe("Sources page refetch with Schema Discovery open", () => {
  it("keeps the open panel mounted, with what was typed in it, across the refetch", async () => {
    render(<SourcesPage />);
    await userEvent.click(await screen.findByTestId("sources-discover-orders-kafka"));
    await userEvent.type(screen.getByLabelText("Topic"), "orders");
    expect(mounts.count).toBe(1);

    await userEvent.click(screen.getByText("registered")); // the page refetches its list
    // Mid-refetch: the panel is still there, not replaced by the page loader.
    expect(screen.getByTestId("discovery-stub")).toBeInTheDocument();
    expect(screen.getByLabelText("Topic")).toHaveValue("orders");

    releaseRefetch();
    await waitFor(() => expect(fetchSettings).toHaveBeenCalledTimes(2));
    expect(screen.getByLabelText("Topic")).toHaveValue("orders");
    expect(mounts.count).toBe(1);
  });
});
