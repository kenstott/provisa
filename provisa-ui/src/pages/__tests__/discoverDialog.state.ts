// Copyright (c) 2026 Kenneth Stott
// Canary: 220f3f06-1a1b-4ce5-8829-c5349a8bebe7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: the state the discovery dialog's tests stand in for, and the modules they mock with it.
// Kept apart from the helpers, which import the page: a mock's factory imports this module.

import { vi } from "vitest";

export const RELS = { relationships: [], loading: false, refetch: vi.fn() };
// Registered before the runs these tests start from (2026-11-15T10:00) unless said otherwise.
export const BEFORE = "2026-11-01T00:00:00+00:00";
export const SINCE = "2026-11-16T09:00:00+00:00";
export const table = (id: number, domainId: string, tableName: string, registeredAt = BEFORE) => ({
  id,
  domainId,
  sourceId: `${domainId}-db`,
  schemaName: domainId,
  tableName,
  alias: null,
  registeredAt,
  columns: [],
});
export const REGISTERED = [
  table(1, "shop", "orders"),
  table(2, "shop", "customers"),
  table(3, "crm", "accounts"),
  table(5, "crm", "contacts"),
  table(4, "meta", "registered_tables"),
];
export const TABLES = { tables: REGISTERED, loading: false, refetch: () => Promise.resolve() };
export const AUTH = { capabilities: ["create_relationship"] };
// The Domains picker has shop, crm and files ticked and meta not: the dialog offers only those
// (REQ-2253) and its source domains start as those of them holding a table.
export const CHECKED = ["shop", "crm", "files"];
export const FILTER = {
  domainsEnabled: true,
  domains: ["shop", "crm", "files", "meta"],
  checkedDomains: new Set<string>(CHECKED),
};

/** useAdminQueries, answering from the state above. */
export const adminQueries = () => ({
  // REQ-2249: no code resolver set is in service here.
  useCodeResolvers: () => ({
    codeResolvers: [],
    loading: false,
    saveCodeResolver: vi.fn(),
    retireCodeResolver: vi.fn(),
  }),
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => ({ domains: [] }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useDeleteRelationship: () => ({ deleteRelationship: vi.fn() }),
  useDeleteRelationships: () => vi.fn(),
});

/** The admin API, with discovery's calls stood in for. */
export const adminApi = (original: typeof import("../../api/admin")) => ({
  ...original,
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn().mockResolvedValue({
    naming: { convention: "apollo_graphql", use_domains: false, default_domain: "" },
  }),
  fetchDiscoveryModels: vi.fn(),
  fetchVendorModels: vi.fn(),
  fetchModelLimits: vi.fn(),
  countDiscoveryRun: vi.fn(),
  startDiscoveryRun: vi.fn(),
  fetchDiscoveryRun: vi.fn(),
  fetchDiscoveryRuns: vi.fn(),
  stopDiscoveryRun: vi.fn(),
  fetchRunProposals: vi.fn(),
  applyRunDecisions: vi.fn(),
  fetchDiscoverySettings: vi.fn(),
  saveDiscoveryThreshold: vi.fn(),
  saveDiscoveryScoreFilter: vi.fn(),
});
