// Copyright (c) 2026 Kenneth Stott
// Canary: 8ec2abc0-8c22-4dc2-a248-90461d48770b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: the discovery dialog's test fixtures and helpers, shared by its test files. Each test
// file mocks the modules with discoverDialog.state and calls resetDiscoveryMocks before each test.

import { expect, vi } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  applyRunDecisions,
  countDiscoveryRun,
  fetchDiscoveryModels,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  fetchModelLimits,
  fetchRunProposals,
  saveDiscoveryThreshold,
  saveDiscoveryScoreFilter,
  fetchDiscoverySettings,
  fetchVendorModels,
  startDiscoveryRun,
  stopDiscoveryRun,
  type DiscoveryCount,
  type DiscoveryModels,
  type DiscoveryRun,
  type DiscoveryScope,
  type RunProposal,
} from "../../api/admin";

import { REGISTERED, TABLES } from "./discoverDialog.state";

export const HAIKU = {
  vendor: "anthropic",
  model: "claude-haiku-4-5",
  input_tokens: 200000,
  output_tokens: 64000,
  limits_source: "known" as const,
};
export const MODELS: DiscoveryModels = {
  assigned: { ...HAIKU, has_key: true },
  vendors: ["anthropic", "openai"],
  semantics_available: true,
};
export const NO_VENDOR: DiscoveryModels = {
  assigned: null,
  vendors: [],
  semantics_available: false,
};
export const COUNT: DiscoveryCount = {
  requests: 3,
  requests_are: "upper_bound",
  by_group: {},
  table_pairs: 40,
  table_pairs_per_request: 27,
  model: HAIKU,
  source_tables: 2,
  target_tables: 2,
  tables_not_read: 0,
};
/** Key value overlap as a person who has set none has it: the defaults. */
export const KEY_VALUES_DEFAULT = {
  chosen: true,
  cutoff: 0.2,
  sample_size: 10000,
  contained_at: 0.95,
  shared_ceiling: 0.6,
};
/** Every run setting as a person who has set none has them: the defaults. */
export const SETTINGS_DEFAULT = {
  threshold: 0.7,
  samples_per_column: 5,
  key_values: KEY_VALUES_DEFAULT,
};
export const SCOPE: DiscoveryScope = {
  kinds: ["foreign_keys", "semantics", "key_values"],
  source_domains: ["shop"],
  source_tables: null,
  target_same_as_source: true,
  target_domains: [],
  target_tables: null,
  vendor: null,
  model: null,
  threshold: 0.7,
  samples_per_column: 5,
  measure_cutoff: 0.2,
  sample_size: 10000,
  contained_at: 0.95,
  shared_ceiling: 0.6,
};
/** The scope with Key value overlap not chosen (or not offered, without Semantics). */
export const UNMEASURED = {
  measure_cutoff: null,
  sample_size: null,
  contained_at: null,
  shared_ceiling: null,
};
export const runOf = (over: Partial<DiscoveryRun>): DiscoveryRun => ({
  id: 7,
  status: "running",
  scope: SCOPE,
  model: "anthropic:claude-haiku-4-5",
  requests_total: 0,
  requests_done: 0,
  suggestions: 0,
  report: null,
  error: null,
  stop_requested: false,
  stopped_by: null,
  started_by: "user-1",
  started_at: "2026-11-15T10:00:00+00:00",
  finished_at: null,
  ...over,
});
export const REPORT = {
  suggestions: 2,
  stored_ids: [101, 102],
  removed: { existing: 2, rejected: 1 },
  foreign_keys: { found: 3, suggested: 1 },
  semantics: {
    suggested: 1,
    empty_reason: null,
    requests_total: 3,
    requests_done: 1,
    split: 0,
    proposed: 2,
    invalid: 0,
    below_threshold: 1,
    threshold: 0.7,
    cut_short: 0,
    unreadable: 0,
    failed: [],
    stopped: false,
  },
  model: HAIKU,
};
export const suggestion = (id: number, column: string, scope: string) => ({
  id,
  source_table_id: 1,
  source_column: column,
  target_table_id: 2,
  target_column: "id",
  cardinality: "many-to-one",
  confidence: 0.9,
  reasoning: "Same customer key",
  suggested_name: `orders-${column}`,
  scope,
});

export const modelsMock = vi.mocked(fetchDiscoveryModels);
export const vendorModelsMock = vi.mocked(fetchVendorModels);
export const limitsMock = vi.mocked(fetchModelLimits);
export const countMock = vi.mocked(countDiscoveryRun);
export const startMock = vi.mocked(startDiscoveryRun);
export const runMock = vi.mocked(fetchDiscoveryRun);
export const runsMock = vi.mocked(fetchDiscoveryRuns);
export const stopMock = vi.mocked(stopDiscoveryRun);
export const proposalsMock = vi.mocked(fetchRunProposals);
export const applyMock = vi.mocked(applyRunDecisions);
export const saveThresholdMock = vi.mocked(saveDiscoveryThreshold);
export const saveScoreFilterMock = vi.mocked(saveDiscoveryScoreFilter);
export const settingsMock = vi.mocked(fetchDiscoverySettings);
/** A proposal as a run lists it. Unless given, its score is the model's (no values compared)
 *  and it can be accepted where it can be decided. */
export const proposal = (over: Partial<RunProposal>): RunProposal => ({
  score: over.confidence === undefined ? 0.95 : over.confidence,
  measured: false,
  measure: null,
  acceptable: over.decidable ?? true,
  id: 501,
  run_id: 7,
  kind: "semantics",
  source_table_id: 1,
  source_column: "customer_id",
  target_table_id: 2,
  target_column: "id",
  cardinality: "many-to-one",
  confidence: 0.95,
  scored: null,
  reasoning: "Each order names its customer",
  suggested_name: "customer-orders",
  reason: null,
  answer: null,
  decidable: true,
  reduced_information: false,
  admitted_on_key_values: false,
  dimension: null,
  via: null,
  ...over,
});
/** A run's table: a constraint, a suggestion, one the model scored high and Jev low, another
 *  suggestion, one under the threshold, one between columns that are not compared, and one that
 *  named no two compared columns. */
export const PROPOSALS: RunProposal[] = [
  proposal({
    id: 500,
    kind: "foreign_keys",
    source_column: "store_id",
    confidence: 1,
    reasoning: "Foreign key constraint",
    suggested_name: "order-store",
  }),
  proposal({}),
  // The model gave it 92% and Jev 31%: a suggestion, the model's score deciding.
  proposal({
    id: 504,
    source_column: "ref",
    confidence: 0.92,
    scored: 0.31,
    suggested_name: "order-ref",
  }),
  proposal({
    id: 502,
    source_column: "payer_id",
    confidence: 0.9,
    suggested_name: "order-payer",
  }),
  proposal({
    id: 503,
    source_column: "agent_id",
    confidence: 0.55,
    reason: "below_threshold",
    reasoning: "Perhaps the agent is a customer",
    suggested_name: "order-agent",
  }),
  proposal({
    id: 505,
    source_column: "store_id",
    confidence: 0.9,
    reason: "not_comparable",
    decidable: false,
  }),
  proposal({
    id: 506,
    source_table_id: null,
    source_column: null,
    target_table_id: null,
    target_column: null,
    cardinality: null,
    confidence: null,
    reasoning: null,
    suggested_name: null,
    reason: "unknown_column",
    answer: '{"source_table_id": 1, "target_table_id": 9}',
    decidable: false,
  }),
];
export const acceptBox = (id: number) =>
  screen.getByTestId(`list-select-${id}`) as HTMLInputElement;
export const notAgainBox = (id: number) =>
  screen.getByTestId(`proposal-not-again-${id}`) as HTMLInputElement;
export const applyButton = () => screen.getByTestId("discover-apply");
export async function endedRun() {
  runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
  proposalsMock.mockResolvedValue({
    proposals: PROPOSALS,
    threshold: 0.7,
    kept_at: 0.7,
    score_filter: null,
  });
  await openDialog();
  fireEvent.click(runButton());
  await screen.findByTestId("proposal-501", {}, SLOW);
}

export const discoverButton = () => screen.getByTestId("rels-discover");
export const dialog = () => screen.getByTestId("discover-dialog");
export const fkBox = () => screen.getByTestId("discover-kind-foreign-keys") as HTMLInputElement;
export const semanticsBox = () => screen.getByTestId("discover-kind-semantics") as HTMLInputElement;
export const runButton = () => screen.getByTestId("discover-run");
export const sameBox = () => screen.getByTestId("discover-same-as-source") as HTMLInputElement;
export const SLOW = { timeout: 4000 };

export async function openDialog() {
  render(<RelationshipsPage />);
  await waitFor(() => expect(runsMock).toHaveBeenCalled());
  fireEvent.click(discoverButton());
  await waitFor(() => expect(modelsMock).toHaveBeenCalled());
  await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
}

// Pick an option from the one select named: every select's options stay in the document, so an
// option is looked for in the list its own textbox controls.
export async function pick(selectName: string, optionName: string) {
  const box = screen.getByRole("textbox", { name: selectName });
  await userEvent.click(box);
  const list = await waitFor(() => {
    const found = document.getElementById(box.getAttribute("aria-controls") ?? "");
    expect(found).not.toBeNull();
    return found as HTMLElement;
  });
  await userEvent.click(within(list).getByRole("option", { name: optionName, hidden: true }));
}

export const lastScope = () => countMock.mock.calls[countMock.mock.calls.length - 1][0];

/** Every test starts from no run, the defaults and nothing remembered. */
export function resetDiscoveryMocks() {
  // A panel opens as this viewer left it: each test starts with nothing remembered.
  window.localStorage.clear();
  TABLES.tables = REGISTERED;
  modelsMock.mockReset().mockResolvedValue(MODELS);
  vendorModelsMock
    .mockReset()
    .mockImplementation(async (vendor) =>
      vendor === "openai" ? ["gpt-4.1", "gpt-4.1-mini"] : ["claude-haiku-4-5", "claude-sonnet-4-5"],
    );
  limitsMock.mockReset().mockImplementation(async (vendor, model) => ({
    vendor,
    model,
    input_tokens: 1047576,
    output_tokens: 32768,
    source: "known",
  }));
  countMock.mockReset().mockResolvedValue(COUNT);
  startMock.mockReset().mockResolvedValue(runOf({}));
  runMock.mockReset().mockResolvedValue(runOf({}));
  runsMock.mockReset().mockResolvedValue([]);
  stopMock.mockReset().mockResolvedValue(runOf({ stop_requested: true }));
  proposalsMock
    .mockReset()
    .mockResolvedValue({ proposals: [], threshold: 0.7, kept_at: 0.7, score_filter: null });
  applyMock.mockReset().mockResolvedValue([]);
  saveThresholdMock.mockReset().mockImplementation(async (threshold) => ({ threshold }));
  saveScoreFilterMock.mockReset().mockImplementation(async (filter) => ({ filter }));
  settingsMock.mockReset().mockResolvedValue({ ...SETTINGS_DEFAULT, defaults: SETTINGS_DEFAULT });
}

export const LAST: DiscoveryScope = {
  kinds: ["semantics"],
  source_domains: ["crm"],
  source_tables: [3],
  target_same_as_source: false,
  target_domains: ["shop"],
  target_tables: [2],
  vendor: "openai",
  model: "gpt-4.1-mini",
  threshold: 0.7,
  samples_per_column: 5,
  // That run did not choose Key value overlap.
  measure_cutoff: null,
  sample_size: null,
  contained_at: null,
  shared_ceiling: null,
};
