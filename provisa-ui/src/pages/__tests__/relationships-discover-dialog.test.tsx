// Copyright (c) 2026 Kenneth Stott
// Canary: 5c8e1a47-2f93-4b6d-a0e5-8d3f7b2c9e16
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: relationship discovery asks before it runs, runs only the kinds ticked, animates while
// it runs, and says what each kind found.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  discoverRelationships,
  fetchCandidates,
  fetchDiscoveryKinds,
  type DiscoveryKinds,
  type DiscoveryResult,
} from "../../api/admin";

const RELS = { relationships: [], loading: false, refetch: vi.fn() };
const TABLES = {
  tables: [
    {
      id: 1,
      domainId: "shop",
      sourceId: "shop-db",
      schemaName: "shop",
      tableName: "orders",
      alias: null,
      columns: [],
    },
    {
      id: 2,
      domainId: "shop",
      sourceId: "shop-db",
      schemaName: "shop",
      tableName: "customers",
      alias: null,
      columns: [],
    },
  ],
  loading: false,
};
const AUTH = { capabilities: ["create_relationship"] };
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };

vi.mock("../../hooks/useAdminQueries", () => ({
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => ({ domains: [] }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useDeleteRelationship: () => ({ deleteRelationship: vi.fn() }),
  useDeleteRelationships: () => vi.fn(),
}));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchCandidates: vi.fn(),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn().mockResolvedValue({
    naming: { convention: "apollo_graphql", use_domains: false, default_domain: "" },
  }),
  fetchDiscoveryKinds: vi.fn(),
  discoverRelationships: vi.fn(),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const AVAILABLE: DiscoveryKinds = {
  foreign_keys: { available: true },
  semantics: { available: true, model: "openai:gpt-test", reason: null },
};
const NO_KEY: DiscoveryKinds = {
  foreign_keys: { available: true },
  semantics: { available: false, model: "openai:gpt-test", reason: "no_key" },
};
const NOTHING_FOUND: DiscoveryResult = { candidates_found: 0, stored_ids: [] };

const kindsMock = vi.mocked(fetchDiscoveryKinds);
const discoverMock = vi.mocked(discoverRelationships);
const candidatesMock = vi.mocked(fetchCandidates);

const discoverButton = () => screen.getByTestId("rels-discover");
const dialog = () => screen.getByTestId("discover-dialog");
const fkBox = () => screen.getByTestId("discover-kind-foreign-keys") as HTMLInputElement;
const semanticsBox = () => screen.getByTestId("discover-kind-semantics") as HTMLInputElement;
const runButton = () => screen.getByTestId("discover-run");

async function openDialog() {
  render(<RelationshipsPage />);
  fireEvent.click(discoverButton());
  await waitFor(() => expect(kindsMock).toHaveBeenCalled());
  await waitFor(() => expect(runButton()).toBeEnabled());
}

beforeEach(() => {
  kindsMock.mockReset().mockResolvedValue(AVAILABLE);
  discoverMock.mockReset().mockResolvedValue(NOTHING_FOUND);
  candidatesMock.mockReset().mockResolvedValue([]);
});

describe("RelationshipsPage: discovery asks before it runs", () => {
  it("opens a dialog with Foreign keys and Semantics both ticked, and runs nothing yet", async () => {
    await openDialog();
    expect(within(dialog()).getByText("Discover relationships")).toBeInTheDocument();
    expect(fkBox()).toBeChecked();
    expect(fkBox()).toBeEnabled();
    expect(semanticsBox()).toBeChecked();
    expect(semanticsBox()).toBeEnabled();
    expect(screen.getByLabelText("Foreign keys")).toBe(fkBox());
    expect(screen.getByLabelText("Semantics")).toBe(semanticsBox());
    expect(discoverMock).not.toHaveBeenCalled();
  });

  it("greys out Semantics, unticked, with the reason, when its model has no key", async () => {
    kindsMock.mockResolvedValue(NO_KEY);
    render(<RelationshipsPage />);
    fireEvent.click(discoverButton());
    await waitFor(() =>
      expect(
        screen.getByLabelText(
          "Semantics: not available. openai:gpt-test, the model assigned to relationship " +
            "inference, has no key. Set one under Admin, AI Models.",
        ),
      ).toBe(semanticsBox()),
    );
    expect(semanticsBox()).toBeDisabled();
    expect(semanticsBox()).not.toBeChecked();
    expect(fkBox()).toBeChecked();
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(discoverMock).toHaveBeenCalledWith("cross-domain", ["foreign_keys"]),
    );
  });

  it("Cancel closes the dialog and runs nothing", async () => {
    await openDialog();
    fireEvent.click(screen.getByTestId("discover-cancel"));
    await waitFor(() => expect(screen.queryByTestId("discover-run")).not.toBeInTheDocument());
    expect(discoverMock).not.toHaveBeenCalled();
  });

  it("Run sends the kinds ticked", async () => {
    await openDialog();
    fireEvent.click(fkBox());
    expect(fkBox()).not.toBeChecked();
    fireEvent.click(runButton());
    await waitFor(() => expect(discoverMock).toHaveBeenCalledWith("cross-domain", ["semantics"]));
    expect(discoverMock).toHaveBeenCalledTimes(1);
  });

  it("Run is refused with nothing ticked", async () => {
    await openDialog();
    fireEvent.click(fkBox());
    fireEvent.click(semanticsBox());
    expect(runButton()).toBeDisabled();
  });

  it("animates the button and disables it while a run is in progress", async () => {
    let finish: (r: DiscoveryResult) => void = () => {};
    discoverMock.mockImplementation(() => new Promise((resolve) => (finish = resolve)));
    await openDialog();
    expect(discoverButton()).not.toHaveAttribute("data-loading");
    fireEvent.click(runButton());
    await waitFor(() => expect(discoverButton()).toHaveAttribute("data-loading", "true"));
    expect(discoverButton()).toBeDisabled();
    finish(NOTHING_FOUND);
    await waitFor(() => expect(discoverButton()).not.toHaveAttribute("data-loading"));
    expect(discoverButton()).toBeEnabled();
  });
});

describe("RelationshipsPage: discovery says what it found", () => {
  async function runWith(result: DiscoveryResult) {
    discoverMock.mockResolvedValue(result);
    await openDialog();
    fireEvent.click(runButton());
    return waitFor(() => screen.getByTestId("rels-discover-result"));
  }

  const semantics = {
    suggested: 0,
    proposed: 0,
    below_threshold: 0,
    threshold: 0.7,
    already_known: 0,
    invalid: 0,
    empty_reason: null,
    cut_short: false,
  } as const;

  it("counts foreign keys and semantic suggestions", async () => {
    const shown = await runWith({
      candidates_found: 5,
      stored_ids: [1, 2, 3, 4, 5],
      foreign_keys: { found: 2 },
      semantics: { ...semantics, suggested: 3, proposed: 3 },
    });
    expect(shown).toHaveTextContent("Foreign keys: 2 found.");
    expect(shown).toHaveTextContent("Semantics: 3 suggestions.");
  });

  it("says when the model's answer could not be read", async () => {
    const shown = await runWith({
      ...NOTHING_FOUND,
      semantics: { ...semantics, empty_reason: "unreadable" },
    });
    expect(shown).toHaveTextContent("Semantics: none. The model's answer could not be read.");
  });

  it("says how many fell below the confidence threshold, and how many were already known", async () => {
    const shown = await runWith({
      ...NOTHING_FOUND,
      foreign_keys: { found: 0 },
      semantics: {
        ...semantics,
        proposed: 4,
        below_threshold: 3,
        already_known: 1,
        empty_reason: "filtered",
      },
    });
    expect(shown).toHaveTextContent("Foreign keys: 0 found.");
    expect(shown).toHaveTextContent(
      "Semantics: none kept of the 4 relationships the model proposed: 3 were below the 70% " +
        "confidence threshold; 1 was already rejected or already present.",
    );
  });

  it("says the model's answer was cut short when it stopped at its length limit", async () => {
    const shown = await runWith({
      candidates_found: 34,
      stored_ids: [],
      semantics: { ...semantics, suggested: 34, proposed: 34, cut_short: true },
    });
    expect(shown).toHaveTextContent(
      "Semantics: 34 suggestions. The model's answer stopped at its length limit, so only the " +
        "suggestions it finished were read.",
    );
  });

  it("says why there are none when the answer stopped before it finished a suggestion", async () => {
    const shown = await runWith({
      ...NOTHING_FOUND,
      semantics: { ...semantics, empty_reason: "cut_short", cut_short: true },
    });
    expect(shown).toHaveTextContent(
      "Semantics: none. The model's answer stopped at its length limit before it finished a " +
        "suggestion.",
    );
    expect(shown).not.toHaveTextContent("could not be read");
  });

  it("says when the model proposed none", async () => {
    const shown = await runWith({
      ...NOTHING_FOUND,
      semantics: { ...semantics, empty_reason: "none_proposed" },
    });
    expect(shown).toHaveTextContent("Semantics: none. The model proposed no relationships.");
  });

  it("shows a model failure as an error", async () => {
    discoverMock.mockRejectedValue(new Error("The model openai:gpt-test failed: 401 invalid key"));
    await openDialog();
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(screen.getByTestId("rels-discover-error")).toHaveTextContent(
        "The model openai:gpt-test failed: 401 invalid key",
      ),
    );
    expect(screen.queryByTestId("rels-discover-result")).not.toBeInTheDocument();
  });

  it("heads the suggestions for what they hold, each row keeping its stated origin", async () => {
    candidatesMock.mockResolvedValue([
      {
        id: 7,
        source_table_id: 1,
        source_column: "customer_id",
        target_table_id: 2,
        target_column: "id",
        cardinality: "many_to_one",
        confidence: 1,
        reasoning: "Foreign key constraint",
        suggested_name: "orders-customer_id-to-customers",
      },
    ]);
    await runWith({ candidates_found: 1, stored_ids: [7], foreign_keys: { found: 1 } });
    await waitFor(() => expect(screen.getByText("Suggested Relationships")).toBeInTheDocument());
    expect(screen.queryByText(/AI-Suggested/)).not.toBeInTheDocument();
    expect(screen.getByText("Foreign key constraint")).toBeInTheDocument();
  });
});
