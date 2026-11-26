// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2d4e81-0c59-4b36-9f17-e5b8c3a6d240
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 / REQ-2262: a source's setup, one flow shown with the same strip wherever it is reached
// -- right after the source is saved (opened: each step with its content) or later from the read
// view -- Connect source, Register tables, Your data, Discover relationships. Each step is done once
// it has an outcome, recorded on the server (setSourceSetupOutcomes), or once it has produced its
// object: each step's Skip and Done, opening a query from the flow recording Query, Finish setup,
// the collapsed Setup complete line, Run setup again, and the hint for tables registered since.

import { useEffect, useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { act, fireEvent, render, screen } from "../../../test-utils/render";
import type { Relationship, SourceSetupOutcomes, TableRow } from "../../../types/admin";
import type { SetupOutcomesWrite } from "../../../hooks/useSourceSetupOutcomes";
import type { TableRegistrationOutcome } from "../../../hooks/useTableQueries";
import type { Registered } from "../../tables/useManyTables";

const auth = { capabilities: ["create_relationship"], actingRoles: "analyst", domainAccess: null };
let tables: TableRow[] = [];
let relationships: Relationship[] = [];
const refetchRelationships = vi.fn();
const refetchTables = vi.fn();
const GRAPHQL = "query {\n  pg__orders(limit: 10) {\n    id\n  }\n}";
const DECIDED_AT = "2026-11-20T12:00:00+00:00";
const NONE: SourceSetupOutcomes = { tables: null, relationships: null, query: null, decidedAt: null };
// What the registration form registers when pressed: the ids it answers with, and the refused.
let registering: { ids: number[]; refused: string[] } = { ids: [], refused: [] };

// The server: each write replaces the record, as setSourceSetupOutcomes does, and the panel is
// given it again.
const writes: { sourceId: string; outcomes: SetupOutcomesWrite }[] = [];
let held: SourceSetupOutcomes = NONE;
let refuse: string | null = null;
let setHeld: (o: SourceSetupOutcomes) => void = () => undefined;

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>(), domainsEnabled: false }),
}));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({ tables, loading: false, refetch: refetchTables }),
  useRelationships: () => ({ relationships, loading: false, refetch: refetchRelationships }),
  useTableAccess: (tableId: number | null, role: string | null) => ({
    access: tableId === 11 && role === "analyst" ? { graphql: GRAPHQL } : null,
  }),
  useSources: () => ({ sources: [] }),
  useDomains: () => ({ domains: [] }),
  useRoles: () => ({ roles: [] }),
  useTableDetails: () => ({ tables: [] }),
  useSuggestTableAlias: () => ({ suggestTableAlias: vi.fn() }),
}));
vi.mock("../../../hooks/useSourceSetupOutcomes", () => ({
  useSetSourceSetupOutcomes: () => async (sourceId: string, outcomes: SetupOutcomesWrite) => {
    writes.push({ sourceId, outcomes });
    if (refuse !== null) throw new Error(refuse);
    const decided = outcomes.tables && outcomes.relationships && outcomes.query;
    // The store stamps a write that decides every step with its own time.
    setHeld({ ...outcomes, decidedAt: decided ? new Date().toISOString() : null });
  },
}));
vi.mock("../../../components/relationships/RelationshipReview", () => ({
  RelationshipReview: ({
    tableIds,
    onDone,
  }: {
    tableIds: number[];
    onDone: (created: number) => void;
  }) => (
    <div data-testid="review">
      review {tableIds.join(",")}
      <button data-testid="review-none" onClick={() => onDone(0)} />
      <button data-testid="review-some" onClick={() => onDone(2)} />
    </div>
  ),
}));
vi.mock("../../tables/RegisterTableForm", () => ({
  RegisterTableForm: ({
    initialSourceId,
    pickReady,
    onRegisteredMany,
    onCancel,
  }: {
    initialSourceId: string;
    pickReady: boolean;
    onRegisteredMany: (outcomes: TableRegistrationOutcome[]) => void;
    onCancel: () => void;
  }) => (
    <div data-testid="register-form">
      {`register ${initialSourceId}${pickReady ? " ready picked" : ""}`}
      <button
        data-testid="register-form-submit"
        onClick={() =>
          onRegisteredMany([
            ...registering.ids.map(
              (id) =>
                ({
                  tableName: `t${id}`,
                  result: { success: true, params: { id } },
                }) as unknown as TableRegistrationOutcome,
            ),
            ...registering.refused.map(
              (tableName) =>
                ({
                  tableName,
                  result: { success: false, message: "no" },
                }) as unknown as TableRegistrationOutcome,
            ),
          ])
        }
      />
      <button data-testid="register-form-cancel" onClick={onCancel} />
    </div>
  ),
}));
vi.mock("../../tables/YourDataStep", () => ({
  YourDataStep: ({
    registered,
    role,
    onOpenQuery,
  }: {
    registered: Registered;
    role: string | null;
    onOpenQuery: (graphql: string) => void;
  }) => (
    <div data-testid="your-data">
      {`data ${registered.tableIds.join(",")} as ${role}`}
      {registered.refused.map((o) => ` refused ${o.tableName}`).join("")}
      <button data-testid="your-data-open-query" onClick={() => onOpenQuery(GRAPHQL)} />
    </div>
  ),
}));

const { SourceSetupStepper } = await import("../SourceSetupStepper");

function table(id: number, sourceId: string, tableName: string, registeredAt?: string): TableRow {
  return {
    id,
    sourceId,
    tableName,
    alias: null,
    registeredAt: registeredAt ?? "2026-11-01T00:00:00+00:00",
  } as unknown as TableRow;
}

function relationship(sourceTableId: number, targetTableId: number): Relationship {
  return { id: 1, sourceTableId, targetTableId } as unknown as Relationship;
}

function QueryState() {
  const location = useLocation();
  const navigate = useNavigate();
  return (
    <>
      <pre data-testid="query-state">{(location.state as { query: string }).query}</pre>
      <button data-testid="back-to-source" onClick={() => navigate("/")} />
    </>
  );
}

interface Calls {
  onConnect: () => void;
  onOpen: (step: number | null) => void;
  onClose: () => void;
  onRelated: (count: number) => void;
}

function Panel({ open, step, calls }: { open: boolean; step: number | null; calls: Calls }) {
  const [outcomes, setOutcomes] = useState(held);
  useEffect(() => {
    setHeld = (o) => {
      held = o;
      setOutcomes(o);
    };
  }, []);
  return (
    <SourceSetupStepper
      sourceId="pg"
      outcomes={outcomes}
      open={open}
      step={step}
      onConnect={calls.onConnect}
      onOpen={calls.onOpen}
      onClose={calls.onClose}
      onRelated={calls.onRelated}
    />
  );
}

function renderStepper({ open = false, step = null }: { open?: boolean; step?: number | null } = {}) {
  const calls = { onConnect: vi.fn(), onOpen: vi.fn(), onClose: vi.fn(), onRelated: vi.fn() };
  render(
    <Routes>
      <Route path="/" element={<Panel open={open} step={step} calls={calls} />} />
      <Route path="/query" element={<QueryState />} />
    </Routes>,
  );
  return calls;
}

async function press(testId: string) {
  await act(async () => {
    fireEvent.click(screen.getByTestId(testId));
  });
}

const active = () => screen.getByTestId("source-setup-stepper").getAttribute("data-active");
const lastWrite = () => writes[writes.length - 1];

beforeEach(() => {
  auth.capabilities = ["create_relationship"];
  tables = [];
  relationships = [];
  held = NONE;
  refuse = null;
  registering = { ids: [], refused: [] };
  writes.length = 0;
  refetchRelationships.mockReset();
  refetchTables.mockReset();
});

describe("SourceSetupStepper on the read view", () => {
  it("a new source, no tables registered: Register tables is current", () => {
    tables = [table(5, "other", "people")];
    const { onOpen } = renderStepper();
    expect(active()).toBe("1");
    expect(screen.getByText("Connect source")).toBeInTheDocument();
    expect(screen.getByText("Your data")).toBeInTheDocument();
    expect(screen.getByText("Discover relationships")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("source-setup-register"));
    // Register tables opens the flow on its current step: the same strip, with the form.
    expect(onOpen).toHaveBeenCalledWith(null);
    expect(screen.queryByTestId("source-setup-query")).toBeNull();
  });

  it("Register tables' Skip records tables skipped and goes on to Your data", async () => {
    tables = [table(5, "other", "people")];
    renderStepper();
    await press("source-setup-skip-tables");
    expect(lastWrite()).toEqual({
      sourceId: "pg",
      outcomes: { tables: "skipped", relationships: null, query: null },
    });
    // No table of its own: nothing to query, nothing to relate, Done finishes.
    expect(active()).toBe("2");
    expect(screen.getByText("Skipped")).toBeInTheDocument();
    expect(screen.getByText("No tables are registered to query.")).toBeInTheDocument();
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "skipped",
      relationships: "none_found",
      query: "done",
    });
    expect(screen.getByTestId("source-setup-complete")).toBeInTheDocument();
  });

  it("tables registered, no query yet: Your data is step 2, a click from the flow", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    relationships = [relationship(5, 99)];
    const { onOpen } = renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("2 tables")).toBeInTheDocument();
    expect(screen.getByTestId("source-setup-query")).toBeEnabled();
    fireEvent.click(screen.getByTestId("source-setup-your-data"));
    expect(onOpen).toHaveBeenCalledWith(null);
  });

  it("Your data decided, no relationships: Discover relationships is step 3, across every source", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    relationships = [relationship(5, 99)];
    held = { ...NONE, query: "opened" };
    renderStepper();
    expect(active()).toBe("3");
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    // The review is scoped to this source's tables; it proposes joins against the whole model.
    expect(screen.getByTestId("review")).toHaveTextContent("review 11,12");
  });

  it("a run that found relationships finishes setup as found, with the count", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    const { onRelated } = renderStepper();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    await press("review-some");
    expect(refetchRelationships).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("review")).toBeNull();
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "found",
      query: "opened",
    });
    expect(onRelated).toHaveBeenCalledWith(2);
    expect(screen.getByTestId("source-setup-complete")).toBeInTheDocument();
  });

  it("a run that found nothing finishes the step as none found", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "done" };
    renderStepper();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    await press("review-none");
    expect(lastWrite().outcomes.relationships).toBe("none_found");
    expect(screen.getByTestId("source-setup-complete")).toBeInTheDocument();
  });

  it("Discover relationships' Skip, before a run, records relationships skipped", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    renderStepper();
    await press("source-setup-skip-relationships");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "skipped",
      query: "opened",
    });
    expect(screen.getByTestId("source-setup-complete")).toBeInTheDocument();
  });

  it("a recorded outcome holds after a reload: the stepper reads the server's record", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { tables: "registered", relationships: null, query: "done", decidedAt: null };
    renderStepper();
    expect(active()).toBe("3");
  });

  it("nothing to relate: says so on Your data, and setup ends with it", async () => {
    tables = [table(11, "pg", "orders")];
    renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("Nothing to relate")).toBeInTheDocument();
    expect(screen.getByTestId("source-setup-relate-note")).toHaveTextContent(
      "There is nothing to relate these tables to yet.",
    );
    expect(screen.getByTestId("source-setup-query")).toBeEnabled();
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "none_found",
      query: "done",
    });
    expect(screen.getByTestId("source-setup-complete")).toBeInTheDocument();
  });

  it("without the create relationship right: no discovery offered", () => {
    auth.capabilities = [];
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("Not permitted")).toBeInTheDocument();
    expect(screen.queryByTestId("source-setup-relate")).toBeNull();
    expect(screen.getByTestId("source-setup-relate-note")).toHaveTextContent(
      "create relationship right",
    );
  });

  it("the starter query records Query opened before it opens the query explorer", async () => {
    tables = [table(11, "pg", "orders")];
    renderStepper();
    expect(screen.getByTestId("source-setup-relate-note").nextSibling).toHaveTextContent("orders");
    await press("source-setup-query");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "none_found",
      query: "opened",
    });
    expect(screen.getByTestId("query-state").textContent).toBe(GRAPHQL);
  });

  it("Your data's Done with relationships already found collapses the strip", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    relationships = [relationship(5, 11)];
    renderStepper();
    expect(active()).toBe("2");
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "found",
      query: "done",
    });
    expect(screen.getByTestId("source-setup-complete")).toHaveTextContent("Setup complete");
    expect(screen.queryByTestId("source-setup-stepper")).toBeNull();
  });

  it("Your data's Done with relationships to discover goes on to Discover relationships", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: null,
      query: "done",
    });
    expect(active()).toBe("3");
    expect(screen.getByTestId("source-setup-skip-relationships")).toBeInTheDocument();
  });

  it("Finish setup, always shown while active, closes every remaining step as skipped", async () => {
    tables = [table(5, "other", "people")];
    renderStepper();
    expect(screen.getByTestId("source-setup-finish")).toHaveTextContent("Finish setup");
    await press("source-setup-finish");
    expect(lastWrite().outcomes).toEqual({
      tables: "skipped",
      relationships: "none_found",
      query: "skipped",
    });
    expect(screen.getByTestId("source-setup-complete")).toBeInTheDocument();
  });

  it("Finish setup keeps what the steps already show", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    expect(active()).toBe("2");
    await press("source-setup-finish");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "skipped",
      query: "skipped",
    });
  });

  it("collapsed, it never reopens on its own; Run setup again clears the record", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = {
      tables: "registered",
      relationships: "skipped",
      query: "skipped",
      decidedAt: DECIDED_AT,
    };
    renderStepper();
    expect(screen.getByTestId("source-setup-complete")).toBeInTheDocument();
    expect(screen.queryByTestId("source-setup-stepper")).toBeNull();
    expect(screen.queryByTestId("source-setup-new-tables")).toBeNull();
    await press("source-setup-again");
    expect(lastWrite().outcomes).toEqual({ tables: null, relationships: null, query: null });
    // The tables it holds still say Register tables is done: back to Your data.
    expect(active()).toBe("2");
  });

  it("collapsed: tables registered since, related to nothing, show as a one-line hint", async () => {
    tables = [
      table(11, "pg", "orders"),
      table(12, "pg", "returns", "2026-11-25T00:00:00+00:00"),
      table(13, "pg", "refunds", "2026-11-25T00:00:00+00:00"),
      table(5, "other", "people"),
    ];
    relationships = [relationship(13, 5)];
    held = { tables: "registered", relationships: "found", query: "done", decidedAt: DECIDED_AT };
    renderStepper();
    expect(screen.queryByTestId("source-setup-stepper")).toBeNull();
    expect(screen.getByTestId("source-setup-new-tables")).toHaveTextContent(
      "1 table registered since setup has no relationship.",
    );
    fireEvent.click(screen.getByTestId("source-setup-new-tables-relate"));
    expect(screen.getByTestId("review")).toHaveTextContent("review 12");
    await press("review-none");
    // Reviewed: decided again from now, so the hint goes.
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "found",
      query: "done",
    });
    expect(screen.queryByTestId("source-setup-new-tables")).toBeNull();
    expect(screen.queryByTestId("source-setup-stepper")).toBeNull();
  });

  it("a refused write says why and keeps the step", async () => {
    tables = [table(5, "other", "people")];
    refuse = "Source 'pg' not found";
    renderStepper();
    await press("source-setup-skip-tables");
    expect(screen.getByTestId("source-setup-error")).toHaveTextContent("Source 'pg' not found");
    expect(active()).toBe("1");
  });

  it("a done step opens the flow on it; a step not reached does nothing", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    const { onConnect, onOpen } = renderStepper();
    expect(active()).toBe("3");
    fireEvent.click(screen.getByText("Connect source"));
    expect(onConnect).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByText("Register tables"));
    expect(onOpen).toHaveBeenLastCalledWith(1);
    fireEvent.click(screen.getByText("Your data"));
    expect(onOpen).toHaveBeenLastCalledWith(2);
    expect(onOpen).toHaveBeenCalledTimes(2);
  });

  it("a step not yet reached does nothing", () => {
    tables = [table(5, "other", "people")];
    const { onConnect, onOpen } = renderStepper();
    fireEvent.click(screen.getByText("Your data"));
    fireEvent.click(screen.getByText("Discover relationships"));
    expect(onConnect).not.toHaveBeenCalled();
    expect(onOpen).not.toHaveBeenCalled();
  });
});

describe("SourceSetupStepper opened: the flow after a save", () => {
  it("is the only setup flow: the separate after-save steps are gone", () => {
    expect(Object.keys(import.meta.glob("../AfterSaveSteps.tsx"))).toHaveLength(0);
  });

  it("shows the strip at Register tables, with Register Table on the source and Skip", () => {
    tables = [table(5, "other", "people")];
    renderStepper({ open: true });
    expect(active()).toBe("1");
    expect(screen.getByTestId("source-setup-step")).toBeInTheDocument();
    expect(screen.getByTestId("register-form")).toHaveTextContent("register pg ready picked");
    expect(screen.getByTestId("source-setup-skip-tables")).toBeInTheDocument();
    expect(screen.getByTestId("source-setup-finish")).toBeInTheDocument();
    // The read view's one-line step is not the opened flow's: the form is the step.
    expect(screen.queryByTestId("source-setup-register")).toBeNull();
  });

  it("registering records the tables and goes on to Your data, step 2, on the tables just registered", async () => {
    tables = [table(5, "other", "people")];
    registering = { ids: [11, 12], refused: ["broken"] };
    refetchTables.mockImplementation(async () => {
      tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    });
    renderStepper({ open: true });
    await press("register-form-submit");
    expect(refetchTables).toHaveBeenCalledTimes(1);
    expect(lastWrite().outcomes).toEqual({ tables: "registered", relationships: null, query: null });
    expect(active()).toBe("2");
    expect(screen.getByTestId("your-data")).toHaveTextContent("data 11,12 as analyst refused broken");
    expect(screen.getByTestId("source-setup-done")).toBeInTheDocument();
    expect(screen.queryByTestId("register-form")).toBeNull();
  });

  it("Your data, opened later, shows the source's own tables", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    renderStepper({ open: true });
    expect(active()).toBe("2");
    expect(screen.getByTestId("your-data")).toHaveTextContent("data 11,12 as analyst");
  });

  it("opening a query from Your data records Query; returning resumes at Discover relationships", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper({ open: true });
    await press("your-data-open-query");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: null,
      query: "opened",
    });
    expect(screen.getByTestId("query-state").textContent).toBe(GRAPHQL);
    // Back on the source: the read view's strip remembers the query and stands at step 3.
    await press("back-to-source");
    expect(active()).toBe("3");
    expect(screen.getByTestId("source-setup-relate")).toBeInTheDocument();
  });

  it("Discover relationships is step 3 with Skip; deciding it ends the flow", async () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    const { onClose, onRelated } = renderStepper({ open: true });
    expect(active()).toBe("3");
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    expect(screen.getByTestId("review")).toHaveTextContent("review 11,12");
    await press("review-some");
    expect(onRelated).toHaveBeenCalledWith(2);
    expect(onClose).toHaveBeenCalled();
  });

  it("Discover relationships' Skip, opened, records skipped and ends the flow", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "done" };
    const { onClose } = renderStepper({ open: true });
    await press("source-setup-skip-relationships");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "skipped",
      query: "done",
    });
    expect(onClose).toHaveBeenCalled();
  });

  it("not offered without create_relationship: the flow ends at Your data", async () => {
    auth.capabilities = ["table_registration"];
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    const { onClose } = renderStepper({ open: true });
    expect(screen.getByTestId("your-data")).toBeInTheDocument();
    expect(screen.queryByTestId("source-setup-relate")).toBeNull();
    await press("source-setup-done");
    expect(lastWrite().outcomes.relationships).toBe("skipped");
    expect(onClose).toHaveBeenCalled();
  });

  it("not offered when no table was registered", async () => {
    tables = [table(5, "other", "people")];
    renderStepper({ open: true });
    await press("source-setup-skip-tables");
    expect(active()).toBe("2");
    expect(screen.getByText("No tables are registered to query.")).toBeInTheDocument();
    expect(screen.queryByTestId("source-setup-relate")).toBeNull();
  });

  it("the form's Cancel leaves the flow with nothing recorded", async () => {
    tables = [table(5, "other", "people")];
    const { onClose } = renderStepper({ open: true });
    await press("register-form-cancel");
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(writes).toHaveLength(0);
  });

  it("back to a done step shows its content without deciding it again", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    renderStepper({ open: true });
    expect(active()).toBe("3");
    fireEvent.click(screen.getByText("Your data"));
    expect(screen.getByTestId("your-data")).toHaveTextContent("data 11 as analyst");
    expect(screen.queryByTestId("source-setup-done")).toBeNull();
    fireEvent.click(screen.getByText("Register tables"));
    expect(screen.getByTestId("register-form")).toBeInTheDocument();
    expect(screen.queryByTestId("source-setup-skip-tables")).toBeNull();
    expect(active()).toBe("3");
    expect(writes).toHaveLength(0);
  });

  it("opened on a done step gone back to from the read view", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    renderStepper({ open: true, step: 1 });
    expect(screen.getByTestId("register-form")).toBeInTheDocument();
  });
});
