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
// view -- Connect source, Register tables, Review samples, Discover relationships. Each step is done once
// it has an outcome, recorded on the server (setSourceSetupOutcomes), or once it has produced its
// object. The strip shows progress only and a step in it opens that step; the choices are the
// panel's command bar (its head): the step's primary action, Skip step where the step may be
// skipped (not Register tables), and Finish later. There is no Cancel and no Finish setup. Also the
// collapsed Setup complete line, Run setup again, and the hint for tables registered since.

import { useEffect, useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { act, fireEvent, render, screen, within } from "../../../test-utils/render";
import { PanelActionsContext } from "../../../components/list/panelActions";
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
    onCancel?: () => void;
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
      {onCancel !== undefined && <button data-testid="register-form-cancel" onClick={onCancel} />}
    </div>
  ),
}));
vi.mock("../ReviewSamplesStep", () => ({
  ReviewSamplesStep: ({
    registered,
    role,
    onOpenQuery,
    onAllOpened,
  }: {
    registered: Registered;
    role: string | null;
    onOpenQuery: (graphql: string) => void;
    onAllOpened: () => void;
  }) => (
    <div data-testid="review-samples">
      {`data ${registered.tableIds.join(",")} as ${role}`}
      {registered.refused.map((o) => ` refused ${o.tableName}`).join("")}
      <button data-testid="review-samples-query-it" onClick={() => onOpenQuery(GRAPHQL)} />
      <button data-testid="review-samples-all-opened" onClick={onAllOpened} />
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

// The panel's pinned head, where its command bar's actions go (PanelActions).
function Panel({ open, step, calls }: { open: boolean; step: number | null; calls: Calls }) {
  const [outcomes, setOutcomes] = useState(held);
  const [head, setHead] = useState<HTMLElement | null>(null);
  useEffect(() => {
    setHeld = (o) => {
      held = o;
      setOutcomes(o);
    };
  }, []);
  return (
    <>
      <div data-testid="panel-head" ref={setHead} />
      <PanelActionsContext.Provider value={head}>
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
      </PanelActionsContext.Provider>
    </>
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
const head = () => within(screen.getByTestId("panel-head"));
/** The head's actions, in order: what the command bar says. */
const headActions = () =>
  Array.from(screen.getByTestId("panel-head").querySelectorAll("button")).map(
    (b) => b.getAttribute("data-testid") ?? b.textContent,
  );
/** What no step offers any more, in either view. */
const REMOVED = [
  "source-setup-finish",
  "source-setup-skip-tables",
  "source-setup-register",
  "source-setup-your-data",
  "source-setup-query",
  "register-form-cancel",
];
function expectRemovedGone() {
  for (const id of REMOVED) expect(screen.queryByTestId(id)).toBeNull();
}
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

describe("SourceSetupStepper on the read view: the strip shows progress only", () => {
  it("a new source, no tables registered: Register tables is current; pressing it opens it", () => {
    tables = [table(5, "other", "people")];
    const { onOpen } = renderStepper();
    expect(active()).toBe("1");
    expect(screen.getByText("Connect source")).toBeInTheDocument();
    expect(screen.getByText("Review samples")).toBeInTheDocument();
    expect(screen.getByText("Discover relationships")).toBeInTheDocument();
    // The strip holds no buttons, and the read view puts nothing in the panel's head.
    expectRemovedGone();
    expect(screen.queryByTestId("source-setup-done")).toBeNull();
    expect(screen.queryByTestId("source-setup-skip")).toBeNull();
    expect(headActions()).toEqual([]);
    fireEvent.click(screen.getByText("Register tables"));
    // The current step opens the flow on it: the same strip, with the form.
    expect(onOpen).toHaveBeenCalledWith(null);
  });

  it("tables registered, no query yet: Review samples is step 2; pressing it opens it", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    relationships = [relationship(5, 99)];
    const { onOpen } = renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("2 tables")).toBeInTheDocument();
    expectRemovedGone();
    expect(headActions()).toEqual([]);
    fireEvent.click(screen.getByText("Review samples"));
    expect(onOpen).toHaveBeenCalledWith(null);
  });

  it("Review samples decided, no relationships: Discover relationships is step 3; pressing it opens it", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    relationships = [relationship(5, 99)];
    held = { ...NONE, query: "opened" };
    const { onOpen } = renderStepper();
    expect(active()).toBe("3");
    expect(screen.queryByTestId("source-setup-relate")).toBeNull();
    fireEvent.click(screen.getByText("Discover relationships"));
    expect(onOpen).toHaveBeenCalledWith(null);
    expect(screen.queryByTestId("review")).toBeNull();
  });

  it("a recorded outcome holds after a reload: the stepper reads the server's record", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { tables: "registered", relationships: null, query: "done", decidedAt: null };
    renderStepper();
    expect(active()).toBe("3");
  });

  it("a skipped step says so on the strip", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { tables: "registered", relationships: null, query: "skipped", decidedAt: null };
    renderStepper();
    expect(active()).toBe("3");
    expect(screen.getByText("Skipped")).toBeInTheDocument();
  });

  it("nothing to relate: the strip says so", () => {
    tables = [table(11, "pg", "orders")];
    renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("Nothing to relate")).toBeInTheDocument();
  });

  it("without the create relationship right: the strip says not permitted", () => {
    auth.capabilities = [];
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("Not permitted")).toBeInTheDocument();
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
    // The tables it holds still say Register tables is done: back to Review samples.
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

  it("a done step opens the flow on it; a step not reached does nothing", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    const { onConnect, onOpen } = renderStepper();
    expect(active()).toBe("3");
    fireEvent.click(screen.getByText("Connect source"));
    expect(onConnect).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByText("Register tables"));
    expect(onOpen).toHaveBeenLastCalledWith(1);
    fireEvent.click(screen.getByText("Review samples"));
    expect(onOpen).toHaveBeenLastCalledWith(2);
    expect(onOpen).toHaveBeenCalledTimes(2);
  });

  it("a step not yet reached does nothing", () => {
    tables = [table(5, "other", "people")];
    const { onConnect, onOpen } = renderStepper();
    fireEvent.click(screen.getByText("Review samples"));
    fireEvent.click(screen.getByText("Discover relationships"));
    expect(onConnect).not.toHaveBeenCalled();
    expect(onOpen).not.toHaveBeenCalled();
  });
});

describe("SourceSetupStepper opened: the choices are the panel's command bar", () => {
  it("is the only setup flow: the separate after-save steps are gone", () => {
    expect(Object.keys(import.meta.glob("../AfterSaveSteps.tsx"))).toHaveLength(0);
  });

  it("Register tables: the form's Register is the primary; no Skip step, no Cancel; Finish later", () => {
    tables = [table(5, "other", "people")];
    renderStepper({ open: true });
    expect(active()).toBe("1");
    expect(screen.getByTestId("source-setup-step")).toBeInTheDocument();
    expect(screen.getByTestId("register-form")).toHaveTextContent("register pg ready picked");
    // Later steps read the tables it registers: it cannot be skipped.
    expect(screen.queryByTestId("source-setup-skip")).toBeNull();
    expectRemovedGone();
    expect(headActions()).toEqual(["source-setup-finish-later"]);
    expect(head().getByTestId("source-setup-finish-later")).toHaveTextContent("Finish later");
  });

  it("Finish later closes the flow and keeps its progress: nothing is recorded or cleared", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    const { onClose } = renderStepper({ open: true });
    expect(active()).toBe("2");
    fireEvent.click(head().getByTestId("source-setup-finish-later"));
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(writes).toHaveLength(0);
  });

  it("registering records the tables and goes on to Review samples, step 2, on the tables just registered", async () => {
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
    expect(screen.getByTestId("review-samples")).toHaveTextContent("data 11,12 as analyst refused broken");
    expect(screen.queryByTestId("register-form")).toBeNull();
  });

  it("Review samples: Done, Skip step and Finish later, in that order, in the head", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    renderStepper({ open: true });
    expect(active()).toBe("2");
    expect(screen.getByTestId("review-samples")).toHaveTextContent("data 11,12 as analyst");
    expect(headActions()).toEqual([
      "source-setup-done",
      "source-setup-skip",
      "source-setup-finish-later",
    ]);
    expect(head().getByTestId("source-setup-done")).toHaveTextContent("Done");
    expect(head().getByTestId("source-setup-skip")).toHaveTextContent("Skip step");
    expectRemovedGone();
  });

  it("Review samples: Skip step records Query skipped and goes on to Discover relationships", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper({ open: true });
    await press("source-setup-skip");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: null,
      query: "skipped",
    });
    expect(active()).toBe("3");
    expect(screen.getByText("Skipped")).toBeInTheDocument();
  });

  it("Review samples: Skip step with nothing to relate ends the flow", async () => {
    tables = [table(11, "pg", "orders")];
    const { onClose } = renderStepper({ open: true });
    await press("source-setup-skip");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "none_found",
      query: "skipped",
    });
    expect(onClose).toHaveBeenCalled();
  });

  it("nothing to relate: says so on Review samples, and Done ends setup", async () => {
    tables = [table(11, "pg", "orders")];
    const { onClose } = renderStepper({ open: true });
    expect(screen.getByTestId("source-setup-relate-note")).toHaveTextContent(
      "There is nothing to relate these tables to yet.",
    );
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "none_found",
      query: "done",
    });
    expect(onClose).toHaveBeenCalled();
  });

  it("a source whose tables were recorded skipped: nothing to query, Done finishes", async () => {
    tables = [table(5, "other", "people")];
    held = { ...NONE, tables: "skipped" };
    renderStepper({ open: true });
    expect(active()).toBe("2");
    expect(screen.getByText("Skipped")).toBeInTheDocument();
    expect(screen.getByText("No tables are registered to query.")).toBeInTheDocument();
    expect(screen.queryByTestId("source-setup-relate")).toBeNull();
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "skipped",
      relationships: "none_found",
      query: "done",
    });
  });

  it("Review samples: Done with relationships already found ends the flow", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    relationships = [relationship(5, 11)];
    const { onClose } = renderStepper({ open: true });
    expect(active()).toBe("2");
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "found",
      query: "done",
    });
    expect(onClose).toHaveBeenCalled();
  });

  it("Review samples: Done with relationships to discover goes on to Discover relationships", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper({ open: true });
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: null,
      query: "done",
    });
    expect(active()).toBe("3");
    expect(head().getByTestId("source-setup-skip")).toBeInTheDocument();
  });

  it("every sample opened records Review samples done and goes on to Discover relationships", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper({ open: true });
    await press("review-samples-all-opened");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: null,
      query: "done",
    });
    expect(active()).toBe("3");
  });

  it("opening a query from Review samples records Query; returning resumes at Discover relationships", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper({ open: true });
    await press("review-samples-query-it");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: null,
      query: "opened",
    });
    expect(screen.getByTestId("query-state").textContent).toBe(GRAPHQL);
    // Back on the source: the read view's strip remembers the query and stands at step 3.
    await press("back-to-source");
    expect(active()).toBe("3");
  });

  it("opening a query with nothing to relate closes Discover relationships with it", async () => {
    tables = [table(11, "pg", "orders")];
    renderStepper({ open: true });
    await press("review-samples-query-it");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "none_found",
      query: "opened",
    });
    expect(screen.getByTestId("query-state").textContent).toBe(GRAPHQL);
  });

  it("Discover relationships: Create relationships, Skip step and Finish later in the head", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    renderStepper({ open: true });
    expect(active()).toBe("3");
    expect(headActions()).toEqual([
      "source-setup-relate",
      "source-setup-skip",
      "source-setup-finish-later",
    ]);
    expect(head().getByTestId("source-setup-relate")).toHaveTextContent("Create relationships");
    expectRemovedGone();
  });

  it("Create relationships opens the review on the source's tables; a run that found some ends the flow", async () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    const { onClose, onRelated } = renderStepper({ open: true });
    fireEvent.click(head().getByTestId("source-setup-relate"));
    // The review is scoped to this source's tables; it proposes joins against the whole model.
    expect(screen.getByTestId("review")).toHaveTextContent("review 11,12");
    await press("review-some");
    expect(refetchRelationships).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("review")).toBeNull();
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "found",
      query: "opened",
    });
    expect(onRelated).toHaveBeenCalledWith(2);
    expect(onClose).toHaveBeenCalled();
  });

  it("a run that found nothing finishes the step as none found", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "done" };
    const { onClose } = renderStepper({ open: true });
    fireEvent.click(head().getByTestId("source-setup-relate"));
    await press("review-none");
    expect(lastWrite().outcomes.relationships).toBe("none_found");
    expect(onClose).toHaveBeenCalled();
  });

  it("Discover relationships' Skip step records skipped and ends the flow", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "done" };
    const { onClose } = renderStepper({ open: true });
    await press("source-setup-skip");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "skipped",
      query: "done",
    });
    expect(onClose).toHaveBeenCalled();
  });

  it("not offered without create_relationship: the flow ends at Review samples", async () => {
    auth.capabilities = ["table_registration"];
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    const { onClose } = renderStepper({ open: true });
    expect(screen.getByTestId("review-samples")).toBeInTheDocument();
    expect(screen.getByTestId("source-setup-relate-note")).toHaveTextContent(
      "create relationship right",
    );
    expect(screen.queryByTestId("source-setup-relate")).toBeNull();
    await press("source-setup-done");
    expect(lastWrite().outcomes.relationships).toBe("skipped");
    expect(onClose).toHaveBeenCalled();
  });

  it("a refused write says why and keeps the step", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    refuse = "Source 'pg' not found";
    renderStepper({ open: true });
    await press("source-setup-skip");
    expect(screen.getByTestId("source-setup-error")).toHaveTextContent("Source 'pg' not found");
    expect(active()).toBe("2");
  });

  it("a step in the strip opens it: back to a done step shows its content without deciding it again", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { ...NONE, query: "opened" };
    renderStepper({ open: true });
    expect(active()).toBe("3");
    fireEvent.click(screen.getByText("Review samples"));
    expect(screen.getByTestId("review-samples")).toHaveTextContent("data 11 as analyst");
    // A done step gone back to is not decided again: the head offers only Finish later.
    expect(headActions()).toEqual(["source-setup-finish-later"]);
    fireEvent.click(screen.getByText("Register tables"));
    expect(screen.getByTestId("register-form")).toBeInTheDocument();
    expect(screen.queryByTestId("source-setup-skip")).toBeNull();
    // The current step, pressed, opens it again.
    fireEvent.click(screen.getByText("Discover relationships"));
    expect(head().getByTestId("source-setup-relate")).toBeInTheDocument();
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
