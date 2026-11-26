// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2d4e81-0c59-4b36-9f17-e5b8c3a6d240
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 / REQ-2262: the source's setup path as a stepper -- Connect source, Register tables,
// Discover relationships, Query. Each step is done once it has an outcome, recorded on the server
// (setSourceSetupOutcomes), or once it has produced its object: each step's Skip and Done, Finish
// setup, the collapsed Setup complete line, Run setup again, and the hint for tables registered
// since.

import { useEffect, useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Route, Routes, useLocation } from "react-router-dom";
import { act, fireEvent, render, screen } from "../../../test-utils/render";
import type { Relationship, SourceSetupOutcomes, TableRow } from "../../../types/admin";
import type { SetupOutcomesWrite } from "../../../hooks/useSourceSetupOutcomes";

const auth = { capabilities: ["create_relationship"], actingRoles: "analyst" };
let tables: TableRow[] = [];
let relationships: Relationship[] = [];
const refetchRelationships = vi.fn();
const GRAPHQL = "query {\n  pg__orders(limit: 10) {\n    id\n  }\n}";
const DECIDED_AT = "2026-11-20T12:00:00+00:00";
const NONE: SourceSetupOutcomes = { tables: null, relationships: null, query: null, decidedAt: null };

// The server: each write replaces the record, as setSourceSetupOutcomes does, and the panel is
// given it again.
const writes: { sourceId: string; outcomes: SetupOutcomesWrite }[] = [];
let held: SourceSetupOutcomes = NONE;
let refuse: string | null = null;
let setHeld: (o: SourceSetupOutcomes) => void = () => undefined;

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({ tables, loading: false }),
  useRelationships: () => ({ relationships, loading: false, refetch: refetchRelationships }),
  useTableAccess: (tableId: number | null, role: string | null) => ({
    access: tableId === 11 && role === "analyst" ? { graphql: GRAPHQL } : null,
  }),
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
  return <pre data-testid="query-state">{(location.state as { query: string }).query}</pre>;
}

function Panel({ onConnect, onRegister }: { onConnect: () => void; onRegister: () => void }) {
  const [outcomes, setOutcomes] = useState(held);
  useEffect(() => {
    setHeld = setOutcomes;
  }, []);
  return (
    <SourceSetupStepper
      sourceId="pg"
      outcomes={outcomes}
      onConnect={onConnect}
      onRegister={onRegister}
    />
  );
}

function renderStepper() {
  const onConnect = vi.fn();
  const onRegister = vi.fn();
  render(
    <Routes>
      <Route path="/" element={<Panel onConnect={onConnect} onRegister={onRegister} />} />
      <Route path="/query" element={<QueryState />} />
    </Routes>,
  );
  return { onConnect, onRegister };
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
  writes.length = 0;
  refetchRelationships.mockReset();
});

describe("SourceSetupStepper", () => {
  it("a new source, no tables registered: Register tables is current", () => {
    tables = [table(5, "other", "people")];
    const { onRegister } = renderStepper();
    expect(active()).toBe("1");
    expect(screen.getByText("Connect source")).toBeInTheDocument();
    expect(screen.getByText("Query")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("source-setup-register"));
    expect(onRegister).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("source-setup-query")).toBeNull();
  });

  it("Register tables' Skip records tables skipped and goes on to Query", async () => {
    tables = [table(5, "other", "people")];
    renderStepper();
    await press("source-setup-skip-tables");
    expect(lastWrite()).toEqual({
      sourceId: "pg",
      outcomes: { tables: "skipped", relationships: null, query: null },
    });
    // No table of its own: nothing to relate, nothing to query, Done finishes.
    expect(active()).toBe("3");
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

  it("tables but no relationships: Discover relationships is current, across every source", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    relationships = [relationship(5, 99)];
    renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("2 tables")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    // The review is scoped to this source's tables; it proposes joins against the whole model.
    expect(screen.getByTestId("review")).toHaveTextContent("review 11,12");
  });

  it("a run that found relationships finishes the step as found", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    await press("review-some");
    expect(refetchRelationships).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("review")).toBeNull();
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "found",
      query: null,
    });
    expect(active()).toBe("3");
  });

  it("a run that found nothing finishes the step as none found", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    await press("review-none");
    expect(lastWrite().outcomes.relationships).toBe("none_found");
    expect(active()).toBe("3");
    expect(screen.getByText("Nothing to relate")).toBeInTheDocument();
  });

  it("Discover relationships' Skip, before a run, records relationships skipped", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    await press("source-setup-skip-relationships");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "skipped",
      query: null,
    });
    expect(active()).toBe("3");
    expect(screen.getByText("Skipped")).toBeInTheDocument();
  });

  it("a recorded outcome holds after a reload: the stepper reads the server's record", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    held = { tables: "registered", relationships: "none_found", query: null, decidedAt: null };
    renderStepper();
    expect(active()).toBe("3");
  });

  it("nothing to relate: says so and goes on to Query", () => {
    tables = [table(11, "pg", "orders")];
    renderStepper();
    expect(active()).toBe("3");
    expect(screen.getByText("Nothing to relate")).toBeInTheDocument();
    expect(screen.getByTestId("source-setup-relate-note")).toHaveTextContent(
      "There is nothing to relate these tables to yet.",
    );
    expect(screen.getByTestId("source-setup-query")).toBeEnabled();
  });

  it("without the create relationship right: no discovery, on to Query", () => {
    auth.capabilities = [];
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    expect(active()).toBe("3");
    expect(screen.queryByTestId("source-setup-relate")).toBeNull();
    expect(screen.getByTestId("source-setup-relate-note")).toHaveTextContent(
      "create relationship right",
    );
  });

  it("Query's starter query records opened and opens the query explorer", async () => {
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

  it("Query's Done records done and collapses the strip", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    relationships = [relationship(5, 11)];
    renderStepper();
    expect(active()).toBe("3");
    await press("source-setup-done");
    expect(lastWrite().outcomes).toEqual({
      tables: "registered",
      relationships: "found",
      query: "done",
    });
    expect(screen.getByTestId("source-setup-complete")).toHaveTextContent("Setup complete");
    expect(screen.queryByTestId("source-setup-stepper")).toBeNull();
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
    // The tables it holds still say Register tables is done: back to Discover relationships.
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

  it("a done step goes back to it; a step not reached does nothing", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    const { onConnect, onRegister } = renderStepper();
    expect(active()).toBe("2");
    fireEvent.click(screen.getByText("Connect source"));
    expect(onConnect).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByText("Register tables"));
    expect(onRegister).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByText("Query"));
    expect(onConnect).toHaveBeenCalledTimes(1);
    expect(onRegister).toHaveBeenCalledTimes(1);
  });

  it("back to Discover relationships once it is done", async () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    await press("review-none");
    expect(screen.queryByTestId("review")).toBeNull();
    fireEvent.click(screen.getByText("Discover relationships"));
    expect(screen.getByTestId("review")).toBeInTheDocument();
  });
});
