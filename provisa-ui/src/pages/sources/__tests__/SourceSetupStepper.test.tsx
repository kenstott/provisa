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
// Discover relationships, Query. Each state (new source, tables but no relationships, nothing to
// relate, complete), going back to a done step, and the hand-off to the query explorer.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { Route, Routes, useLocation } from "react-router-dom";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { Relationship, TableRow } from "../../../types/admin";

const auth = { capabilities: ["create_relationship"], actingRoles: "analyst" };
let tables: TableRow[] = [];
let relationships: Relationship[] = [];
const refetchRelationships = vi.fn();
const GRAPHQL = "query {\n  pg__orders(limit: 10) {\n    id\n  }\n}";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({ tables, loading: false }),
  useRelationships: () => ({ relationships, loading: false, refetch: refetchRelationships }),
  useTableAccess: (tableId: number | null, role: string | null) => ({
    access: tableId === 11 && role === "analyst" ? { graphql: GRAPHQL } : null,
  }),
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

function table(id: number, sourceId: string, tableName: string): TableRow {
  return { id, sourceId, tableName, alias: null } as unknown as TableRow;
}

function relationship(sourceTableId: number, targetTableId: number): Relationship {
  return { id: 1, sourceTableId, targetTableId } as unknown as Relationship;
}

function QueryState() {
  const location = useLocation();
  return <pre data-testid="query-state">{(location.state as { query: string }).query}</pre>;
}

function renderStepper() {
  const onConnect = vi.fn();
  const onRegister = vi.fn();
  render(
    <Routes>
      <Route
        path="/"
        element={<SourceSetupStepper sourceId="pg" onConnect={onConnect} onRegister={onRegister} />}
      />
      <Route path="/query" element={<QueryState />} />
    </Routes>,
  );
  return { onConnect, onRegister };
}

const active = () => screen.getByTestId("source-setup-stepper").getAttribute("data-active");

beforeEach(() => {
  auth.capabilities = ["create_relationship"];
  tables = [];
  relationships = [];
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

  it("tables but no relationships: Discover relationships is current, across every source", () => {
    tables = [table(11, "pg", "orders"), table(12, "pg", "lines"), table(5, "other", "people")];
    relationships = [relationship(5, 99)];
    renderStepper();
    expect(active()).toBe("2");
    expect(screen.getByText("2 tables")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    // The review is scoped to this source's tables; it proposes joins against the whole model.
    expect(screen.getByTestId("review")).toHaveTextContent("review 11,12");
    fireEvent.click(screen.getByTestId("review-some"));
    expect(refetchRelationships).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("review")).toBeNull();
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

  it("a review that created nothing: nothing to relate", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    fireEvent.click(screen.getByTestId("review-none"));
    expect(active()).toBe("3");
    expect(screen.getByText("Nothing to relate")).toBeInTheDocument();
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

  it("complete: one Setup complete line, no stepper", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    relationships = [relationship(5, 11)];
    renderStepper();
    expect(screen.getByTestId("source-setup-complete")).toHaveTextContent("Setup complete");
    expect(screen.queryByTestId("source-setup-stepper")).toBeNull();
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

  it("back to Discover relationships once it is done by nothing to relate", () => {
    tables = [table(11, "pg", "orders"), table(5, "other", "people")];
    renderStepper();
    fireEvent.click(screen.getByTestId("source-setup-relate"));
    fireEvent.click(screen.getByTestId("review-none"));
    expect(screen.queryByTestId("review")).toBeNull();
    fireEvent.click(screen.getByText("Discover relationships"));
    expect(screen.getByTestId("review")).toBeInTheDocument();
  });

  it("Query opens the query explorer on a starter query over the source's first table", () => {
    tables = [table(11, "pg", "orders")];
    renderStepper();
    expect(screen.getByTestId("source-setup-relate-note").nextSibling).toHaveTextContent("orders");
    fireEvent.click(screen.getByTestId("source-setup-query"));
    expect(screen.getByTestId("query-state").textContent).toBe(GRAPHQL);
  });
});
