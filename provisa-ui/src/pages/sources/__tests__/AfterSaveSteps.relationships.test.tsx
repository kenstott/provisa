// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e2a17-6b38-4f05-a1d9-7e3f5c0b8d42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2262: registering a source's tables ends with a step to relate them. Beside Close, Create
// relationships appears when at least one table was just registered and the person holds
// create_relationship; it opens the review, and Done returns to the source with the count.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { TableRegistrationOutcome } from "../../../hooks/useTableQueries";

const auth = { capabilities: ["create_relationship"], domainAccess: null, actingRoles: "admin" };
let registeredIds: number[] = [];

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>(), domainsEnabled: false }),
}));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useSources: () => ({ sources: [] }),
  useDomains: () => ({ domains: [] }),
  useRoles: () => ({ roles: [] }),
  useTableDetails: () => ({ tables: [] }),
  useSuggestTableAlias: () => ({ suggestTableAlias: vi.fn() }),
}));
vi.mock("../../tables/RegisterTableForm", () => ({
  RegisterTableForm: ({
    onRegisteredMany,
  }: {
    onRegisteredMany: (outcomes: TableRegistrationOutcome[]) => void;
  }) => (
    <button
      data-testid="register"
      onClick={() =>
        onRegisteredMany(
          registeredIds.map(
            (id) =>
              ({
                result: { success: true, params: { id } },
              }) as unknown as TableRegistrationOutcome,
          ),
        )
      }
    >
      register
    </button>
  ),
}));
vi.mock("../../tables/YourDataStep", () => ({
  YourDataStep: () => <div data-testid="your-data" />,
}));
vi.mock("../../../components/relationships/RelationshipReview", () => ({
  RelationshipReview: ({
    tableIds,
    onDone,
  }: {
    tableIds: number[];
    onDone: (created: number) => void;
  }) => (
    <button data-testid="review" onClick={() => onDone(2)}>
      review {tableIds.join(",")}
    </button>
  ),
}));

const { AfterSaveSteps } = await import("../AfterSaveSteps");

function renderSteps() {
  const onClose = vi.fn();
  const onRelated = vi.fn();
  render(<AfterSaveSteps sourceId="pg" onClose={onClose} onRelated={onRelated} />);
  fireEvent.click(screen.getByTestId("register"));
  return { onClose, onRelated };
}

beforeEach(() => {
  auth.capabilities = ["create_relationship"];
  registeredIds = [11, 12];
});

describe("AfterSaveSteps: Create relationships", () => {
  it("appears beside Close once tables are registered, and opens the review on them", () => {
    const { onRelated } = renderSteps();
    expect(screen.getByTestId("your-data-back")).toBeTruthy();
    fireEvent.click(screen.getByTestId("your-data-create-relationships"));
    expect(screen.getByTestId("review").textContent).toBe("review 11,12");
    fireEvent.click(screen.getByTestId("review"));
    expect(onRelated).toHaveBeenCalledWith(2);
  });

  it("is not offered without create_relationship", () => {
    auth.capabilities = ["table_registration"];
    renderSteps();
    expect(screen.getByTestId("your-data")).toBeTruthy();
    expect(screen.queryByTestId("your-data-create-relationships")).toBeNull();
  });

  it("is not offered when no table was registered", () => {
    registeredIds = [];
    renderSteps();
    expect(screen.getByTestId("your-data")).toBeTruthy();
    expect(screen.queryByTestId("your-data-create-relationships")).toBeNull();
  });
});
