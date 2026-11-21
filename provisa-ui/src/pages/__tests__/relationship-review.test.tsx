// Copyright (c) 2026 Kenneth Stott
// Canary: 3f7b0e94-2d61-4a8c-9e35-c8a14b6d0f27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2262: the review after registration opens the discover dialog scoped to the tables just
// registered, against every table either way round, with declared foreign keys, key names and
// types, and semantics asked. Every candidate waits in the run's result; Done ends the review
// with how many relationships its Applies created.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { RelationshipReview } from "../../components/relationships/RelationshipReview";
import { reviewStart } from "../../components/relationships/reviewStart";
import type { ListedTable } from "../../types/admin";
import {
  SLOW,
  applyMock,
  lastScope,
  proposal,
  proposalsMock,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  startMock,
} from "./discoverDialog.fixtures";
import { REGISTERED } from "./discoverDialog.state";

vi.mock("../../hooks/useAdminQueries", async () =>
  (await import("./discoverDialog.state")).adminQueries(),
);
vi.mock("../../context/AuthContext", async () => {
  const { AUTH } = await import("./discoverDialog.state");
  return { useAuth: () => AUTH };
});
vi.mock("../../context/DomainFilterContext", async () => {
  const { FILTER } = await import("./discoverDialog.state");
  return { useDomainFilter: () => FILTER };
});
vi.mock("../../api/admin", async (importOriginal) =>
  (await import("./discoverDialog.state")).adminApi(
    await importOriginal<typeof import("../../api/admin")>(),
  ),
);

beforeEach(resetDiscoveryMocks);

const keyNamesBox = () => screen.getByTestId("discover-kind-key-names") as HTMLInputElement;

async function openReview(tableIds: number[], onDone = vi.fn()) {
  render(<RelationshipReview tableIds={tableIds} onDone={onDone} />);
  await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
  return onDone;
}

describe("reviewStart", () => {
  it("sets the tables just registered against every table, either way round", () => {
    const at = new Date("2026-11-21T12:00:00Z");
    const start = reviewStart([2], REGISTERED as unknown as ListedTable[], at);
    expect(start.startedAt).toBe("2026-11-21T12:00:00.000Z");
    expect(start.scope).toMatchObject({
      kinds: ["foreign_keys", "key_names", "semantics"],
      source_domains: ["shop"],
      source_tables: [2],
      target_same_as_source: false,
      target_domains: [],
      target_tables: null,
      either_way: true,
    });
  });
});

describe("Relationship review after registration", () => {
  it("opens with foreign keys, key names and semantics asked, scoped to the new tables", async () => {
    await openReview([2]);
    expect(keyNamesBox().checked).toBe(true);
    expect((screen.getByTestId("discover-kind-foreign-keys") as HTMLInputElement).checked).toBe(
      true,
    );
    await waitFor(
      () =>
        expect(lastScope()).toMatchObject({
          kinds: ["foreign_keys", "key_names", "semantics"],
          source_tables: [2],
          target_same_as_source: false,
          target_tables: null,
          either_way: true,
        }),
      SLOW,
    );
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(startMock).toHaveBeenCalledWith(
        expect.objectContaining({ source_tables: [2], either_way: true }),
        expect.anything(),
      ),
    );
  });

  it("lists each candidate with its cardinality and why, and Done returns the count created", async () => {
    const ended = runOf({ status: "done", report: null });
    startMock.mockResolvedValue(ended);
    runMock.mockResolvedValue(ended);
    proposalsMock.mockResolvedValue({
      proposals: [
        proposal({
          id: 700,
          kind: "key_names",
          source_column: "customer_id",
          confidence: 0.9,
          reasoning: "name and type match the primary key customers.id",
        }),
        proposal({
          id: 701,
          kind: "foreign_keys",
          source_column: "store_id",
          confidence: 1,
          reasoning: "Foreign key constraint",
        }),
      ],
      threshold: 0.95,
      kept_at: 0.7,
      score_filter: null,
    });
    applyMock.mockResolvedValue([{ id: 701, action: "accept", ok: true, relationship_id: "r" }]);
    const onDone = await openReview([2]);
    fireEvent.click(runButton());
    const row = await screen.findByTestId("proposal-700", {}, SLOW);
    expect(within(row).getByText("name and type match the primary key customers.id")).toBeTruthy();
    expect(within(row).getByText("Name match")).toBeTruthy();
    expect(within(row).getByText("many-to-one")).toBeTruthy();
    fireEvent.click(screen.getByTestId("discover-apply"));
    await waitFor(() => expect(applyMock).toHaveBeenCalled());
    fireEvent.click(await screen.findByTestId("discover-done"));
    expect(onDone).toHaveBeenCalledWith(1);
  });
});
