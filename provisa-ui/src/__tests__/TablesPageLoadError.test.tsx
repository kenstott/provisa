// Copyright (c) 2026 Kenneth Stott
// Canary: c8d21f04-95b7-4e6a-83aa-16f0e5b7c9d2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1293: a failed `tables` query used to render as an empty "Registered Tables" grid — visually
// identical to "this org has no registered tables", a statement about the org's data that the
// failed request never established. That is what a user saw after self-creating an org: the demo
// data was there, the query was not answering, and the page said the org was empty.

import { describe, it, expect, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, screen } from "../test-utils/render";
import type { MockedResponse } from "@apollo/client/testing";
import { AuthProvider } from "../context/AuthContext";
import { DomainFilterProvider } from "../context/DomainFilterContext";
import { TablesQuery } from "../hooks/admin.graphql";
import { TablesPage } from "../pages/TablesPage";

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({}),
  fetchMe: vi.fn().mockRejectedValue(new Error("no identity in this test")),
}));

function renderTables(mocks: readonly MockedResponse[]) {
  return render(
    <MemoryRouter>
      <AuthProvider authEnabled={false} authSettled>
        <DomainFilterProvider>
          <TablesPage />
        </DomainFilterProvider>
      </AuthProvider>
    </MemoryRouter>,
    { mocks },
  );
}

describe("TablesPage — failed tables query (REQ-1293)", () => {
  it("reports the failure instead of rendering an empty grid", async () => {
    renderTables([
      {
        request: { query: TablesQuery },
        error: new Error("Org selection required"),
      },
    ]);

    const alert = await screen.findByTestId("tables-load-error");
    expect(alert).toHaveTextContent(/could not load registered tables/i);
    // The cause must reach the user — "something went wrong" does not distinguish an unbound org
    // from an empty one, which is the confusion this alert exists to end.
    expect(alert).toHaveTextContent(/Org selection required/);
  });

  it("stays silent when the query succeeds with no tables", async () => {
    renderTables([
      {
        request: { query: TablesQuery },
        result: { data: { tables: [] } },
      },
    ]);

    // An answered query that reports zero tables is a real answer; the alert must not claim failure.
    expect(await screen.findByText(/registered tables/i)).toBeInTheDocument();
    expect(screen.queryByTestId("tables-load-error")).not.toBeInTheDocument();
  });
});
