// Copyright (c) 2026 Kenneth Stott
// Canary: ff0f83b4-46ac-4be4-b95b-ad6bcfbc7b14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table write refetches the table queries a view is showing and no other. It used to name
// `{ query: TablesQuery }`, which Apollo fetches whether or not anything shows it: every write from
// a page that lists tables re-read the editor's whole catalog.

import { describe, expect, it } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ApolloClient, ApolloLink, InMemoryCache } from "@apollo/client";
import { ApolloProvider } from "@apollo/client/react";
import { of } from "rxjs";
import type { ReactNode } from "react";
import { useShownTableQueries, useTables, useUpdateTable } from "../useTableQueries";

const ROW = {
  __typename: "RegisteredTableType",
  id: 1,
  sourceId: "shop-db",
  domainId: "sales",
  schemaName: "public",
  tableName: "orders",
  alias: null,
  description: null,
  draft: false,
  viewSql: null,
  materialize: false,
  productId: null,
  dqContract: null,
  dqDataset: null,
  timeTravel: null,
  apiEndpoint: null,
  modelingRole: null,
  viewMetrics: null,
};

function harness() {
  const sent: string[] = [];
  const link = new ApolloLink((operation) => {
    sent.push(operation.operationName ?? "");
    if (operation.operationName === "UpdateTable") {
      return of({
        data: {
          updateTable: {
            __typename: "MutationResult",
            success: true,
            message: "",
            code: null,
            params: null,
          },
        },
      });
    }
    return of({ data: { tables: [ROW] } });
  });
  const client = new ApolloClient({ link, cache: new InMemoryCache() });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <ApolloProvider client={client}>{children}</ApolloProvider>
  );
  return { sent, wrapper };
}

describe("a table write's refetch", () => {
  it("names the table queries a view is showing", async () => {
    const { wrapper } = harness();
    const { result } = renderHook(
      () => ({ rows: useTables({ columns: false }), shown: useShownTableQueries() }),
      { wrapper },
    );
    await waitFor(() => expect(result.current.rows.tables).toHaveLength(1));
    expect(result.current.shown()).toEqual(["TableRowsQuery"]);
  });

  it("refetches the shown list and never the editor's TablesQuery", async () => {
    const { sent, wrapper } = harness();
    const { result } = renderHook(
      () => ({ rows: useTables({ columns: false }), write: useUpdateTable() }),
      { wrapper },
    );
    await waitFor(() => expect(result.current.rows.tables).toHaveLength(1));
    await act(() => result.current.write.updateTable({ id: 1 }));
    await waitFor(() => expect(sent.filter((n) => n === "TableRowsQuery")).toHaveLength(2));
    expect(sent).not.toContain("TablesQuery");
  });
});
