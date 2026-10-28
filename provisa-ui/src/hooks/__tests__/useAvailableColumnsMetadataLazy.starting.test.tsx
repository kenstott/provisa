// Copyright (c) 2026 Kenneth Stott
// Canary: 5b0e7c1a-62c4-4f0b-9a57-3d1f8e2c4a91
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.


// The columns read of a source whose connector server is still booting answers STARTING
// (REQ-1824). The read reports that as null (not as no columns), exposes the starting state, asks
// again every few seconds, and hands the columns to the caller when the server answers.

import { describe, expect, it } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { MockedProvider } from "@apollo/client/testing/react";
import { GraphQLError } from "graphql";
import type { ReactNode } from "react";
import { AvailableColumnsMetadata } from "../admin.graphql";
import { useAvailableColumnsMetadataLazy } from "../useAdminQueries";
import { STILL_STARTING_POLL_MS } from "../discoveryStartingPoll";

const variables = { sourceId: "e2e_splunk", schemaName: "main", tableName: "events" };
const column = {
  __typename: "ColumnMetadata",
  name: "id",
  dataType: "bigint",
  subtype: null,
  comment: null,
  nativeFilterType: null,
  nativeFilterRequired: false,
  isPrimaryKey: true,
  path: null,
};

describe("useAvailableColumnsMetadataLazy", () => {
  it(
    "reports STARTING, asks again, and hands the caller the columns once the server answers",
    async () => {
      const request = { query: AvailableColumnsMetadata, variables };
      const mocks = [
        {
          request,
          result: {
            errors: [new GraphQLError("STARTING: 'e2e_splunk''s connector is listening")],
          },
        },
        { request, result: { data: { availableColumnsMetadata: [column] } } },
      ];
      const wrapper = ({ children }: { children: ReactNode }) => (
        <MockedProvider mocks={mocks}>{children}</MockedProvider>
      );
      const { result } = renderHook(() => useAvailableColumnsMetadataLazy(), { wrapper });
      // No answer yet: not an empty table.
      expect(await result.current.fetchColumns("e2e_splunk", "main", "events")).toBeNull();
      await waitFor(() => expect(result.current.starting).toBe(true));
      expect(result.current.late).toBeNull();
      // The poll's answer reaches the caller, with the table it belongs to.
      await waitFor(() => expect(result.current.late?.columns.map((c) => c.name)).toEqual(["id"]), {
        timeout: STILL_STARTING_POLL_MS * 3,
      });
      expect(result.current.late).toMatchObject(variables);
      await waitFor(() => expect(result.current.starting).toBe(false));
    },
    STILL_STARTING_POLL_MS * 4,
  );
});
