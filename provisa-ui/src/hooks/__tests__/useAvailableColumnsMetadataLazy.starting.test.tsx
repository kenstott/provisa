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
// (REQ-1824). Like the table list, it asks again every few seconds; once the server answers, the
// columns are in the cache and the next read returns them.

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
    "asks again while the connector reports STARTING, and returns the columns once it answers",
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
      expect(await result.current("e2e_splunk", "main", "events")).toEqual([]);
      await waitFor(
        async () => {
          const columns = await result.current("e2e_splunk", "main", "events");
          expect(columns.map((c) => c.name)).toEqual(["id"]);
        },
        { timeout: STILL_STARTING_POLL_MS * 3 },
      );
    },
    STILL_STARTING_POLL_MS * 4,
  );
});
