// Copyright (c) 2026 Kenneth Stott
// Canary: d32039be-3d62-4489-801b-64c3fc35c6b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source whose connector server is still booting answers its schema list with STARTING
// (REQ-1824). The picker asks again until the list arrives; before, the first answer was an error
// and the list stayed empty for good — a Splunk source's Register Table form never offered a schema.

import { describe, expect, it } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { MockedProvider } from "@apollo/client/testing/react";
import { GraphQLError } from "graphql";
import type { ReactNode } from "react";
import { AvailableSchemas } from "../admin.graphql";
import { useAvailableSchemas } from "../useAdminQueries";
import { STILL_STARTING_POLL_MS } from "../discoveryStartingPoll";

const query = { query: AvailableSchemas, variables: { sourceId: "e2e_splunk" } };

describe("useAvailableSchemas", () => {
  it(
    "asks again while the connector reports STARTING, and lists the schemas once it answers",
    async () => {
      const mocks = [
        {
          request: query,
          result: {
            errors: [new GraphQLError("STARTING: 'e2e_splunk''s connector is still starting up")],
          },
        },
        { request: query, result: { data: { availableSchemas: ["e2e_splunk", "public"] } } },
      ];
      const wrapper = ({ children }: { children: ReactNode }) => (
        <MockedProvider mocks={mocks}>{children}</MockedProvider>
      );
      const { result } = renderHook(() => useAvailableSchemas("e2e_splunk"), { wrapper });
      await waitFor(() => expect(result.current.schemas).toEqual(["e2e_splunk", "public"]), {
        timeout: STILL_STARTING_POLL_MS * 3,
      });
    },
    STILL_STARTING_POLL_MS * 4,
  );
});
