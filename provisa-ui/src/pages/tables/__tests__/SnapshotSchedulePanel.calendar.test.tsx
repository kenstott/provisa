// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8bc0ae-c2c7-4f48-8b29-19955da9ef5c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-962: a snapshot calendar created from a view's Snapshot Schedule closes its dialog once the
// server says it was saved, and the picker takes the new calendar.

import { useState } from "react";
import { describe, expect, it } from "vitest";
import type { MockedResponse } from "@apollo/client/testing";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";
import { Calendars, CreateCalendar } from "../../../hooks/admin.graphql";
import type { RegisteredTable } from "../../../types/admin";
import { SnapshotSchedulePanel } from "../SnapshotSchedulePanel";

const FISCAL = {
  name: "e2e-fiscal",
  version: "v1",
  baseSystem: "gregorian",
  tz: "UTC",
  fiscalAnchorMonth: 1,
  fiscalAnchorDay: 1,
  retailAnchor: null,
  weekStart: 0,
  holidays: [],
  weekend: [5, 6],
  dayClose: null,
};

const mocks: MockedResponse[] = [
  { request: { query: Calendars }, result: { data: { calendars: [] } } },
  {
    request: {
      query: CreateCalendar,
      variables: {
        input: {
          name: "e2e-fiscal",
          version: "v1",
          baseSystem: "gregorian",
          tz: "UTC",
          weekStart: 0,
          fiscalAnchorMonth: 1,
          fiscalAnchorDay: 1,
          retailAnchor: null,
          holidays: [],
          weekend: [5, 6],
          dayClose: null,
        },
      },
    },
    // The server takes its time, as a CI runner does.
    delay: 300,
    result: {
      data: {
        createCalendar: {
          __typename: "MutationResult",
          success: true,
          message: "calendar 'e2e-fiscal' v1 saved",
          code: "schema.calendar_saved",
          params: null,
        },
      },
    },
  },
  { request: { query: Calendars }, result: { data: { calendars: [FISCAL] } }, maxUsageCount: 5 },
];

function Host() {
  const [table, setTable] = useState({
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvBusinessDayGrain: false,
    mvExpectedEvents: null,
  } as unknown as RegisteredTable);
  return <SnapshotSchedulePanel editingTable={table} setEditingTable={setTable} />;
}

describe("a snapshot calendar created from the Snapshot Schedule", () => {
  it("closes its dialog once saved, and the picker takes the new calendar", async () => {
    renderInForm(<Host />, { mocks });
    fireEvent.click(await screen.findByTestId("mv-calendar-new"));
    fireEvent.change(await screen.findByTestId("calendar-name"), {
      target: { value: "e2e-fiscal" },
    });
    fireEvent.click(screen.getByTestId("calendar-create-submit"));
    await waitFor(() => expect(screen.queryByTestId("calendar-name")).not.toBeInTheDocument(), {
      timeout: 3000,
    });
    // The picker names the version as it is ("v1"), not with a second prefix.
    await waitFor(() =>
      expect(screen.getByTestId("mv-calendar")).toHaveValue("e2e-fiscal (v1, gregorian, UTC)"),
    );
  });
});
