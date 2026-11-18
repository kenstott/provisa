// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3e1f60-2a94-4d7c-9e05-c1f7a2d6b843
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2247: a Syncing entry's differences with its catalog, and its standing choices. The list is
// grouped by kind then by table or term, each with its count; each row shows both values, the
// value last exchanged and which side changed, with Accept per side, and each group takes a side
// at once. A kind's standing choice is set per direction, from the list or from the entry's own
// table of standing choices.
//
// What is asserted is what a wrong panel gets wrong silently: a count that is not the group's, an
// Accept that sends another row or the wrong side, a group Accept that sends each row instead of
// the group, a standing choice sent for the wrong direction or kind, offering the catalog's side
// for a kind the server refuses (without saying why), and hiding a refusal.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, fireEvent, within } from "../../../../test-utils/render";
import { EntrySync } from "../EntrySync";
import type { EntryReview, EntryStandingChoices } from "../../../../api/syncing";

vi.mock("../../../../api/syncing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../../api/syncing")>()),
  fetchReview: vi.fn(),
  applyReview: vi.fn(),
  fetchStandingChoices: vi.fn(),
  setStandingChoice: vi.fn(),
}));

import {
  applyReview,
  fetchReview,
  fetchStandingChoices,
  setStandingChoice,
} from "../../../../api/syncing";

const mockReview = vi.mocked(fetchReview);
const mockApply = vi.mocked(applyReview);
const mockStanding = vi.mocked(fetchStandingChoices);
const mockSetStanding = vi.mocked(setStandingChoice);

const GROUP = "wh.public.orders";

const REVIEW: EntryReview = {
  id: "mz",
  provider: "openmetadata",
  directions: { export: true, import: true },
  total: 4,
  kinds: [
    {
      kind: "description",
      count: 1,
      standing_choice: { export: null, import: null },
      groups: [
        {
          group: GROUP,
          count: 1,
          rows: [
            {
              kind: "description",
              group: GROUP,
              object: GROUP,
              provisa: "Orders placed",
              catalog: "Orders, as the catalog says",
              last_exchanged: {
                value: "Orders placed",
                direction: "export",
                decided_by: "publish",
                at: "2026-11-17T10:00:00+00:00",
              },
              changed: "catalog",
            },
          ],
        },
      ],
    },
    {
      kind: "column_description",
      count: 2,
      standing_choice: { export: null, import: null },
      groups: [
        {
          group: GROUP,
          count: 2,
          rows: [
            {
              kind: "column_description",
              group: GROUP,
              object: `${GROUP}.id`,
              provisa: "Order id",
              catalog: "The id",
              last_exchanged: null,
              changed: null,
            },
            {
              kind: "column_description",
              group: GROUP,
              object: `${GROUP}.total`,
              provisa: "Total",
              catalog: "Sum",
              last_exchanged: null,
              changed: null,
            },
          ],
        },
      ],
    },
    {
      kind: "tag",
      count: 1,
      standing_choice: { export: null, import: null },
      groups: [
        {
          group: GROUP,
          count: 1,
          rows: [
            {
              kind: "tag",
              group: GROUP,
              object: GROUP,
              provisa: [],
              catalog: ["PII.Sensitive"],
              last_exchanged: null,
              changed: null,
            },
          ],
        },
      ],
    },
  ],
};

const STANDING: EntryStandingChoices = {
  id: "mz",
  directions: { export: true, import: true },
  standing_choices: { export: {}, import: { description: "catalog" } },
};

/** The option named `name` of the select `input` opened: its own list, not another select's. */
async function optionOf(input: HTMLElement, name: string): Promise<HTMLElement> {
  await waitFor(() => expect(input).toHaveAttribute("aria-controls"));
  const list = document.getElementById(input.getAttribute("aria-controls") as string);
  return within(list as HTMLElement).getByRole("option", { name, hidden: true });
}

beforeEach(() => {
  vi.clearAllMocks();
  mockReview.mockResolvedValue(REVIEW);
  mockStanding.mockResolvedValue(STANDING);
  mockApply.mockResolvedValue({ id: "mz", applied: { provisa: 0, catalog: 1 }, publish: null });
  mockSetStanding.mockResolvedValue(STANDING);
});

describe("an entry's difference list", () => {
  it("groups by kind then table, each with its count, and shows both values per row", async () => {
    render(<EntrySync entryId="mz" />);
    const list = await screen.findByTestId("syncing-diff-list");
    expect(within(list).getByTestId("syncing-diff-kind-description")).toHaveTextContent("(1)");
    expect(within(list).getByTestId("syncing-diff-kind-column_description")).toHaveTextContent(
      "(2)",
    );
    expect(
      within(list).getByTestId(`syncing-diff-group-column_description-${GROUP}`),
    ).toHaveTextContent("(2)");
    const row = within(list).getByTestId(`syncing-diff-row-description-${GROUP}`);
    expect(row).toHaveTextContent("Orders placed");
    expect(row).toHaveTextContent("Orders, as the catalog says");
    // The value last exchanged, and which side moved off it.
    expect(row).toHaveTextContent("Changed in the catalog");
    const tag = within(list).getByTestId(`syncing-diff-row-tag-${GROUP}`);
    expect(tag).toHaveTextContent("PII.Sensitive");
  });

  it("accepts one row's side, sending that row only, and reads the list again", async () => {
    render(<EntrySync entryId="mz" />);
    fireEvent.click(
      await screen.findByTestId(`syncing-accept-catalog-column_description-${GROUP}.id`),
    );
    await waitFor(() =>
      expect(mockApply).toHaveBeenCalledWith("mz", [
        { side: "catalog", kind: "column_description", object: `${GROUP}.id` },
      ]),
    );
    await waitFor(() => expect(mockReview).toHaveBeenCalledTimes(2));

    fireEvent.click(screen.getByTestId(`syncing-accept-provisa-description-${GROUP}`));
    await waitFor(() =>
      expect(mockApply).toHaveBeenLastCalledWith("mz", [
        { side: "provisa", kind: "description", object: GROUP },
      ]),
    );
  });

  it("accepts a whole group's side as one group decision", async () => {
    render(<EntrySync entryId="mz" />);
    fireEvent.click(
      await screen.findByTestId(`syncing-accept-group-provisa-column_description-${GROUP}`),
    );
    await waitFor(() =>
      expect(mockApply).toHaveBeenCalledWith("mz", [
        { side: "provisa", kind: "column_description", group: GROUP },
      ]),
    );
  });

  it("does not offer the catalog's side of a kind Provisa cannot take, and says why", async () => {
    render(<EntrySync entryId="mz" />);
    const accept = await screen.findByTestId(`syncing-accept-catalog-tag-${GROUP}`);
    expect(accept).toBeDisabled();
    expect(screen.getByTestId(`syncing-accept-group-catalog-tag-${GROUP}`)).toBeDisabled();
    // Provisa's side of the same row stays open.
    expect(screen.getByTestId(`syncing-accept-provisa-tag-${GROUP}`)).toBeEnabled();
    fireEvent.mouseEnter(accept.parentElement as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Taking a catalog's owners, tags and glossary links into Provisa is not supported yet.",
    );
  });

  it("does not offer a side whose direction is off", async () => {
    mockReview.mockResolvedValue({ ...REVIEW, directions: { export: false, import: true } });
    render(<EntrySync entryId="mz" />);
    expect(await screen.findByTestId(`syncing-accept-provisa-description-${GROUP}`)).toBeDisabled();
    expect(screen.getByTestId(`syncing-accept-catalog-description-${GROUP}`)).toBeEnabled();
  });

  it("shows a refusal by name in plain words", async () => {
    mockReview.mockRejectedValue(new Error("This catalog's values cannot be read back yet."));
    render(<EntrySync entryId="mz" />);
    expect(await screen.findByTestId("syncing-diff-problem")).toHaveTextContent(
      "This catalog's values cannot be read back yet.",
    );
  });

  it("says when there is no difference", async () => {
    mockReview.mockResolvedValue({ ...REVIEW, total: 0, kinds: [] });
    render(<EntrySync entryId="mz" />);
    expect(await screen.findByTestId("syncing-diff-none")).toBeInTheDocument();
  });
});

describe("an entry's standing choices", () => {
  it("shows each kind's choice per direction that runs", async () => {
    mockStanding.mockResolvedValue({ ...STANDING, directions: { export: false, import: true } });
    render(<EntrySync entryId="mz" />);
    const table = await screen.findByTestId("syncing-standing-choices");
    expect(within(table).getByTestId("syncing-standing-import-description")).toHaveValue(
      "Take the catalog's",
    );
    // A direction that is off never runs, so it has no choices to set.
    expect(within(table).queryByTestId("syncing-standing-export-description")).toBeNull();
  });

  it("sets a kind's choice for one direction from the entry's table", async () => {
    const user = userEvent.setup();
    render(<EntrySync entryId="mz" />);
    const select = await screen.findByTestId("syncing-standing-export-owner");
    await user.click(select);
    await user.click(await optionOf(select, "Hold"));
    await waitFor(() =>
      expect(mockSetStanding).toHaveBeenCalledWith("mz", "export", "owner", "hold"),
    );
  });

  it("sets 'always do this' for a kind from its heading in the list", async () => {
    const user = userEvent.setup();
    render(<EntrySync entryId="mz" />);
    const select = await screen.findByTestId("syncing-always-export-column_description");
    await user.click(select);
    await user.click(await optionOf(select, "Take Provisa's"));
    await waitFor(() =>
      expect(mockSetStanding).toHaveBeenCalledWith("mz", "export", "column_description", "provisa"),
    );
  });

  it("does not offer taking the catalog's value for a kind Provisa cannot take", async () => {
    const user = userEvent.setup();
    render(<EntrySync entryId="mz" />);
    const select = await screen.findByTestId("syncing-standing-import-tag");
    await user.click(select);
    const option = await optionOf(select, "Take the catalog's (not supported yet)");
    expect(option).toHaveAttribute("data-combobox-disabled");
  });
});
